



# Ryuk — Distributed Priority Queue

A priority queue service. Producers submit work with a priority and an optional
ordering key; workers pull it, process it, and acknowledge. Ryuk owns the queues,
tracks each message from submission to completion, and reports metrics.

**Design:** [`docs/RYUK_HLD.md`](docs/RYUK_HLD.md) — the whole system, and why.

**Component detail:**
[queue engine](docs/lld/01-queue-engine.md) ·
[write-ahead log](docs/lld/02-write-ahead-log.md) ·
[node](docs/lld/03-node.md) ·
[gateway](docs/lld/04-gateway.md) ·
[placement and metadata](docs/lld/05-placement-and-metadata.md) ·
[metrics and testing](docs/lld/06-metrics-and-testing.md)

## Demo

https://github.com/user-attachments/assets/adb64610-61bf-4f6d-a1fc-3acc24502eed

## Architecture

![Architecture](docs/diagram-architecture.png)

Two tiers. Gateways are stateless and interchangeable — any one can serve any
request, so they sit behind one address and scale by adding more. Nodes own the
data: slots, leases and a write-ahead log per slot. Neither metadata store sits
on the path a message takes.

![Message lifecycle](docs/diagram-lifecycle.png)

Source: [`docs/ryuk_architecture.drawio`](docs/ryuk_architecture.drawio) (two
pages). Re-export with:

```bash
cd docs
drawio -x -f png --scale 2.5 -p 1 -o diagram-architecture.png ryuk_architecture.drawio
drawio -x -f png --scale 2.5 -p 2 -o diagram-lifecycle.png ryuk_architecture.drawio
```

---

## Running it

```bash
make up                 # postgres, etcd, gateway, 3 nodes
make scale N=6          # add three more; queues rebalance onto them
make down
```

The UI is at <http://localhost:8090>, served by the gateway.

## Trying it

```bash
T="Authorization: Bearer acme-token"

curl -X POST localhost:8090/v1/queues -H "$T" \
  -d '{"name":"orders","visibilityTimeout":"30s","maxRetries":3,"defaultTtl":"1h"}'

curl -X POST localhost:8090/v1/queues/orders/messages -H "$T" \
  -d '{"payload":"ship-9981","priority":"HIGH","groupId":"user-123"}'

curl -X POST localhost:8090/v1/queues/orders/messages/dequeue -H "$T" -d '{}'

curl -X POST localhost:8090/v1/queues/orders/messages/ack -H "$T" \
  -d '{"receipt":"<from the dequeue>"}'

curl localhost:8090/v1/queues/orders/stats -H "$T"
```

## Two kinds of queue

|  | `distributed: false` (default) | `distributed: true` |
|---|---|---|
| Where it lives | One node, all 16 slots | Up to 64 nodes, a slot each |
| Priority and FIFO | **Exact** | Approximate across machines |
| Message counts | **Exact**, one request | Summed across machines |
| If its node dies | Whole queue waits for it | Only its slots; the rest keep serving |

Ordering **within a group** is strict either way, because a group never spans
slots. Two messages that must be ordered should share a group key.

## Where state lives

Postgres holds what a queue is and where it sits. etcd holds who is alive.
Messages live in node memory, backed by a write-ahead log on that node's disk.
Prometheus holds the history behind the charts.

### Postgres

```sql
CREATE TABLE queues (
    org_id      TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    settings    JSONB       NOT NULL,   -- visibility timeout, retries, ttl,
                                        -- starvation threshold and reserve,
                                        -- max depth, dead-letter queue
    distributed BOOLEAN     NOT NULL DEFAULT false,
    state       TEXT        NOT NULL DEFAULT 'active',  -- active | migrating | deleting
    owner_node  TEXT,                   -- set for a normal queue only
    generation  BIGINT      NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, name)
);

CREATE TABLE slot_placement (
    org_id     TEXT     NOT NULL,
    queue_name TEXT     NOT NULL,
    slot       SMALLINT NOT NULL,
    owner_node TEXT     NOT NULL,
    generation BIGINT   NOT NULL DEFAULT 1,
    PRIMARY KEY (org_id, queue_name, slot)
);

CREATE INDEX queues_owner_idx ON queues (owner_node);
```

Two tables because there are two placement units. A normal queue is placed whole,
so its owner is one column on its row. A distributed queue is placed per slot, so
it gets sixty-four rows.

`generation` rises on every ownership change and prefixes the sequence numbers a
node hands out, so two owners can never issue overlapping ones.

#### Gotcha: why store placement at all, when it is a pure hash?

Rendezvous hashing already tells every gateway which node should own a slot,
with nothing elected and no coordination. So `slot_placement` looks redundant,
and in a system with **replication** it largely is: when the ring moves a slot,
the new owner already holds the data, so routing can follow the hash directly
and the mapping can stay derived.

This system has no replication, which splits one question into two:

| | Answer |
|---|---|
| Where *should* this slot live? | the hash, recomputed from the member list |
| Where *is* its data right now? | `slot_placement` |

They diverge the moment a node joins or leaves. Messages do not move on their
own, so if routing followed the hash alone, adding a node would silently point
the gateway at a machine holding nothing, and the messages on the old owner
would become invisible — depth reading zero while they sit there. **Ownership has
to follow the data.** The hash proposes, the table records, and the rebalancer is
what makes them agree: freeze the old owner, ship, absorb, and only then update
the row and bump the generation.

It is also what makes a lost node honest. The row still names the dead machine,
so the queue returns 503 rather than reassigning to a new owner that would serve
an empty queue.

Worth noting that storing placement is not unusual: Kafka keeps partition
leadership in cluster metadata and Redis Cluster gossips an explicit slot map,
both on top of a hash. The hash gives the target; the metadata is the truth
during and after a move. Replication changes the *cost* of that gap, not whether
it exists.

### etcd

Membership only. One key per node, held by a lease, so a node that stops
refreshing disappears on its own.

```
/ryuk/members/<node-id>  →  {"id":"node-8b5f01fadac2","addr":"172.30.0.4:9090"}
```

The lease TTL is 10s (`RYUK_LEASE_TTL`). Gateways watch the prefix, so a node
joining or leaving reaches every gateway without anyone polling.

Placement is **not** in etcd. Without replication a queue's data exists in one
place, so ownership has to follow the data rather than the hash — which means it
has to be written down, and Postgres is where the queue already lives.

### Prometheus

Prometheus scrapes the **gateway**, never a node. The gateway already collects
from every node and exposes the result at `/metrics`, so scraping the nodes
would duplicate that path and give two things to keep in step.

```
prometheus  --scrape-->  gateway /metrics  --gRPC-->  nodes
     ^
     |  query_range, org label injected server-side
  gateway /v1/queues/{name}/timeseries  <--  the metrics page
```

The browser never talks to Prometheus directly. It asks the gateway, which
builds the PromQL with the org taken from the credential — so one tenant cannot
read another's series by naming its queue. The window (`5m`, `1h`, `6h`, `24h`)
picks the step, and the query set is fixed rather than caller-supplied.

Prometheus is optional. Without `RYUK_PROMETHEUS` the endpoint answers
`available: false` and the page falls back to sampling the live endpoint itself,
which only covers the time the tab has been open. It is at <http://localhost:9091>
with seven days of retention.

## Testing

```bash
make test           # unit tests
make test-race      # the same under the race detector
make integration    # the real stack in Docker, driven through the REST API
```

The integration suite is Gherkin scenarios run by godog — see
[`integration/`](integration/README.md).

## Layout

```
backend/
  queue/        the node: engine, gRPC server, write-ahead log
    logic/engine/   priority, groups, leases, starvation reserve
  gateway/      REST, routing, placement, rebalancing, metric collection
  cmd/          two binaries
frontend/       Next.js UI, static export, served by the gateway
integration/    BDD scenarios against a real Docker cluster
deploy/         compose file and one Dockerfile per service
docs/           design
```

Each service is `controller → logic → entity ← repo`. The engine imports nothing
outside the standard library; `make lint-layers` checks that and the other layer
rules.

## What is not built

Replication, and with it failing over to a new owner rather than waiting for the
one that holds the data. Cells, cluster-wide quotas, load-aware placement, and
automatic promotion of a busy queue to `distributed`.
