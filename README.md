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

**[docs/demo/ryuk-demo-compressed.mp4](docs/demo/ryuk-demo-compressed.mp4)** (4.8 MB)
— an 80-second walkthrough against a live four-node cluster: create a queue, send
two HIGH, one MEDIUM and one LOW, then poll them one at a time through
acknowledge and nack, watching the ordering and retry counters move.

`docs/demo/ryuk-demo.mp4` is the full-quality render, and the
[HyperFrames source](docs/demo/composition) rebuilds either one.

> For a player embedded in the page rather than a download link, drag the
> compressed file into a GitHub issue, pull request or release. GitHub returns a
> `user-attachments` URL that renders inline; a repository path does not.

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
