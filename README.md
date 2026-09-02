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

---

## Running it

```bash
make up                 # postgres, etcd, gateway, 3 nodes
make scale N=6          # add three more; queues rebalance onto them
make down
```

Without Docker:

```bash
go build ./backend/...
go test -race ./backend/...
```

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
curl localhost:8090/v1/cluster
curl localhost:8090/metrics
```

Load and invariant check:

```bash
go run ./backend/tests/harness -producers 8 -consumers 8 -messages 500
```

---

## Two kinds of queue

The only placement decision a caller makes, and it defaults to off.

|  | `distributed: false` (default) | `distributed: true` |
|---|---|---|
| Where it lives | One node, all 16 slots | Up to 64 nodes, a slot each |
| Priority ordering | **Exact** | Approximate across machines |
| FIFO within a priority | **Exact** | Approximate across slots |
| Message counts | **Exact**, one request | Sum across machines |
| Ceiling | One machine | Up to 64 machines |
| If its node dies | Whole queue unavailable | Only its slots; the rest keep serving |

Ordering **within a group** is strict in both cases, because a group never spans
slots. That is the guarantee to reach for: two messages that must be ordered
relative to each other should share a group key.

A single machine handles a few hundred thousand messages a second. Reach for
`distributed` when one queue genuinely outgrows that, and accept that priority
and FIFO become approximate for it.

## Key decisions

**Priority is an integer 0–100**, with HIGH/MEDIUM/LOW as named points (75/50/25).
Supporting finer priorities later is a config change rather than a rewrite.

**Ordering comes from groups, not from the queue.** This is SQS FIFO's model, not
Kafka's. Kafka pins a consumer to a partition, so a consumer on partition 3 cannot
see an urgent message on partition 5 — which would break the main thing we promise.
Ryuk locks a group when it hands out a message and unlocks it on acknowledgment,
so no worker is tied to anything.

**Acknowledge takes a receipt, not a bare message ID.** Two reasons, and the
second matters even on one machine: an ID cannot be routed without a cluster-wide
index, and it cannot tell two deliveries apart. If a worker stalls past its lease
and the message goes to somebody else, a late acknowledgment from the first worker
would delete a message the second is still processing. The receipt carries a lease
generation that makes that detectable.

**Low priority cannot starve.** A fixed share of deliveries is reserved for work
that has waited past a threshold. The gate means it costs nothing when nothing is
stuck, and the cap means a large old backlog cannot flip the problem around and
starve urgent work instead. Queue creation rejects a threshold at or above the
expiry, because that combination silently deletes low-priority work.

**One lock per slot.** Handing out a message changes five structures at once and
they have to move together. A normal queue has sixteen slots and therefore sixteen
independent locks; a distributed queue has sixty-four, which is also the ceiling on
how many machines it can use.

**Placement is a pure function, so nothing is elected.** Rendezvous hashing over
the live member list: every gateway computes the same owner from the same list.
Adding a machine moves only the keys that machine now wins.

**Placement is also stored, and the stored value wins.** The hash says where a
queue *should* go; Postgres says where it *is*. When a node dies the hash
immediately names someone else, but the data is only on the dead node, so
ownership has to follow the data. Nothing is reassigned automatically.

**The gateway collects metrics; nothing scrapes a node.** One request per node
per interval returns every queue that node holds. Nodes have no public listener,
and a single aggregation path means the dashboard and the stats endpoint cannot
disagree.

## What is guaranteed

**At-least-once delivery.** A message is delivered until acknowledged. It can
arrive more than once — a worker that processes but does not acknowledge in time,
or a node that restarts while the message is out. **Workers must be idempotent.**

**Durability: the write-ahead log.** A message is written and flushed before it
becomes visible, so a worker can only be handed something already on disk.

| Failure | Survives |
|---|---|
| Process panics, killed, redeployed | Yes, completely |
| Machine reboots | Yes |
| Disk fills | Yes — submissions rejected, nothing lost |
| Log torn at the end | Yes — truncated at the checksum |
| **Machine destroyed** | **No** |

The last row is the honest limit, and replication is what fixes it. It is designed
and not built: the log is already the replication stream, so shipping records to
followers and waiting for a quorum is an addition rather than a redesign — but a
partly-working replication protocol is worse than none.

There is a cheaper production answer too: put the log on storage that outlives the
machine. Cloud persistent disks detach from a dead instance and attach to a new
one, which turns permanent machine loss into a slow restart with no consensus
protocol.

## Node failure

```
node-2 dies holding a normal queue and 4 slots of a distributed one

  normal queue        → 100% unavailable until it returns
  distributed queue   → 4 of 64 slots unavailable, 12 keep serving
  data                → safe in node-2's log
  reassignment        → none; its data is only there
```

When it comes back it replays its logs, bumps its incarnation, and either resumes
its queues or ships them to whoever owns them now — the same transfer used for
rebalancing.

A receipt issued before the crash is rejected on the incarnation, because lease
generations restart from zero after a replay and would otherwise let an
acknowledgment for a long-dead delivery delete somebody else's message.

## Scaling

`make scale N=6` starts three more nodes. Each takes one setting — the etcd
address — and registers itself under a lease. Every node then works out on its own
which of its queues no longer hash to it and hands them over, because the
placement function is pure and the member list is the same everywhere.

Guards against thrashing: membership must be stable for a window before anything
moves, migrations are rate-limited, and a just-received queue is pinned briefly.

## Testing

```bash
go test -race ./backend/...
```

36 tests across three packages.

**The engine is tested with a clock the test controls.** Visibility timeouts,
expiry, delayed release and starvation are driven by advancing a fake clock, not
by sleeping — sleeping is slow, flaky, and cannot test a twelve-hour timeout.

**Concurrency is tested with the race detector**, not by hoping a stress test
trips something. Eight producers and eight consumers move four thousand messages
and then the run asserts: everything reached one end state, nothing acknowledged
twice, nothing delivered to two workers at once, group order preserved, and the
queue drained.

That group-order assertion found a real bug: sequence numbers were assigned before
the slot lock, so two concurrent producers could take 100 and 101 and then insert
in the opposite order. The fix makes the lock the point that orders both.

**Crash recovery is tested for real**: write, `SIGKILL`, restart, and check that
acknowledged messages stay gone while unacknowledged ones come back with their
attempt counts.

**The WAL is tested against a torn tail** — appending garbage to the file and
checking that replay stops at the checksum and the next append still works.

## The UI

`make up` serves it at [localhost:8090](http://localhost:8090) alongside the API.
Queues with depth and owner, a create form exposing every setting, send and poll
with ack and nack, priority-stacked charts, and a cluster page showing which node
holds what. The org dropdown in the header switches credentials.

## Layout

```
backend/
  queue/                  the node
    logic/engine/         the queue engine — no imports outside the stdlib
    logic/                manager, recovery, sweepers, transfer
    repo/walfile/         write-ahead log, node identity
    controller/           internal HTTP API
  gateway/
    entity/               models, placement hashing, repo interfaces
    logic/                routing, operations, collector, rebalancer
    repo/                 postgres, node gRPC client, etcd
    controller/           public HTTP API
  proto/                  the gateway-to-node contract
  third_party/            logger, cache
  cmd/                    ryuk-node, ryuk-gateway
  tests/harness/          producer and consumer load check
```

`make lint-layers` checks the rules that matter: the engine imports nothing from
the module, controllers never reach into repos, entity never imports the layers
above it, and the two services never import each other.

`engine` imports nothing outside the standard library, which is what lets the
concurrency tests run the real code path at full speed with no transport, no disk
and no real clock.

## Deviations from the design docs

**No coordinator election.** The HLD describes an elected coordinator writing a
placement map. Rendezvous hashing removed the need — placement is a pure function
of the member list, so every gateway computes the same answer and nothing has to
be agreed. The election comes back for load-aware placement, where the choice is
no longer a pure function.

**Rebalancing runs on the gateway, not the node.** Ownership records live in
Postgres, which only the gateway talks to. Both compute the same target, so the
outcome is identical.

## With more time

In rough order of value:

1. **Replication** — the one thing between this and surviving a lost machine.
2. **Failing over to a new owner** instead of returning 503 while the owning node
   is down. Worth doing once replication exists: without the data, a new owner
   breaks group ordering across the failure window, so today the queue blocks.
3. **Automatic promotion to `distributed`** when a queue saturates its machine.
4. **Two-pass migration** so a handoff never freezes enqueues at all.
5. **Per-org quotas and cells** — per-queue depth limits cover most of it today.
