# Ryuk — Code Plan

How `RYUK_HLD.md` and `lld/01-queue-engine.md` become code.

Module: `github.com/harryv2/ryuk-dpq`

---

## 1. Conventions

Taken from `payment-service`, unchanged:

| Thing | Convention |
|---|---|
| Entity file | `repo-<what>.go` — the model struct **and** its repo interface together |
| Request/response | `XxxRequestOptions`, `XxxResponse` in `entity/req-*.go` |
| God struct | `logic/app.go` holds the struct and its interface; one file per operation |
| Logic method | `func (l *XxxLogic) Op(ctx, opts entity.OpRequestOptions) (entity.OpResponse, enterr.CustomError)` |
| Controller file | `v1-<operation>.go`, each with `parseAndValidateXxxRequest` |
| Errors | `enterr.CustomError` out of logic; controllers map it to a status |
| DI | `container.go` with `//go:build wireinject`, generated `wire_gen.go` |
| Shared wrappers | `third_party/` — logger, validator, cache. Injected into logic |

**Repo means the real backing store, nothing else.** One implementation per interface:
Postgres for configs and placement, etcd for membership, gRPC for nodes, files for the
log. Caching is `third_party/inmemorycache` injected into the god struct, not a second
repo implementation.

**Each service owns its mocks.** `gateway/mocks/` and `queue/mocks/` are separate
packages with separate wire containers, and neither imports the other.

---

## 2. Layout

```
ryuk-dpq/
├── backend/
│   ├── gateway/
│   │   ├── entity/
│   │   │   ├── enterr/errors.go
│   │   │   ├── repo-queue-config.go        QueueConfig, Placement + QueueConfigRepo
│   │   │   ├── repo-node.go                NodeRepo — gRPC to nodes
│   │   │   ├── repo-membership.go          MembershipRepo — etcd watch
│   │   │   ├── model-message.go
│   │   │   ├── model-member.go             Member, OwnerFor
│   │   │   └── req-*.go                    one per endpoint
│   │   ├── controller/
│   │   │   ├── router.go  handlers.go  middlewares.go  common.go
│   │   │   ├── v1-create-queue.go   v1-delete-queue.go   v1-list-queues.go
│   │   │   ├── v1-enqueue.go        v1-dequeue.go
│   │   │   ├── v1-ack.go            v1-nack.go
│   │   │   ├── v1-stats.go          v1-metrics.go        v1-metrics-prom.go
│   │   │   └── v1-cluster.go
│   │   ├── logic/
│   │   │   ├── app.go   container.go   wire_gen.go
│   │   │   ├── queue-admin-logic.go        create, delete, list
│   │   │   ├── placement-logic.go          rendezvous, owner assignment
│   │   │   ├── routing-logic.go            queue → owner → client, ErrMoved retry
│   │   │   ├── enqueue-logic.go
│   │   │   ├── dequeue-logic.go            long poll, waiter registry
│   │   │   ├── ack-logic.go
│   │   │   ├── config-cache-logic.go       positive and negative TTL
│   │   │   ├── summary-logic.go            priority summaries for distributed queues
│   │   │   └── metrics-collector-logic.go  one call per node per interval
│   │   ├── repo/
│   │   │   ├── queueconfigpg/              Postgres: queues + slot_placement
│   │   │   ├── nodegrpc/                   gRPC client, one stream per node
│   │   │   └── membershipetcd/             lease, keepalive, watch
│   │   └── mocks/
│   │
│   ├── queue/
│   │   ├── entity/
│   │   │   ├── enterr/errors.go
│   │   │   ├── repo-wal.go                 WALRepo
│   │   │   ├── repo-membership.go          register self, keepalive
│   │   │   ├── repo-placement.go           read/write ownership in Postgres
│   │   │   ├── model-record.go             log record types
│   │   │   └── req-*.go
│   │   ├── controller/
│   │   │   ├── grpc-server.go
│   │   │   ├── v1-enqueue.go   v1-dequeue.go   v1-ack.go   v1-nack.go
│   │   │   ├── v1-stats.go                 every queue on this node, one call
│   │   │   ├── v1-subscribe.go             streaming: work available + summaries
│   │   │   └── v1-migrate.go               freeze, ship, absorb, confirm
│   │   ├── logic/
│   │   │   ├── app.go   container.go   wire_gen.go
│   │   │   ├── engine/                     ← LLD 1. Pure. Zero dependencies.
│   │   │   ├── queue-manager-logic.go      live queues, create, drop
│   │   │   ├── enqueue-logic.go            journal, then engine
│   │   │   ├── dequeue-logic.go
│   │   │   ├── ack-logic.go
│   │   │   ├── sweeper-logic.go            lease, TTL, delay timers
│   │   │   ├── notify-logic.go             subscriber registry, proportional wake
│   │   │   ├── recovery-logic.go           replay, bump incarnation, reconcile owner
│   │   │   ├── rebalance-logic.go          what should I give up, and to whom
│   │   │   ├── migration-logic.go          freeze, ship, absorb
│   │   │   └── stats-logic.go
│   │   ├── repo/
│   │   │   ├── walfile/                    segments, CRC, replay, snapshot
│   │   │   ├── membershipetcd/
│   │   │   └── placementpg/
│   │   └── mocks/
│   │
│   ├── proto/queue/v1/queue.proto
│   ├── config/  constants/  helper/
│   ├── third_party/
│   │   ├── logger/           zap
│   │   ├── validatorwrpr/    go-playground/validator
│   │   └── inmemorycache/    ristretto — config, placement, stats
│   ├── tests/
│   │   ├── harness-producer/  harness-consumer/  integration/
│   └── cmd/
│       ├── ryuk-gateway/main.go
│       └── ryuk-node/main.go
│
├── frontend/                               Next.js, app router, TypeScript, Tailwind
│   ├── app/
│   │   ├── layout.tsx                      org dropdown in the header
│   │   ├── page.tsx                        dashboard: queues, depth, cluster
│   │   ├── queues/new/page.tsx
│   │   ├── queues/[name]/page.tsx          send, poll, in-flight, DLQ
│   │   ├── queues/[name]/metrics/page.tsx  graphs
│   │   └── cluster/page.tsx                nodes and where queues live
│   ├── components/                         OrgSelect, QueueTable, MessageList, charts
│   └── lib/api.ts  lib/orgs.ts
│
├── deploy/docker-compose.yml  deploy/init.sql
├── Makefile
└── docs/
```

---

## 3. Layer rules

```
controller  →  logic  →  entity  ←  repo
```

- **controller** imports `logic` and `entity`. Never `repo`.
- **logic** imports `entity` and `third_party`. It holds repo *interfaces* only.
- **repo** imports `entity` and implements its interfaces.
- **entity** imports nothing from the other three.
- **gateway and queue never import each other.** They share only `proto/`.

`engine` sits under `queue/logic` and imports **nothing outside the standard library**.
`make lint-layers` greps for violations, because this is the rule that erodes first —
one import of a logging framework and the concurrency tests slow down and stop being
deterministic.

---

## 4. The two queue types in code

The whole difference is in three places.

**Creation** — the gateway computes the owner and writes it:

```go
if !cfg.Distributed {
    owner := entity.OwnerFor(key.String(), membershipRepo)
    // queues.owner_node = owner
} else {
    for slot := 0; slot < 16; slot++ {
        owner := entity.OwnerFor(fmt.Sprintf("%s/slot-%d", key, slot), membershipRepo)
        // slot_placement row per slot
    }
}
```

**Routing** — the gateway picks a node:

```go
if !cfg.Distributed {
    return cfg.OwnerNode                    // one field, cached
}
slot := hashGroupToSlot(key, groupID)
return placement[slot].OwnerNode            // 16 fields, cached
```

**Dequeue** — the gateway picks which node to ask:

```go
if !cfg.Distributed {
    return cfg.OwnerNode                    // nothing to choose
}
return l.summaries.BestNodeFor(key)         // highest claimed priority
```

Inside the engine it is one method: `Cluster.LocalSlots` returns all 64 for a normal
queue and a subset for a distributed one (LLD 1 §8.2). Nothing else in the engine
branches on the flag.

---

## 5. Placement

**Membership** — each node writes one key and keeps renewing it:

```
/ryuk/membershipRepo/{hostname}  →  {"addr":"node-3:9090"}    lease TTL 10s
```

`hostname` is the container's, so `--scale node=5` produces five distinct membershipRepo with
no configuration.

**Choosing an owner** is a pure function, so no coordinator exists:

```go
func OwnerFor(key string, membershipRepo []Member) Member {
    best, bestScore := Member{}, uint64(0)
    for _, m := range membershipRepo {
        if s := xxhash.Sum64String(key + "\x00" + m.ID); s > bestScore {
            best, bestScore = m, s
        }
    }
    return best
}
```

**Storing it** is what makes ownership follow the data rather than the hash. When a node
dies the hash immediately says somebody else owns its queues; the stored record still
says otherwise, and the stored record wins (HLD §7, §12).

**Rebalancing** runs on each node independently:

```go
// after the member list has been stable for 15s
for _, u := range n.owned() {
    if want := entity.OwnerFor(u.Key(), membershipRepo); want.ID != n.id {
        n.migrations <- migration{unit: u, to: want}   // max 2 concurrent
    }
}
```

Every node runs this simultaneously and they agree, because the function is pure and the
member list is identical everywhere.

---

## 6. Metrics without a Prometheus client

```
GET /v1/queues/{name}/stats     one queue, JSON
GET /v1/metrics                 every queue in the caller's org, JSON
GET /metrics                    the same numbers in Prometheus text format
```

The collector calls `NodeRepo.StatsAll(node)` once per node every ten seconds — one call
returns every queue that node holds — and caches the result. All three endpoints read
that cache, so they cannot disagree.

`/metrics` is hand-written text formatting, about forty lines and no dependency. The
assignment asks for metrics *"that could be scraped by a monitoring system like
Prometheus"*, and this satisfies it literally while the JSON endpoints are what the UI
uses.

---

## 7. Wire and mocks, per service

```go
//go:build wireinject
// +build wireinject

func InitialiseGatewayLogic(cfg *config.Store) (*GatewayLogic, func(), error) {
    wire.Build(
        logger.New, validatorwrpr.New, inmemorycache.New,
        queueconfigpg.New,   wire.Bind(new(entity.QueueConfigRepo), new(*queueconfigpg.Repo)),
        nodegrpc.New,        wire.Bind(new(entity.NodeRepo), new(*nodegrpc.Repo)),
        membershipetcd.New,  wire.Bind(new(entity.MembershipRepo), new(*membershipetcd.Repo)),
        newGatewayLogic,
    )
    return nil, nil, nil
}
```

Every entity file carrying an interface gets:

```go
//go:generate mockgen -source=repo-node.go -destination=../mocks/mock_node_repo.go -package=mocks
```

Each `mocks` package has its own wire container that builds its god struct with all repos
mocked, so a unit test is three lines of setup.

**The engine gets no mocks.** It depends on nothing. Only `Clock` is substituted, and
that is a hand-written fake.

---

## 8. Tests, per service

| Where | What |
|---|---|
| `queue/logic/engine/` | Every state change, the seven invariants, `-race`, property test against a naive single-lock model, misbehaving workers, fake clock |
| `queue/logic/` | God struct against mocked WAL, membership and placement |
| `queue/repo/walfile/` | Round trip, torn tail, replay, snapshot |
| `gateway/logic/` | Routing, placement, config cache, collector, `ErrMoved` retry — all repos mocked |
| `gateway/controller/` | Request parsing, validation, status mapping |
| `tests/integration/` | Real cluster: enqueue → dequeue → ack, node restart, scale up, node kill for both queue types |

`make test` runs both services' unit tests with `-race`. `make test-integration` needs
compose up.

---

## 9. Frontend

Next.js, app router, TypeScript, Tailwind, Recharts.

| Page | What |
|---|---|
| `/` | Queue table: depth, in-flight, oldest age, DLQ, owner node, type |
| `/queues/new` | Create form: settings, `distributed`, and a dead-letter queue picked from the ones that exist |
| `/queues/[name]` | Send a message. Poll and display. In-flight list with ack and nack. DLQ tab |
| `/queues/[name]/metrics` | Depth by priority over time, enqueue and ack rate, oldest age, starvation escapes |
| `/cluster` | Live nodes, which queue sits where, migrations in progress |

The org dropdown lives in the layout header over a hardcoded list in `lib/orgs.ts`, and
the selection becomes a bearer token the gateway maps back to an org. Real
authentication is a middleware swap.

The dashboard polls `/v1/metrics` every few seconds and keeps a rolling window
client-side — no time-series database.

The frontend builds into the gateway image and is served at `/`, so one published port
gives API and UI together.

---

## 10. Docker

```yaml
services:
  postgres:   # queue configs and placement
  etcd:       # membership
  gateway:    # port 8080 — API and UI
  node:       # no published port, no fixed hostname
```

```bash
docker compose up --scale node=3
docker compose up --scale node=6      # registers, rebalances, settles in ~20s
```

Nodes need one setting: the etcd address. They take their identity from the container
hostname, so scaling needs no compose edit and no gateway restart.

---

## 11. Build order

| # | Phase | Ends with |
|---|---|---|
| 1 | Scaffold | `go build ./...`. Module, Makefile, config, third_party, `enterr` |
| 2 | **Engine** | LLD 1 complete |
| 3 | **Engine tests** | Seven invariants under `-race`, property test, misbehaving workers |
| 4 | WAL | Segments, CRC, replay, snapshot. Crash-recovery test |
| 5 | Queue logic | Manager, sweepers, notify, recovery. Mocked-repo unit tests |
| 6 | Proto + node controller | gRPC server; `ryuk-node` runs standalone |
| 7 | Membership + placement | etcd register and watch, rendezvous, ownership in Postgres |
| 8 | Gateway repo + logic | Configs, node client, cache, routing |
| 9 | Gateway controller | REST surface; end to end enqueue → dequeue → ack |
| 10 | Long poll | Subscribe stream, waiter registry, proportional wake |
| 11 | Metrics | Collector and all three endpoints |
| 12 | Compose | Postgres, etcd, gateway, scalable nodes. Integration tests |
| 13 | Distributed queues | Per-slot placement, summaries, cross-node dequeue routing |
| 14 | Rebalancing | Freeze, ship, absorb, `ErrMoved` retry, anti-thrash guards |
| 15 | Merge on return | A returning node ships its stranded slots to the current owner |
| 16 | Harnesses | Producer and consumer stubs, N×M concurrency, invariant assertions |
| 17 | Frontend | Five pages |
| 18 | README | Decisions, trade-offs, scalability, durability, what is next |

**Phases 2–4 are the graded core.** Deliverable 1 says correctness over completeness, so
the engine and its tests get the time they need before anything else starts.

Phases 13–15 are where the two queue types and real elasticity land. Each is a clean
stopping point: if 15 slips, `block` is still the documented default and nothing is
half-built.

---

## 12. Remaining LLDs

| # | Component | Covers |
|---|---|---|
| 2 | Write-ahead log | Record format, segments, CRC, replay, snapshot |
| 3 | Node | Slot ownership, gRPC surface, sweepers, notifications, migration |
| 4 | Gateway | REST, routing, config cache, long polling, collection |
| 5 | Placement and metadata | Postgres schema, etcd membership, rendezvous, rebalancing |
| 6 | Frontend and harnesses | UI, producer and consumer stubs |

Written as each phase is reached, rather than all up front — the engine LLD is the one
that had to exist before any code.
