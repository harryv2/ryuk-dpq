# LLD 3 — Node

Implements HLD §6, §8, §9, §12. Packages `backend/queue/logic` and
`backend/queue/controller`.

A node holds queues, runs their timers, and answers gateways over gRPC. It needs
exactly one piece of configuration: the etcd address.

---

## 1. Shape

```
controller/   gRPC service, protobuf conversion
logic/        QueueLogic: which queues live here, recovery, timers, transfer
logic/engine/ LLD 1 — pure, no imports outside the standard library
repo/walfile/ LLD 2
```

```go
type QueueLogic struct {
    cfg     Config          // node id, incarnation, sweep interval
    log     *slog.Logger
    clock   engine.Clock
    cluster engine.Cluster
    wals    entity.WALFactory

    mu     sync.RWMutex
    queues map[engine.QueueKey]*liveQueue

    deadLetters chan deadLetter
    subs        *subscribers
}

type liveQueue struct {
    q   *engine.Queue
    wal entity.WALRepo
}
```

---

## 2. Queues materialise on first use

Every request carries a `QueueSpec`, so a node can start serving a queue it has
never seen without a separate create call:

```go
func (l *QueueLogic) queueFor(spec entity.QueueSpec) (*liveQueue, error) {
    // fast path under RLock, then create under Lock with a second check
    wal, err := l.wals.Open(key)
    lq = &liveQueue{
        q:   engine.New(spec.EngineConfig(), l.clock, l.cluster, wal, spec.Generation),
        wal: wal,
    }
}
```

An idle queue costs nothing anywhere: no row is pushed to nodes at creation, and
no directory exists until a message arrives.

The generation comes from the spec, which is what makes sequence numbers issued
by a new owner sort after the old owner's.

---

## 3. Recovery

Runs before the node starts serving:

```go
func (l *QueueLogic) Recover() error {
    keys, _ := l.wals.List()          // what does this disk hold?
    for _, key := range keys {
        lq, _ := l.queueFor(spec)
        bySlot, _ := lq.wal.Replay()
        lq.q.Absorb(bySlot)
    }
}
```

Two things fall out of this and are worth stating.

**The node discovers what it holds from its own disk**, not from being told. It
can come back and be useful before anything has updated it.

**`Absorb` rather than `Enqueue`** because replayed messages already have their
sequence numbers, attempt counts and timestamps. Re-enqueuing would stamp new ones
and lose the ordering the log preserved.

Leases do not survive: everything that was in flight comes back as available, and
the incarnation is bumped so receipts from before the crash are rejected.

---

## 4. Timers

```go
func (l *QueueLogic) RunSweeper(stop <-chan struct{}) {
    t := time.NewTicker(l.cfg.SweepEvery)   // 200ms
    for { select { case <-stop: return; case <-t.C: l.Sweep() } }
}
```

`Sweep` takes a snapshot of the queue map, then works through them one at a time.
Each queue's `Sweep` releases delayed messages, expires leases and drops
TTL-expired messages, taking one slot lock at a time.

Dead-lettered messages come back from the engine rather than being routed by it —
the engine knows nothing about other queues. They go into a buffered channel that
the caller drains.

A sweep that falls behind delays redelivery. It does not block traffic, because it
never holds more than one slot lock and never holds one across a queue.

---

## 5. Waking a parked consumer

The naive way to long-poll is to forward a waiting dequeue to every machine that
might get the message. Two could answer, and the second message would be leased
with nobody to process it — invisible until its lease expired.

So notification and dequeue are separate things:

```go
type subscribers struct {
    mu   sync.RWMutex
    next uint64
    m    map[uint64]chan Notification
}
```

One channel per connected gateway, not per consumer. A gateway keeps one stream to
each node, so this map has a handful of entries however many consumers are parked.

```go
func (s *subscribers) publish(n Notification, count int) {
    sent := 0
    for _, ch := range s.m {
        if sent >= count { return }
        select {
        case ch <- n: sent++
        default:                    // a slow gateway is skipped, never blocked on
        }
    }
}
```

**Only as many gateways are woken as there is work for.** Waking all of them would
send several dequeues for one message and most would come back empty.

The non-blocking send matters: a gateway that stops reading must not be able to
slow down an enqueue.

Published on enqueue, and after a sweep puts messages back.

---

## 6. Transfer

Two operations, used for both rebalancing and for a returning node handing its
data to whoever owns it now.

```go
func (l *QueueLogic) Freeze(spec entity.QueueSpec, only []uint16) (map[uint16][]entity.WireMessage, error)
func (l *QueueLogic) Absorb(req entity.TransferRequest) error
```

**Freeze** stops the queue serving and drains it. An empty filter means every slot
this node holds; a distributed queue names the slots that actually moved, and the
rest are put back and the queue thawed so it keeps serving them.

**Absorb** writes each message to this node's log *before* making it visible — the
same ordering as a normal enqueue — then hands them to the engine, which inserts
by sequence number so an older generation lands ahead of anything already here.

---

## 7. gRPC surface

```protobuf
service QueueService {
  rpc Enqueue(EnqueueRequest) returns (EnqueueResponse);
  rpc Dequeue(DequeueRequest) returns (DequeueResponse);
  rpc Ack(AckRequest) returns (Empty);
  rpc Nack(NackRequest) returns (Empty);

  rpc Stats(StatsRequest) returns (QueueStats);
  rpc StatsAll(Empty) returns (StatsAllResponse);

  rpc Drop(StatsRequest) returns (Empty);
  rpc Freeze(FreezeRequest) returns (Transfer);
  rpc Absorb(Transfer) returns (Empty);

  rpc Subscribe(SubscribeRequest) returns (stream WorkAvailable);
  rpc Health(Empty) returns (HealthResponse);
}
```

`StatsAll` answers for every queue on this node in one call, which is what makes
metric collection cost one request per node rather than one per queue.

`Subscribe` is the only streaming method. When a gateway goes away the stream
breaks, `stream.Context()` is cancelled, and the subscription is dropped.

Errors map through one function so a caller sees the same distinction the logic
layer made:

| Logic code | gRPC code |
|---|---|
| `invalid` | `InvalidArgument` |
| `not_found` | `NotFound` |
| `conflict` | `FailedPrecondition` |
| `exhausted` | `ResourceExhausted` |
| `moved` | `Aborted` |

---

## 8. Startup

```
1. node id      from <dataDir>/node-id, generated on first ever start
2. incarnation  bumped and written
3. WAL factory  opened over <dataDir>
4. Recover()    replay every queue this disk holds
5. sweeper      started
6. etcd         register under a lease, keep it alive
7. gRPC         serve
```

Registering *after* recovery matters: a node should not be routable until it can
actually answer for the data it holds.

The advertised address is the container hostname plus the listen port, which
resolves inside a container network without any configuration.

---

## 9. What the node does not do

- **It does not decide placement.** It serves what it is asked for and holds what
  it has.
- **It does not talk to Postgres.** Ownership is the gateway's to record.
- **It does not know about other nodes**, except when handed a transfer.

That keeps a node a single-purpose thing: hold slots, run timers, answer requests.
