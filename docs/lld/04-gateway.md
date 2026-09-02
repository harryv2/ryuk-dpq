# LLD 4 — Gateway

Implements HLD §6, §8, §9, §13, §17. Packages `backend/gateway/*`.

The gateway stores nothing. It resolves a credential to an org, works out which
machine owns a queue, forwards, and collects metrics.

---

## 1. Shape

```
controller/   public HTTP, credential to org, status mapping
logic/        routing, placement, operations, collector, rebalancer, waiters
repo/queueconfigpg/  Postgres
repo/nodegrpc/       one gRPC connection per node
```

```go
type GatewayLogic struct {
    configs entity.QueueConfigRepo
    nodes   entity.NodeRepo
    members *membership.Client

    wait  *waiters
    cache *cache.TTL[string, entity.QueueConfig]
    stats *cache.TTL[string, entity.NodeStats]
}
```

---

## 2. Credential first

```go
func (h *Handlers) withOrg(next orgHandler) http.HandlerFunc {
    token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
    org, ok := h.orgs[token]
    if !ok { writeErr(w, ...); return }
    next(w, r, org)
}
```

Resolving the credential before anything else means every later step is already
scoped to one org, so there is no second check to forget. A caller cannot reach
another org's queue by guessing a name, because the name is only half the
identifier.

The token-to-org map is hardcoded today. Real authentication replaces this one
function.

---

## 3. Config cache

```go
func (l *GatewayLogic) config(ctx, org, name) (entity.QueueConfig, error) {
    if c, ok := l.cache.Get(key(org, name)); ok { ... }
    cfg, err := l.configs.Get(ctx, org, name)
    ...
}
```

Two rules keep it from causing trouble.

**A miss means go and look, not "no such queue."** Someone creates a queue and
submits to it immediately; treating a miss as a 404 would fail.

**A negative result is cached for two seconds**, so a loop with a typo'd name does
not hit Postgres every time, without bringing back the first problem. An empty
`QueueConfig` in the cache is the marker.

Evicted immediately when a node says a queue has moved.

---

## 4. Placement

```go
func OwnerFor(key string, members []Member) (Member, bool)   // rendezvous hashing
```

Every gateway computes the same owner from the same member list, so there is
nothing to elect and no map to keep consistent. Adding a machine moves only the
keys that machine now wins.

**But the stored owner wins over the hash.**

```go
func (l *GatewayLogic) ownerAddr(ctx, cfg, slot) (string, string, error) {
    stored := cfg.OwnerNode
    if cfg.Distributed { stored = cfg.SlotOwners[uint16(slot)] }

    if stored != "" {
        if m, ok := l.members.Lookup(stored); ok { return stored, m.Addr, nil }
        // the owner is down; nothing is reassigned, because its data is only there
        return stored, "", enterr.New(enterr.CodeExhausted, ...)
    }
    // first use: pick an owner and record it
}
```

When a node dies the hash immediately names somebody else, but the data is only on
the dead node. Reassigning would hand the caller an empty queue while the real
messages sat on a disk nobody was reading. So ownership follows the data, and a
queue on a dead node returns 503 until it comes back.

---

## 5. Routing and the moved retry

```go
func (l *GatewayLogic) withOwner(ctx, cfg, slot, call) error {
    for attempt := 0; attempt < 2; attempt++ {
        _, addr, err := l.ownerAddr(ctx, cfg, slot)
        err = call(addr, cfg.Spec())
        if err == nil { return nil }
        if enterr.CodeOf(err) != enterr.CodeMoved || attempt == 1 { return err }
        l.cache.Evict(key(cfg.Org, cfg.Name))
        cfg, err = l.config(ctx, cfg.Org, cfg.Name)
    }
}
```

A node that no longer owns a queue says so, the cached config is dropped, and the
call is retried once against the new owner. That is the whole cost of a migration
as far as a producer is concerned.

---

## 6. The two queue types

The difference appears in exactly three places.

**Creation.** A normal queue gets one owner; a distributed queue places each of
its sixty-four slots independently, so they spread over up to sixty-four machines.

**Enqueue.** The slot comes from `hash(org, queue, groupID)`, so every message in
a group lands on the same slot — that is what makes ordering possible. The gateway
sends the slot explicitly, so the node uses the same one rather than choosing its
own.

**Dequeue and stats.**

```go
if !cfg.Distributed {
    // one machine: one call, and the answer is exact
}
// spread: ask each machine holding a slot
```

For a distributed queue, dequeue tries owners ranked by the collector's last view
of who has the most urgent work, falling through when one comes back empty. Stats
asks every owner and sums, reports `exact: false`, and counts machines it could
not reach rather than quietly under-reporting.

A stale ranking costs one wasted call. Asking every machine on every request would
put a round trip on the hot path.

---

## 7. Long polling

```go
msgs, err := take()
if len(msgs) > 0 || req.WaitTime <= 0 { return ... }

ch := l.wait.park(key(org, queue))
defer l.wait.unpark(...)

select {
case <-timer.C:  return nothing
case <-ch:       take() again
}
```

The try-once first is what keeps this cheap: a busy queue never parks, so the
machinery only exists on idle queues where nobody notices it.

`RunSubscriber` keeps one gRPC stream open per node and forwards notifications to
whichever consumer is parked. Ten thousand parked consumers are ten thousand map
entries and no extra connections.

Waking releases **one** waiter, because a notification means one message.

---

## 8. Rebalancing

```go
func (l *GatewayLogic) RunRebalancer(ctx context.Context) {
    case <-l.members.Changed():
        settleAt = time.Now().Add(stabilityWindow)   // 15s
    case <-tick.C:
        if past settleAt { l.Rebalance(ctx) }
}
```

The window exists because a container in a crash loop would otherwise move data
continuously, which hurts far more than the imbalance it is correcting. At most
two queues move per pass.

A migration is a state flip, then freeze, ship, record, release:

```go
if err := l.configs.SetState(ctx, org, name, StateMigrating); err != nil { return }
transfer, _ := l.nodes.Freeze(ctx, from.Addr, spec, slots)
transfer.Spec.Generation = cfg.Generation + 1
l.nodes.Absorb(ctx, to.Addr, transfer)
l.configs.SetOwner(ctx, org, name, to.ID, nextGen)
l.nodes.Drop(ctx, from.Addr, spec)
```

`SetState` into `migrating` only succeeds from `active`, so whichever gateway wins
the update owns the move and the others skip it. That is the only coordination
between gateways in the whole system.

**A queue whose owner is down is never moved.** Its data is only there.

A distributed queue is rebalanced slot by slot, with slots going the same way
batched into one handoff.

---

## 9. Collector

```go
for _, m := range l.members.Members() {
    nodeID, all, err := l.nodes.StatsAll(ctx, m.Addr)
    for _, s := range all {
        l.stats.Put(key(s.Org, s.Name), s)
        l.stats.Put(key(s.Org, s.Name)+"@"+nodeID, s)
    }
}
```

One request per node every five seconds returns every queue that node holds, so
the cost is fixed in the number of machines rather than the number of queues.

The per-node entry is what ranks owners for a distributed dequeue.

Everything that reports numbers reads this one cache, so the dashboard, the stats
endpoint and `/metrics` cannot disagree.

---

## 10. Status mapping

| Logic code | HTTP |
|---|---|
| `invalid` | 400 |
| `not_found` | 404 |
| `conflict` | 409 |
| `exhausted` | 503 |
| `moved` | 503 |

Dequeue returns **204** when nothing is available, because an empty queue is not
an error.
