# LLD 4 — Gateway

The stateless tier. Terminates client HTTP, resolves the caller to an org, works out
where a message belongs, and forwards to the node that holds it.

Implements: HLD Sections 4, 5, 6, 7.

---

## 1. Structure

```go
type Gateway struct {
    meta    *meta.Client       // Postgres configs, etcd watches (LLD 5)
    auth    *authCache         // credential → org
    configs *configCache       // (org, name) → config
    limits  *rateLimiter       // token bucket per org
    pool    *nodePool          // gRPC connections, one per node
    placement atomic.Pointer[PlacementMap]
}
```

Nothing here is authoritative. Every field is a cache of something owned elsewhere,
which is what makes gateways interchangeable and lets the load balancer treat them as
identical (HLD §5).

---

## 2. Request path

```
  authenticate → resolve config → validate → rate limit
      → compute slot → look up node → forward → translate the reply
```

```go
func (g *Gateway) enqueue(w http.ResponseWriter, r *http.Request) {
    org, err := g.auth.Resolve(r.Header.Get("Authorization"))
    if err != nil { write(w, 401, "unauthenticated"); return }

    name := mux.Var(r, "queue")
    cfg, err := g.configs.Get(QueueKey{org, name})
    switch {
    case errors.Is(err, meta.ErrNoQueue): write(w, 404, "no such queue"); return
    case err != nil:                      write(w, 503, "metadata unavailable"); return
    case cfg.State == meta.StateDeleting: write(w, 404, "queue is being deleted"); return
    }

    var req EnqueueRequest
    if err := decode(r, &req, maxPayloadBytes); err != nil { write(w, 400, err); return }
    if err := validate(&req, cfg); err != nil { write(w, 400, err); return }
    if !g.limits.Allow(org, 1) { writeRetryAfter(w, 429); return }

    key  := QueueKey{org, name}
    slot := slotFor(key, req.GroupID, cfg)
    node, ok := g.route(key, slot)
    if !ok { write(w, 503, "no owner for this slot"); return }

    resp, err := g.pool.For(node).Enqueue(r.Context(), buildRPC(key, slot, &req))
    if err != nil { g.translateRPCError(w, err, key, slot); return }
    writeJSON(w, 201, EnqueueResponse{MessageID: resp.MessageId})
}
```

**Authentication happens first, before anything reads the queue name.** Every step
after it is scoped to one org, so there is no later check that can be forgotten
(HLD §5).

---

## 3. Authentication

```go
type authCache struct {
    ttl   time.Duration          // 60s
    neg   time.Duration          // 5s for failures
    lru   *lru.Cache[string, authEntry]
    sf    singleflight.Group
}

type authEntry struct {
    org       string
    crossOrg  bool               // admin credentials only
    expiresAt time.Time
}
```

Credentials are hashed before they are used as a cache key, and the hash is what
appears in logs. A raw credential never reaches a log line or an error message.

`singleflight` collapses concurrent lookups of the same credential into one database
query. Without it, a burst from a new client turns into a burst against Postgres.

Failures are cached briefly. A client looping with a bad credential should not
produce one query per request, and five seconds is short enough that rotating a
credential takes effect quickly.

### Cross-org access

```go
if entry.crossOrg {
    if h := r.Header.Get("X-Ryuk-Org"); h != "" { org = h }
}
```

For an ordinary credential the header is **ignored, not rejected** (HLD §4). A client
that sets it by accident sees its own org rather than a confusing error.

---

## 4. Config cache

```go
type configCache struct {
    ttl time.Duration            // 30s
    neg time.Duration            // 2s
    m   *lru.Cache[QueueKey, cfgEntry]
    sf  singleflight.Group
}
```

Two rules, both from HLD §5:

**A miss means go and look, not "no such queue."** Someone creates a queue and
submits to it immediately. Treating a miss as a 404 would fail.

**Negative results live for a second or two.** Not caching them means a typo in a
loop queries Postgres every time; caching them for long recreates the problem above.

Thirty seconds of staleness is acceptable because everything a config controls
tolerates it: a new visibility timeout applies to new leases, a new retry limit to
new failures. The optional faster path is a version counter in etcd, which gateways
already watch for the placement map.

---

## 5. Routing

```go
func slotFor(k QueueKey, groupID string, cfg *meta.Config) uint16 {
    if groupID != "" {
        return uint16(hash(k.Org, k.Name, groupID) % queue.SlotsPerQueue)
    }
    return uint16(rand.Uint32() % uint32(cfg.Fanout()))    // §5.1
}

func (g *Gateway) route(k QueueKey, slot uint16) (NodeID, bool) {
    pm := g.placement.Load()
    pg := uint16(hash(k.Org, k.Name, slot) % NumPlacementGroups)
    owner, ok := pm.Owner(pg)
    return owner, ok
}
```

Both hashes salt with the org and the queue name, so two tenants using the same group
key never collide on a slot (HLD §9).

The gateway **computes and looks up; it never chooses** (HLD §5). Slot and placement
group are pure functions, and ownership comes from the map. Nothing about routing is
a judgement call, which is why many gateways need no coordination between them.

### 5.1 Fan-out for ungrouped messages

The gateway cannot see queue depth, so it cannot compute the adaptive fan-out from
LLD 1 §8.1 itself. Nodes report each queue's current fan-out in their stats, and it
rides along on the same gossip that carries priority hints (§6). A stale value only
means messages land in slightly fewer or more slots than ideal, which is harmless —
ungrouped messages carry no ordering constraint.

### 5.2 Placement map changes

The map is watched, not polled. A change replaces the pointer atomically, so readers
never lock:

```go
func (g *Gateway) onPlacementChange(pm *PlacementMap) {
    g.placement.Store(pm)
}
```

A node that no longer owns a slot returns `FailedPrecondition` with its map version.
If that version is newer than ours, we refresh and retry once. If it is older, the
node is behind and we retry a different replica.

---

## 6. Choosing a node for dequeue

A consumer asks for work from a queue, not from a slot. The gateway has to pick.

```go
func (g *Gateway) dequeueTarget(k QueueKey) NodeID {
    best, bestBand := NodeID(""), -1
    for _, n := range g.owners(k) {
        if b := g.hints.Band(n, k); b > bestBand {
            best, bestBand = n, b
        }
    }
    return best
}
```

Each node publishes the most urgent priority it currently holds per queue — a few
bits, refreshed every 100 ms (HLD §7). Gateways route to whichever claims the most
urgent work.

Gateways are few relative to nodes, so this converges quickly. A stale hint costs one
suboptimal choice and corrects on the next refresh. Asking every node on every request
would put a round trip on the hot path and produce traffic quadratic in cluster size.

---

## 7. Long polling and the delivery handshake

```go
func (g *Gateway) dequeue(w http.ResponseWriter, r *http.Request) {
    // ... auth, config, validate ...
    stream := g.pool.For(node).DequeueStream()      // multiplexed, long-lived

    if err := stream.Send(&Want{Key: key, Max: req.MaxMessages, Wait: req.WaitTime}); err != nil {
        write(w, 503, "node unavailable"); return
    }
    msgs, err := stream.Recv()
    if err != nil { write(w, 503, "node unavailable"); return }
    if len(msgs.Messages) == 0 { w.WriteHeader(204); return }

    if err := writeJSON(w, 200, toResponse(msgs)); err != nil {
        return                      // consumer vanished; leases release on stream close
    }
    stream.Send(&Handed{Receipts: receiptsOf(msgs)})   // ← confirms delivery
}
```

The `Handed` message is what closes the failure window in HLD §11. If the gateway
dies before sending it, the node releases those leases immediately instead of holding
them for a full visibility timeout. If it dies after, the lease behaves normally,
because a consumer really did receive the message.

One stream per gateway-node pair, multiplexed across consumers, so a thousand waiting
consumers do not open a thousand streams to each node.

**Idle timeouts.** A `waitTime` of up to twenty seconds means the load balancer's idle
timeout must exceed it. Most default to sixty seconds, but a shorter one would kill
long polls and look like a client bug. This belongs in the deployment notes because
it is invisible until it bites.

---

## 8. Rate limiting

```go
type rateLimiter struct {
    gateways atomic.Int32                    // live gateway count, from etcd
    buckets  *lru.Cache[string, *bucket]     // per org
}

func (l *rateLimiter) Allow(org string, n int) bool {
    share := l.orgLimit(org) / int(l.gateways.Load())
    return l.bucket(org, share).AllowN(n)
}
```

Each gateway enforces its share of the org's allowance. This is approximate — an org
whose traffic lands unevenly gets a little less than its limit, and one gateway going
down briefly raises everyone's share (HLD §9).

Exactness would require a shared counter consulted on every request, which is a round
trip on the hot path to enforce a capacity-planning limit. Not worth it.

The gateway count comes from etcd membership, so the share adjusts as gateways scale.

---

## 9. Error translation

The one place engine and RPC vocabulary become HTTP.

| Source | HTTP | Body |
|---|---|---|
| `ErrLeaseExpired` | 409 | lease expired, message was redelivered |
| `ErrNotInFlight` | **200** | idempotent, nothing to do |
| `ErrBadReceipt` | 400 | malformed receipt |
| `ErrBadPriority` | 400 | priority out of range |
| `ErrQueueFull`, `ErrQuotaExceeded` | 503 | with `Retry-After` |
| `meta.ErrNoQueue` | 404 | |
| `FailedPrecondition` from a node | retry once, then 503 | stale placement map |
| `Unavailable` from a node | retry another replica, then 503 | |
| `DeadlineExceeded` | 504 | |

`ErrNotInFlight` mapping to 200 is the interesting one. The engine cannot tell "already
acknowledged" from "never existed," and a consumer retrying an acknowledgment after a
network timeout is the common case. Returning 404 would make correct clients log
errors for doing the right thing.

---

## 10. Connection pool

```go
type nodePool struct {
    mu    sync.RWMutex
    conns map[NodeID]*grpc.ClientConn
}
```

One HTTP/2 connection per node, multiplexing all requests. Connections are created on
first use and closed when a node leaves membership.

Health follows gRPC's own connectivity state rather than a separate health check.
A node in `TRANSIENT_FAILURE` is skipped for dequeue routing, since another replica
can serve it. Enqueue and acknowledge cannot be rerouted — only the owner can take
them — so those fail fast with 503 and let the client retry.

---

## 11. What the gateway must never do

Three rules, each of which breaks a guarantee if violated.

**Never buffer a write.** No acknowledgment to a producer until the owning node has
committed. A stateless tier holding accepted-but-not-durable messages would void the
durability guarantee (HLD §5).

**Never decide placement.** Slot and placement group are pure functions; ownership
comes from the map. If gateways could choose, two of them could send one group to
different slots and silently break ordering.

**Never trust a client-supplied org.** It comes from the credential. The only
exception is an admin credential carrying explicit cross-org scope.

---

## 12. Edge cases

| Case | Behaviour |
|---|---|
| Credential valid, queue belongs to another org | 404, not 403 — existence is not disclosed |
| Queue created moments ago | Cache miss triggers a read; found |
| Queue deleted moments ago | Serves until the cache refreshes, then 404; the node rejects sooner |
| Placement map stale | Node returns its version, gateway refreshes and retries once |
| Every owner of a slot is down | 503; the coordinator is already promoting a replica |
| Consumer disconnects mid-response | `Handed` is not sent, leases release immediately |
| `waitTime` above the cap | Clamped to 20s |
| Payload above the cap | 413, rejected before it crosses the internal network |
| Postgres down, config cached | Serves normally |
| Postgres down, config not cached | 503 — we cannot invent a config |
| etcd down | Serves from the cached placement map |
| Two gateways create the same queue at once | The primary key decides; the loser reads back and returns 200 or 409 |
