# LLD 3 — Node

The stateful tier. A node owns a set of slots, runs a queue engine over each, keeps
their write-ahead logs, and serves gateways over an internal RPC.

Implements: HLD Sections 5, 9, 11.

---

## 1. Structure

```go
type Node struct {
    id   NodeID
    meta *meta.Client            // Postgres configs + etcd watches (LLD 5)

    mu     sync.RWMutex
    queues map[QueueKey]*queue.Queue   // materialised on first use

    owned   *ownedSlots           // placement groups this node holds
    sweeper *sweeper              // shared timer wheel, §4
    waiters *waiterRegistry       // long-poll parking, §5
    leases  *leaseTracker         // which stream issued which lease, §3.2
}
```

A node holds slots from many queues belonging to many orgs. They never interact:
each slot has its own lock, its own log and its own messages (LLD 1 §5).

### Which slots does this node own

```go
func (n *Node) LocalSlots(q QueueKey) []uint16 {
    out := make([]uint16, 0, 8)
    for slot := uint16(0); slot < queue.SlotsPerQueue; slot++ {
        pg := placementGroupFor(q, slot)
        if n.owned.Has(pg) {
            out = append(out, slot)
        }
    }
    return out
}
```

Sixty-four hashes per call is too much for the dispatch path, so the result is cached
per queue and invalidated when the placement map version changes. The cache is the
only place the node consults placement at all — everything below it works in slots.

---

## 2. Startup and shutdown

### Startup

```
1. Read the placement map from etcd; note which placement groups are ours
2. Scan the data directory; for each slot directory, recover it (LLD 2 §8)
3. Drop any slot whose queue no longer exists, or whose placement group is not ours
4. Register liveness in etcd with a TTL, and start refreshing it
5. Start the sweeper and the RPC server
6. Announce readiness
```

Step 3 is reconciliation, and it has to happen before serving. A node that was down
during a rebalance holds slots that now belong elsewhere. Serving them would mean two
nodes handing out the same messages.

Recovery is per slot and runs in parallel across a bounded pool — a node with
thousands of slots should not replay them one at a time, and each slot's log is
independent so there is nothing to order.

### Shutdown

```
1. Stop accepting new work; in-flight RPCs finish
2. Deregister from etcd so the coordinator starts reassigning
3. Flush every log and take a final snapshot per slot
4. Hand off slots the coordinator has already reassigned
5. Exit
```

Step 3 matters more than it looks. A clean shutdown that skips the flush loses up to
the sync interval for no reason, and turns an ordinary deploy into a redelivery
event.

Leases are **not** preserved across a restart, by design (HLD §11). Every in-flight
message becomes available again when the node comes back.

---

## 3. RPC surface

Internal only. Nodes have no public listener (HLD §5).

```protobuf
service Node {
  rpc Enqueue (EnqueueRequest) returns (EnqueueResponse);
  rpc Dequeue (stream DequeueRequest) returns (stream DequeueResponse);
  rpc Ack     (AckRequest)     returns (AckResponse);
  rpc Nack    (NackRequest)    returns (NackResponse);
  rpc Stats   (StatsRequest)   returns (StatsResponse);

  // control plane, called by the coordinator
  rpc AssumeSlot  (AssumeSlotRequest)  returns (AssumeSlotResponse);
  rpc FreezeSlot  (FreezeSlotRequest)  returns (FreezeSlotResponse);
  rpc ShipSlot    (ShipSlotRequest)    returns (stream SlotChunk);
  rpc ReleaseSlot (ReleaseSlotRequest) returns (ReleaseSlotResponse);
}
```

Every request carries the org, the queue name and the slot. The node validates that
it actually owns that slot and returns `FailedPrecondition` with the placement map
version if it does not, so a gateway with a stale map corrects itself rather than
retrying blindly.

### 3.1 Enqueue

```go
func (n *Node) Enqueue(ctx context.Context, r *EnqueueRequest) (*EnqueueResponse, error) {
    q, err := n.queue(r.Key)                    // materialise from config if needed
    if err != nil { return nil, err }
    if !n.owns(r.Key, r.Slot) { return nil, errNotOwner(n.mapVersion()) }

    id, err := q.EnqueueToSlot(r.Slot, r.Message)   // durable before visible
    if err != nil { return nil, err }
    n.waiters.Wake(r.Key)
    return &EnqueueResponse{MessageId: id}, nil
}
```

`EnqueueToSlot` writes the log record and waits for it before touching memory
(LLD 1 §13, LLD 2 §1). Waking waiters happens after, so a long-polling consumer never
sees a message that is not yet durable.

### 3.2 Dequeue is a stream, and why

Dequeue is bidirectional streaming rather than request-response. That is not for
throughput — it is how the node learns that a gateway died.

```
gateway ──▶ node   Want{key, maxMessages, waitFor}
node    ──▶ gateway Delivered{message, receipt}
gateway ──▶ node   Handed{receipt}          ← the consumer actually received it
```

Without this, a gateway dying between the lease being taken and the consumer
receiving the message leaves that message invisible for a full visibility timeout
(HLD §11).

```go
type leaseTracker struct {
    mu     sync.Mutex
    byStream map[streamID]map[Receipt]struct{}   // unconfirmed only
}
```

A lease enters the tracker when it is issued and leaves it when the gateway sends
`Handed`. If the stream breaks, **only the unconfirmed leases are released**:

```go
func (n *Node) onStreamClosed(sid streamID) {
    for r := range n.leases.Take(sid) {
        n.releaseLease(r)     // straight back to available, no timeout wait
    }
}
```

The distinction is what keeps this correct. Releasing every lease on the stream would
redeliver messages a consumer is actively working on. Releasing only the ones that
never reached a consumer is free of duplicates.

A lease that was confirmed behaves normally — it expires on its visibility timeout
like any other.

### 3.3 Ack and Nack

Thin wrappers. The receipt names the slot, so there is no lookup:

```go
func (n *Node) Ack(ctx context.Context, r *AckRequest) (*AckResponse, error) {
    if !n.owns(r.Key, r.Receipt.Slot) { return nil, errNotOwner(n.mapVersion()) }
    q, err := n.queue(r.Key)
    if err != nil { return nil, err }
    err = q.Ack(r.Receipt)
    n.leases.Confirm(r.Receipt)          // no longer a candidate for stream release
    return &AckResponse{}, err
}
```

---

## 4. Background work without a goroutine per slot

Four things run on timers: lease expiry, TTL sweeping, delayed release and
snapshots. A goroutine each per slot would be forty thousand goroutines on a node
with ten thousand slots.

Instead there is **one heap of slots keyed by when each next needs attention**, and a
small pool of workers.

```go
type sweeper struct {
    mu    sync.Mutex
    due   dueHeap                  // (deadline, slot), min-heap
    index map[*slot]int
    wake  chan struct{}
    n     int                      // workers, ~GOMAXPROCS/2
}

func (w *sweeper) run() {
    for {
        s, wait := w.next()
        if s == nil {
            select {
            case <-time.After(wait):
            case <-w.wake:
            }
            continue
        }
        dead := s.SweepLeases(w.clock.Now())
        for _, m := range dead { w.dlq(m) }
        w.reschedule(s, s.NextDeadline())
    }
}
```

Each slot already keeps a min-heap of lease deadlines (LLD 1 §5), so `NextDeadline`
is O(1). The node-level heap only ever holds one entry per slot, so it is bounded by
slot count rather than by lease count.

A slot with nothing in flight is not in the heap at all, so an idle queue does no
work. Enqueueing into an idle slot pushes it in.

**TTL sweeps and snapshots are lower frequency and go through a rotating pass**
instead of the heap: every second, one sixtieth of the slots are swept, so each slot
is visited about once a minute and the work is spread evenly rather than arriving as
a spike.

---

## 5. Long polling

```go
type waiterRegistry struct {
    mu      sync.Mutex
    waiting map[QueueKey][]chan struct{}
    count   atomic.Int64            // read on the enqueue path
}

func (r *waiterRegistry) Wake(k QueueKey) {
    if r.count.Load() == 0 { return }     // the common case, one atomic load
    r.mu.Lock()
    for _, ch := range r.waiting[k] { close(ch) }
    delete(r.waiting, k)
    r.mu.Unlock()
}
```

The atomic check first is what keeps enqueue cheap when nobody is waiting, which is
most of the time on a busy queue. Only an idle queue has waiters, and an idle queue
has no enqueue traffic to slow down.

A waiter that times out removes itself. A woken waiter retries the dispatch once and
returns empty if it loses the race, rather than looping — otherwise many waiters woken
by one message would spin against each other.

---

## 6. Slot handoff

The node side of the migration in HLD §9. The coordinator drives it; the node
executes four steps.

```go
// on the losing node
func (n *Node) FreezeSlot(k QueueKey, slot uint16) error {
    s := n.slot(k, slot)
    s.Freeze()          // dequeues return "moved", enqueues forward
    return nil
}

func (n *Node) ShipSlot(k QueueKey, slot uint16, out ChunkStream) error {
    s := n.slot(k, slot)
    snap := s.SnapshotView()        // lock held for a slice copy only (LLD 2 §7)
    return streamSnapshot(snap, out)
}

func (n *Node) ReleaseSlot(k QueueKey, slot uint16) error {
    n.dropSlot(k, slot)             // memory freed, directory deleted
    return nil
}
```

```go
// on the gaining node
func (n *Node) AssumeSlot(k QueueKey, slot uint16, in ChunkStream) error {
    s := newSlot(slot)
    if err := loadSnapshot(s, in); err != nil { return err }
    s.VoidLeases()                  // inherited leases do not carry over
    n.attach(k, slot, s)
    return nil
}
```

**Freezing is per slot, not per placement group.** A placement group can hold many
slots, and freezing them together would be a large stall. The placement map flips for
the whole group at once, but data moves slot by slot, and the losing node keeps
serving slots it has not shipped yet (HLD §9).

**Inherited leases are voided rather than transferred.** A deadline only means
something against the clock that set it (HLD §11).

---

## 7. Backpressure

| Signal | Response |
|---|---|
| Queue at `maxDepth` | Enqueue returns `ResourceExhausted`; gateway maps it to 503 |
| Org over its volume quota | Same, with a different reason code |
| Log cannot accept writes | Enqueue fails; dequeue and ack keep working |
| Sweeper falling behind | Reported as a metric; nothing is rejected |
| Memory above a high-water mark | Enqueue rejected before dequeue is affected |

The last row is deliberate. Under memory pressure the right thing is to stop taking
new work while continuing to hand out and retire what is already accepted, because
that is the path that reduces memory. Rejecting dequeues would make the problem worse.

---

## 8. Failure behaviour

| Event | What the node does |
|---|---|
| etcd unreachable | Keeps serving from the cached placement map; cannot join or rebalance |
| Postgres unreachable | Keeps serving known queues; cannot materialise a new one |
| Gateway stream dies | Releases unconfirmed leases immediately (§3.2) |
| Disk full | Rejects enqueues, keeps serving dequeues and acks |
| Placement map says a slot is no longer ours | Stops serving it, ships or drops it |
| Own liveness key expired but the process is alive | Stops serving and re-registers — it may already have been replaced |

The last row is the leader-lease behaviour from HLD §11 applied locally. A node that
has lost contact with etcd for longer than its lease must assume it has been replaced
rather than assume it is still the owner.

---

## 9. Concurrency rules

Adding to LLD 1 §8, which covers the slot lock:

- `Node.mu` guards only the queue map, and only for lookups. Never held across a
  slot lock, an RPC, or any I/O.
- The lease tracker has its own lock, never held with a slot lock.
- The waiter registry has its own lock, never held with a slot lock.
- The sweeper takes one slot lock at a time and releases it before moving on.

Every lock in the node is a leaf except `Node.mu`, which is only ever taken first.
There are no cycles, so no ordering rules to remember.

---

## 10. Edge cases

| Case | Behaviour |
|---|---|
| Request for a slot we do not own | `FailedPrecondition` plus the map version |
| Request for a queue that does not exist | Config looked up once, then `NotFound` |
| Queue deleted while messages are in flight | Slots discard and release; acks return success |
| Stream dies after `Handed` | Lease behaves normally, expires on its timeout |
| Stream dies before `Handed` | Lease released immediately |
| Two nodes claim the same slot | Prevented by the lock file in the data directory |
| Recovery finds a slot for an unknown queue | Directory deleted during reconciliation |
| Sweeper worker panics | Recovered, slot rescheduled, counted as an error |
| Snapshot in progress during shutdown | Waited for; a partial snapshot is never renamed |
