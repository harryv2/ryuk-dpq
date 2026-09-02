# LLD 1 — Queue Engine

The core data structures and algorithms. Everything else in the system is transport or
coordination around this.

Implements HLD sections 3, 4, 9, 10, 14, 15 and 16.

---

## 1. Position

```
internal/queue/logic/engine/     ← this document
    priority.go   message.go   deque.go    group.go
    slot.go       queue.go     dispatch.go sweep.go
    stats.go      cluster.go   journal.go  clock.go
```

**The engine imports nothing outside the standard library.** No HTTP, no gRPC, no
Postgres, no etcd, no logging framework. That is what lets the concurrency tests run
the real production code path at full speed with no transport, no disk and no real
clock.

Everything it needs from the outside arrives through three small interfaces: `Clock`,
`Journal` and `Cluster`.

---

## 2. Types

### 2.1 Priority

```go
type Priority uint8

const (
    MaxPriority          = 100
    NumBands             = MaxPriority + 1   // 101
    Low         Priority = 25
    Medium      Priority = 50
    High        Priority = 75
)

func (p Priority) Valid() bool { return p <= MaxPriority }
func bucketOf(p Priority) int  { // 0 low, 1 medium, 2 high — for metrics
    switch {
    case p <= 33: return 0
    case p <= 66: return 1
    default:      return 2
    }
}
```

`HIGH`/`MEDIUM`/`LOW` are parsed at the API edge. The engine only sees numbers.

### 2.2 Queue key

```go
type QueueKey struct {
    Org  string
    Name string
}
```

The engine treats the pair as opaque and never parses either half, but it is what the
slot hash is salted with, so two orgs using the same queue name never share a slot.

### 2.3 Message

```go
type Message struct {
    ID           string
    Payload      []byte
    Priority     Priority
    GroupID      string      // "" means the message is its own group
    Seq          uint64      // (generation << 40) | counter
    EnqueuedAt   time.Time
    ExpiresAt    time.Time   // zero: no TTL
    DeliverAfter time.Time   // zero: available immediately
    Attempts     uint32
}

func (m *Message) expired(now time.Time) bool {
    return !m.ExpiresAt.IsZero() && !now.Before(m.ExpiresAt)
}
```

`Seq` orders messages of equal priority sitting in different slots. It is **not** used
for ordering inside a group — that is positional.

The **generation prefix** matters. A queue that changes owner gets a new generation, so
two owners can never hand out overlapping sequence numbers, and messages merged back
from an older owner sort before anything the new owner accepted (HLD §7, §12).

`EnqueuedAt` and `ExpiresAt` are stamped once, by the node that accepts the message,
and written into the log record. Replay reads them rather than recomputing, which is
what makes replay deterministic.

### 2.4 Receipt

```go
type Receipt struct {
    Slot        uint16
    MessageID   string
    Epoch       uint64   // lease generation — rejects a stale ack
    Incarnation uint64   // restart generation — rejects an ack from before a replay
}
```

`Epoch` counts leases within one run. `Incarnation` counts runs.

**Both are needed.** After a crash the engine replays and `epochSeq` restarts from
zero, so a receipt issued before the crash could match a generation issued after it,
and an acknowledgment for a long-dead delivery would delete a message another worker is
actively processing. The incarnation is read from the journal at startup, incremented,
written back, and any receipt from an earlier run is rejected outright.

### 2.5 Errors

```go
var (
    ErrNotInFlight  = errors.New("queue: message not in flight")
    ErrLeaseExpired = errors.New("queue: lease expired, message was redelivered")
    ErrBadReceipt   = errors.New("queue: malformed receipt")
    ErrBadPriority  = errors.New("queue: priority out of range")
    ErrQueueFull    = errors.New("queue: at maxDepth")
    ErrFrozen       = errors.New("queue: frozen, migrating")
)
```

---

## 3. The deque

Bands and groups are both first-in-first-out. One type serves both.

```go
type deque[T any] struct {
    buf  []T
    head int
}

func (d *deque[T]) len() int    { return len(d.buf) - d.head }
func (d *deque[T]) empty() bool { return d.len() == 0 }
func (d *deque[T]) pushBack(v T) { d.buf = append(d.buf, v) }

func (d *deque[T]) pushFront(v T) {
    if d.head > 0 {
        d.head--
        d.buf[d.head] = v
        return
    }
    d.buf = append([]T{v}, d.buf...)
}

func (d *deque[T]) popFront() (v T, ok bool) {
    if d.head == len(d.buf) { return v, false }
    v = d.buf[d.head]
    var zero T
    d.buf[d.head] = zero                    // drop the reference so GC can collect
    d.head++
    if d.head > 32 && d.head*2 >= len(d.buf) {
        d.buf = append(d.buf[:0], d.buf[d.head:]...)
        d.head = 0
    }
    return v, true
}

func (d *deque[T]) front() (v T, ok bool) {
    if d.head == len(d.buf) { return v, false }
    return d.buf[d.head], true
}
```

Two details matter.

**Zeroing on pop.** Without it the backing array keeps pointers to popped messages
alive and a busy queue leaks memory in proportion to throughput.

**Compaction on pop.** Advancing `head` alone never reclaims the front of the array.
Copying down once the dead prefix is half the array keeps it amortised O(1).

`pushFront` on a full front is O(n), but it happens only on retry, and only when the
group has never been popped from.

---

## 4. Group

```go
type group struct {
    id      string
    msgs    deque[*Message]
    locked  bool       // a message from this group is in flight
    inBand  bool       // a live band entry exists for this group
    band    Priority   // which band that entry is in
    version uint64     // invalidates older band entries
}
```

A group with no `groupID` is created with `id = message.ID`, so it holds exactly one
message and can never block anything.

`version` avoids removing entries from the middle of a band deque. A band entry is
valid only if its version matches the group's current version, so bumping the version
kills every older entry without touching them.

---

## 5. Slot

```go
type slot struct {
    id uint16

    mu       sync.Mutex
    bandMask [2]uint64                        // which priorities are non-empty
    bands    map[Priority]*deque[bandEntry]   // only priorities in use
    groups   map[string]*group
    inflight map[string]*lease
    timers   timerHeap
    delayed  delayHeap
    epochSeq uint64
    st       slotStats

    // read without the lock by the dispatcher
    hint    atomic.Uint32   // highest non-empty band + 1; 0 means empty
    headSeq atomic.Uint64   // Seq of the head message in that band
}

type bandEntry struct { g *group; ver uint64 }

type lease struct {
    msg      *Message
    g        *group
    epoch    uint64
    deadline time.Time
}
```

One lock covers everything above it. Handing out a message mutates five structures and
they must move together (HLD §10).

**A slot belongs to one queue, and therefore to one org.** Nothing here is shared
between tenants, so no hash collision can put two orgs behind the same mutex.

**Why `bands` is a map and not `[101]deque`.** A dense array costs 101 deque headers —
3,232 bytes — per slot, allocated whether used or not. Real queues use a handful of
priorities, so nearly all of it is waste, and it is paid 16 times per queue. A map costs
about 200 bytes for three priorities and shrinks when they empty.

The bitmap stays dense because it is 16 bytes and it is what makes selection constant
time. The map is consulted only after the bitmap names the priority, so the extra cost
is one map lookup on a path whose next step is a network hop.

### 5.1 Bitmap

```go
func (s *slot) setBand(p Priority)   { s.bandMask[p>>6] |= 1 << (p & 63) }
func (s *slot) clearBand(p Priority) { s.bandMask[p>>6] &^= 1 << (p & 63) }

func (s *slot) highestBand() (Priority, bool) {
    if w := s.bandMask[1]; w != 0 {
        return Priority(127 - bits.LeadingZeros64(w)), true    // 64..100
    }
    if w := s.bandMask[0]; w != 0 {
        return Priority(63 - bits.LeadingZeros64(w)), true     // 0..63
    }
    return 0, false
}
```

Word 1 holds priorities 64–100, word 0 holds 0–63. Constant time regardless of how many
priority levels exist.

**The bitmap is a superset.** A set bit means the band deque is non-empty; it does not
mean the band will yield a message, because entries may be stale or their groups locked.
There are never false negatives, so nothing is missed. A band that yields nothing is
handled by moving to the next.

### 5.2 Band membership

```go
// caller holds s.mu
func (s *slot) pushGroup(g *group) {
    m, ok := g.msgs.front()
    if !ok || g.locked { return }
    g.version++
    g.band = m.Priority
    g.inBand = true

    d := s.bands[m.Priority]
    if d == nil { d = &deque[bandEntry]{}; s.bands[m.Priority] = d }
    d.pushBack(bandEntry{g: g, ver: g.version})
    s.setBand(m.Priority)
    s.refreshHint()
}

// caller holds s.mu
func (s *slot) takeGroup(p Priority) *group {
    d := s.bands[p]
    if d == nil { s.clearBand(p); return nil }
    for {
        e, ok := d.popFront()
        if !ok { break }
        if e.ver != e.g.version { continue }   // stale: the group moved or was taken
        e.g.inBand = false                     // this was the live entry
        if e.g.locked || e.g.msgs.empty() { continue }
        if d.empty() { s.dropBand(p) }
        return e.g
    }
    s.dropBand(p)
    return nil
}

// caller holds s.mu
func (s *slot) dropBand(p Priority) {
    delete(s.bands, p)      // release the deque so an idle slot shrinks
    s.clearBand(p)
}
```

Setting `inBand = false` **only after** the version check is load-bearing. A stale entry
means the group already has a newer live entry elsewhere; clearing the flag there would
let `pushGroup` add a second one, and the group would be dispatched twice — the same
message to two workers.

---

## 6. Operations

### 6.1 Enqueue

```go
// caller holds s.mu
func (s *slot) enqueue(m *Message, now time.Time) {
    if !m.DeliverAfter.IsZero() && m.DeliverAfter.After(now) {
        heap.Push(&s.delayed, delayEntry{msg: m, at: m.DeliverAfter})
        s.st.delayed++
        return
    }
    gid := m.GroupID
    if gid == "" { gid = m.ID }

    g := s.groups[gid]
    if g == nil { g = &group{id: gid}; s.groups[gid] = g }
    g.msgs.pushBack(m)

    s.st.enqueued++
    s.st.ready[bucketOf(m.Priority)]++
    s.st.bytes += int64(len(m.Payload))

    if !g.locked && !g.inBand { s.pushGroup(g) }
}
```

O(1). A group already in a band or already locked needs no band work — its existing
entry, or its eventual unlock, picks the message up.

### 6.2 Take

```go
// caller holds s.mu
func (s *slot) take(p Priority, now time.Time, vis time.Duration) (*Message, Receipt, bool) {
    g := s.takeGroup(p)
    if g == nil { return nil, Receipt{}, false }

    var m *Message
    for {
        v, ok := g.msgs.popFront()
        if !ok { break }
        if v.expired(now) { s.retireExpired(v); continue }
        m = v
        break
    }
    if m == nil {
        s.dropGroupIfIdle(g)
        return nil, Receipt{}, false          // caller retries
    }

    m.Attempts++
    s.epochSeq++
    g.locked = true

    l := &lease{msg: m, g: g, epoch: s.epochSeq, deadline: now.Add(vis)}
    s.inflight[m.ID] = l
    heap.Push(&s.timers, timerEntry{id: m.ID, epoch: l.epoch, at: l.deadline})

    s.st.ready[bucketOf(m.Priority)]--
    s.st.inflight++
    s.refreshHint()

    return m, Receipt{Slot: s.id, MessageID: m.ID, Epoch: l.epoch}, true
}
```

TTL is checked here rather than by a scan, so an expired message costs one pop. The
active TTL sweep exists to keep the oldest-age metric honest, not for correctness.

`Incarnation` is filled by the layer above, which owns it.

### 6.3 Acknowledge

```go
// caller holds s.mu
func (s *slot) ack(r Receipt) error {
    l, ok := s.inflight[r.MessageID]
    if !ok  { return ErrNotInFlight }   // already acked, dead-lettered, or never existed
    if l.epoch != r.Epoch { return ErrLeaseExpired }   // redelivered — do not delete

    delete(s.inflight, r.MessageID)
    s.st.inflight--
    s.st.acked++
    s.st.bytes -= int64(len(l.msg.Payload))
    s.unlock(l.g)
    return nil
}

// caller holds s.mu
func (s *slot) unlock(g *group) {
    g.locked = false
    if g.msgs.empty() { s.dropGroupIfIdle(g); return }
    if !g.inBand      { s.pushGroup(g) }
}

// caller holds s.mu
func (s *slot) dropGroupIfIdle(g *group) {
    if !g.locked && g.msgs.empty() && !g.inBand { delete(s.groups, g.id) }
}
```

The epoch check is the whole stale-acknowledgment defence. Without it, a worker that
stalls past its lease and acknowledges late deletes a message another worker is
processing.

Groups are deleted once idle. Without that, a queue with a unique group ID per message
leaks one map entry per message forever.

---

## 7. Sweeps

### 7.1 Lease expiry

```go
// caller holds s.mu
func (s *slot) sweepLeases(now time.Time, maxRetries uint32) (dead []*Message) {
    for s.timers.Len() > 0 && !s.timers[0].at.After(now) {
        e := heap.Pop(&s.timers).(timerEntry)
        l, ok := s.inflight[e.id]
        if !ok || l.epoch != e.epoch { continue }   // acked, or re-leased since
        delete(s.inflight, e.id)
        s.st.inflight--
        if m := s.retire(l, now, maxRetries, 0); m != nil { dead = append(dead, m) }
    }
    s.refreshHint()
    return dead
}

// caller holds s.mu. Returns non-nil if the message must be dead-lettered.
func (s *slot) retire(l *lease, now time.Time, maxRetries uint32, delay time.Duration) *Message {
    g, m := l.g, l.msg
    g.locked = false

    switch {
    case m.expired(now):
        s.retireExpired(m); s.dropGroupIfIdle(g); return nil
    case m.Attempts >= maxRetries:
        s.st.deadLettered++
        s.st.bytes -= int64(len(m.Payload))
        s.dropGroupIfIdle(g)
        return m
    }

    if delay > 0 {
        m.DeliverAfter = now.Add(delay)
        heap.Push(&s.delayed, delayEntry{msg: m, at: m.DeliverAfter})
        s.st.delayed++; s.st.requeued++
        s.dropGroupIfIdle(g)
        return nil
    }

    g.msgs.pushFront(m)                        // ← FRONT of the group
    s.st.ready[bucketOf(m.Priority)]++
    s.st.requeued++
    if !g.inBand { s.pushGroup(g) }            // ← TAIL of the band
    return nil
}
```

**The asymmetry in `retire` is the important part.** A retried message goes back to the
**front of its group**, because it was the group's head and group ordering is strict.
The **group** goes to the **tail of its band**, so a repeatedly failing message does not
block the head of its priority level on every cycle.

Both halves of HLD §16's retry rule are in those three lines.

The timer heap uses lazy deletion. An acknowledged message leaves its entry in place; it
is discarded when it surfaces and its epoch no longer matches. Heap size is bounded by
leases ever created rather than leases outstanding, and it drains as time passes.

### 7.2 TTL

```go
// caller holds s.mu
func (s *slot) sweepTTL(now time.Time) int {
    n := 0
    for _, g := range s.groups {
        for {
            m, ok := g.msgs.front()
            if !ok || !m.expired(now) { break }
            g.msgs.popFront(); s.retireExpired(m); n++
        }
        if !g.inBand && !g.locked && !g.msgs.empty() { s.pushGroup(g) }
        s.dropGroupIfIdle(g)
    }
    s.refreshHint()
    return n
}
```

Only the front of each group is examined. Messages behind a live head are removed when
they reach the front. This keeps the oldest-message-age metric honest without walking
every message.

### 7.3 Delayed release

```go
// caller holds s.mu
func (s *slot) releaseDelayed(now time.Time) int {
    n := 0
    for s.delayed.Len() > 0 && !s.delayed[0].at.After(now) {
        e := heap.Pop(&s.delayed).(delayEntry)
        e.msg.DeliverAfter = time.Time{}
        s.st.delayed--
        s.enqueue(e.msg, now)
        n++
    }
    return n
}
```

---

## 8. Queue and dispatch

```go
type Config struct {
    Key                 QueueKey
    VisibilityTimeout   time.Duration
    MaxRetries          uint32
    DefaultTTL          time.Duration
    StarvationThreshold time.Duration
    StarvationReserve   float64        // 0..1, default 0.2
    MaxDepth            int64
    Distributed         bool
}

type Queue struct {
    cfg     Config
    clock   Clock
    cluster Cluster
    journal Journal

    slots   map[uint16]*slot          // lazily created
    slotsMu sync.RWMutex

    generation  uint64                // from the placement record
    incarnation uint64                // from the journal
    counter     atomic.Uint64         // low bits of Seq
    dispatch    atomic.Uint64         // drives the starvation reserve
    rr          atomic.Uint64         // round robin for ungrouped messages
    cursor      atomic.Uint32         // rotating start for the starvation scan
    depth       atomic.Int64
    frozen      atomic.Bool           // set during a migration

    dlq *Queue
}

const (
    SlotsPerQueue    = 16   // a normal queue
    MaxSlotsPerQueue = 64   // a distributed queue
)

func SlotCountFor(distributed bool) int {
    if distributed {
        return MaxSlotsPerQueue
    }
    return SlotsPerQueue
}
```

### 8.1 Sequence numbers

```go
func (q *Queue) nextSeq() uint64 {
    return (q.generation << 40) | (q.counter.Add(1) & (1<<40 - 1))
}
```

Twenty-four bits of generation and forty of counter — a trillion messages per
generation, sixteen million ownership changes. The prefix is what stops two owners
handing out overlapping values.

### 8.2 Cluster

```go
type Cluster interface {
    // SlotFor maps a group key to one of the queue's slots.
    SlotFor(q QueueKey, groupID string, slots int) uint16
    // LocalSlots lists the slots of this queue that this process owns.
    LocalSlots(q QueueKey, slots int) []uint16
}
```

```go
func (c *localCluster) SlotFor(q QueueKey, groupID string, slots int) uint16 {
    return uint16(hash(q.Org+"\x00"+q.Name+"\x00"+groupID) % uint64(slots))
}
```

The count is passed in rather than read from a constant because the two queue types
have different counts. The `Queue` knows its own from `cfg.Distributed`, and it never
changes: a group key has to keep resolving to the same slot.

The separator byte matters. Without it, org `a` with queue `bc` and org `ab` with queue
`c` hash identically, and two tenants would share slots.

**`LocalSlots` is where the `distributed` flag lands in the engine.** For a normal queue
the owning node holds all 64, so it returns all 64 and the dispatcher below sees every
slot. For a distributed queue it returns only the slots this node owns, and the
dispatcher cannot compare against the rest.

That single difference is the entire cost of the flag inside the engine. Nothing else in
this document changes.

### 8.3 Slot selection at enqueue

```go
func (q *Queue) slotFor(m *Message) uint16 {
    if m.GroupID != "" {
        return q.cluster.SlotFor(q.cfg.Key, m.GroupID)   // same group, same slot, always
    }
    local := q.cluster.LocalSlots(q.cfg.Key)
    return local[q.rr.Add(1)%uint64(q.fanout(len(local)))]
}

// fanout grows the number of slots an ungrouped message can land in,
// in proportion to how deep the queue is.
func (q *Queue) fanout(n int) int {
    d, want := q.depth.Load(), 1
    for want < n && int64(want)*msgsPerSlotTarget < d { want *= 2 }
    return want
}

const msgsPerSlotTarget = 1000
```

Round-robin across every slot from the first message is wrong. A queue holding a
thousand messages would scatter them over every lock-striped structure for parallelism
it does not need, materialise them all, and make cross-slot comparison harder for
nothing.

This is safe only because ungrouped messages carry no ordering constraint — nothing
breaks when the fan-out changes underneath them. Grouped messages always hash.

### 8.4 The dispatcher

```go
func (q *Queue) Dequeue() (*Message, Receipt, bool) {
    now := q.clock.Now()
    n := q.dispatch.Add(1)

    if q.reserveTick(n) {
        if m, r, ok := q.takeStarved(now); ok {
            q.st.escapes.Add(1)
            return m, r, true
        }
    }
    return q.takeUrgent(now)
}

func (q *Queue) reserveTick(n uint64) bool {
    if q.cfg.StarvationReserve <= 0 { return false }
    every := uint64(1.0 / q.cfg.StarvationReserve)   // 0.2 → every 5th
    return n%every == 0
}
```

### 8.5 Most urgent, without locking every slot

```go
func (q *Queue) takeUrgent(now time.Time) (*Message, Receipt, bool) {
    for attempt := 0; attempt < maxDispatchRetries; attempt++ {
        var best *slot
        var bestBand Priority
        var bestSeq uint64 = math.MaxUint64

        for _, s := range q.localSlots() {
            h := s.hint.Load()
            if h == 0 { continue }
            band, seq := Priority(h-1), s.headSeq.Load()
            if band > bestBand || (band == bestBand && seq < bestSeq) {
                best, bestBand, bestSeq = s, band, seq
            }
        }
        if best == nil { return nil, Receipt{}, false }

        best.mu.Lock()
        m, r, ok := best.take(bestBand, now, q.cfg.VisibilityTimeout)
        best.mu.Unlock()
        if ok { return m, r, true }
        // the slot changed under us, or that band held only stale entries
    }
    return nil, Receipt{}, false
}
```

Two atomic loads per slot, then one lock on the winner. Sixty-four atomic loads, the
worst case, is a few dozen nanoseconds; sixty-four mutex acquisitions would not be.

`hint` and `headSeq` are read separately and can be momentarily inconsistent. That is
fine: they steer the choice, and `take` verifies under the lock. A mismatch costs one
retry.

**Comparing `headSeq` at equal priority is what makes FIFO exact.** For a normal queue
every slot is in this process, so this loop sees all 64 and picks the genuinely oldest
message at the highest non-empty priority.

**For a distributed queue `localSlots` returns a subset**, so the comparison cannot span
the rest and ordering is exact within a slot but approximate across machines. The whole
cost of the flag appears in this one loop.

### 8.6 The starvation path

```go
func (q *Queue) takeStarved(now time.Time) (*Message, Receipt, bool) {
    cutoff := now.Add(-q.cfg.StarvationThreshold)
    slots := q.localSlots()
    start := int(q.cursor.Add(1)) % len(slots)

    for i := 0; i < len(slots) && i < starvationScanLimit; i++ {
        s := slots[(start+i)%len(slots)]
        s.mu.Lock()
        p, ok := s.oldestBandBefore(cutoff)
        if !ok { s.mu.Unlock(); continue }
        m, r, taken := s.take(p, now, q.cfg.VisibilityTimeout)
        s.mu.Unlock()
        if taken { return m, r, true }
    }
    return nil, Receipt{}, false
}
```

`oldestBandBefore` walks set bits from the lowest priority upward and returns the first
band whose head was enqueued before the cutoff. Low priorities are checked first because
that is where starvation happens.

The scan is capped and starts from a rotating cursor, so the work is bounded while every
slot is still reached over time. This path runs on `StarvationReserve` of dispatches.

---

## 9. Migration support

The engine does not move data — the layer above does. It provides two operations.

```go
// Freeze stops the engine serving. Enqueue and Dequeue return ErrFrozen.
// In-flight leases are voided and their messages return to available.
func (q *Queue) Freeze() []*Message      // snapshot, in Seq order

// Absorb merges messages into a live queue, inserting by Seq rather than
// appending, so messages from an older generation land in the right place.
func (q *Queue) Absorb(msgs []*Message) error
```

`Freeze` returns everything in `Seq` order so the receiving side can insert cheaply.
`Absorb` handles both cases from HLD §12 and §13: a migration into a fresh queue, where
every message is new, and a merge into a running queue after a node returns, where
older-generation messages must sort ahead of newer ones.

Insertion by `Seq` is what makes the merge correct. Appending would put recovered
messages behind everything accepted during the outage.

---

## 10. Invariants

Asserted by the property tests in HLD §21.

1. A message is in exactly one of: a group deque, `inflight`, or `delayed`.
2. `g.locked` is true if and only if some lease in `inflight` points at `g`.
3. If `g.inBand`, exactly one entry in `bands[g.band]` has `ver == g.version`.
4. `bandMask` bit `p` is set if `bands[p]` is non-empty. The converse may not hold —
   the bitmap is a superset, never a subset.
5. A group in `s.groups` has at least one message, or is locked, or is in a band.
6. Within a group, `msgs` is in submission order at all times, including after retry
   and after `Absorb`.
7. `epochSeq` never decreases within a run; `Incarnation` never decreases across runs;
   `generation` never decreases across owners.

---

## 11. Concurrency rules

- **One lock per slot**, covering every field of that slot.
- **Never hold two slot locks at once.** No cross-slot transaction exists, so the lock
  graph has no cycles and deadlock is impossible.
- **Never do I/O under a slot lock.** The journal append happens before the lock.
- `slotsMu` is held only for the map lookup that finds a slot.
- Sweeps take one slot's lock at a time and release it before moving on.

---

## 12. Complexity

| Operation | Cost |
|---|---|
| Enqueue | O(1) |
| Dequeue, urgent path | O(S) atomic loads + O(1) under one lock |
| Dequeue, starvation path | O(min(S, 16)) locks, on a fraction of dispatches |
| Acknowledge | O(1) |
| Lease sweep | O(k log T) for k expired |
| TTL sweep | O(G) groups |
| Delayed release | O(k log D) |
| Freeze | O(N) messages |
| Absorb | O(N log N) |

S = local slots (≤64), T = timer heap, D = delay heap, G = groups, N = messages moved.

**Nothing on the request path scales with queue depth.** That is the property the
latency budget depends on.

### Memory

| Part of a live slot | Bytes |
|---|---|
| `bandMask` | 16 |
| `bands`, three priorities in use | ~200 |
| `groups`, `inflight` map headers | ~96 |
| `timers`, `delayed` slice headers | 48 |
| mutex, counters, hints, id | ~80 |
| **Total, before any message** | **~440** |

The two counts are set by different things.

A normal queue's slots are only lock stripes, and a benchmark of the full enqueue,
dequeue and acknowledge cycle shows the gain is flat past four (1707 ns at one slot,
1187 at four, 1171 at sixteen, 1150 at sixty-four). Sixteen sits past that knee with
margin for a machine with many cores, and costs a quarter of what sixty-four would.

A distributed queue's count is also the ceiling on how many machines it can use, and
sixty-four keys spread evenly across a handful of machines where sixteen are lumpy.
Fan-out costs there are bounded by machine count rather than slot count, so the larger
number is close to free.

The engine imports nothing outside the standard library, so these constants cannot
be shared with `backend/constants`. A test in `queue/logic` asserts the two agree,
and `EnqueueToSlot` rejects a slot outside the queue's range: without both, the gateway
could route a message to a slot the dispatcher never scans, and it would be
written to the log and then never delivered.

A dense `[101]deque` would add 3,232 bytes to every one. Slots are created on first use,
so an idle queue costs nothing.

---

## 13. Counters and stats

```go
type slotStats struct {
    ready    [3]int64   // bucketed low / medium / high
    inflight int64
    delayed  int64
    bytes    int64
    enqueued, acked, expired, requeued, deadLettered uint64   // cumulative
}

// Stats reads every slot under its own lock, sequentially.
func (q *Queue) Stats(now time.Time) QueueStats
```

`ready` is bucketed into three rather than kept per level, because that is the
granularity metrics are reported at and 101 counters per slot would cost more than the
bands themselves.

None of this is written to the log. After a crash the node replays and rebuilds, and the
counters come out of the rebuilt state.

**For a normal queue `Stats` is exact.** All 16 slots are here, each counter is read
under the lock that maintains it, and the whole call is microseconds. Reads are
sequential, not simultaneous, because holding two slot locks at once is forbidden — the
gap between first and last is a few microseconds, under one message at any realistic
rate.

`OldestAge` aggregates as a **maximum** across slots, never a sum, and is clamped at
zero so a clock adjustment cannot produce a negative age.

---

## 14. Edge cases

| Case | Behaviour |
|---|---|
| Acknowledge after the lease expired | `ErrLeaseExpired` — epoch mismatch |
| Acknowledge with a receipt from before a restart | Rejected on incarnation |
| Acknowledge with a receipt from before a move | Rejected on generation, in `Seq` |
| Acknowledge twice | `ErrNotInFlight`; the API layer maps it to 200 |
| Every group in a slot is locked | `take` returns false; the dispatcher moves on |
| Head of a group is TTL-expired | Popped and skipped inside `take` |
| Every message in a group expires | Group deleted, caller retries |
| `MaxRetries` is 0 | Dead-letters on the first lease expiry |
| Ungrouped messages | `gid = m.ID`, a group of one, never blocked |
| Group spanning priorities | Sits in the band of its current head, re-banded on unlock |
| Band holds only stale entries | `takeGroup` drains them and clears the bit |
| Delay longer than the TTL | Released, then dropped as expired at `take` |
| Queue at `MaxDepth` | `ErrQueueFull`; the API layer returns 503 |
| Enqueue or dequeue while frozen | `ErrFrozen`; the gateway retries at the new owner |
| Absorb of an older generation | Inserted by `Seq`, ahead of newer messages |
| Two orgs, same queue name | Different `QueueKey`, different slots, no shared state |
| Clock moves backwards | Leases use monotonic readings, unaffected |

---

## 15. What the layer above must provide

```go
type Clock interface{ Now() time.Time }

type Journal interface {
    // Incarnation is read once at startup, after replay.
    Incarnation() uint64

    // AppendEnqueue writes and flushes before returning. Its error fails the
    // enqueue, so nothing becomes visible that is not already in the log.
    AppendEnqueue(slot uint16, m *Message) error

    // Buffered, flushed on the journal's own schedule. Losing them costs at
    // most one extra retry or one redelivery.
    AppendAttempt(slot uint16, id string, n uint32, epoch uint64)
    AppendTerminal(slot uint16, id string, kind TerminalKind)
}
```

Only `AppendEnqueue` is on the critical path, and it is what makes HLD §11's ordering
real: write, flush, then apply in memory. The others are buffered because flushing every
delivery would double the cost of the most common operation.

The no-op journal returns incarnation 0 and discards everything, which is what lets the
engine be tested with no disk at all. `FakeClock` is a hand-written struct with an
`Advance` method — the only substitution the tests need.

---

## 16. The other documents

| # | Component | Covers |
|---|---|---|
| 2 | Write-ahead log | Record format, segments, CRC, replay, snapshot |
| 3 | Node | Slot ownership, gRPC surface, sweepers, notifications, migration |
| 4 | Gateway | REST, routing, config cache, long polling, metric collection |
| 5 | Placement and metadata | Postgres schema, etcd membership, rendezvous, rebalancing |
| 6 | Frontend and harnesses | UI, producer and consumer stubs |
