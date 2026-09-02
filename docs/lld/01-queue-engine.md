# LLD 1 — Queue Engine

The core data structures and algorithms. Everything else in the system is transport
or coordination around this.

Implements: HLD Sections 3, 7, 8, 12, 13, 14.

---

## 1. Package layout

```
internal/
  queue/            ← this document
    priority.go     Priority type, bands, bitmap
    message.go      Message, Receipt, errors
    deque.go        ring deque used by bands and groups
    group.go        group state
    slot.go         slot: bands, groups, in-flight, timers
    queue.go        Queue: slot set, dispatch, config
    dispatch.go     priority selection and the starvation reserve
    sweep.go        lease expiry, TTL, delayed release
    clock.go        Clock interface, system and fake
  wal/              LLD 2
  node/             LLD 3
  gateway/          LLD 4
  meta/             LLD 5
  metrics/          LLD 6
cmd/
  ryuk-node/
  ryuk-gateway/
```

`internal/queue` has no imports outside the standard library. It knows nothing about
HTTP, RPC, Postgres, or etcd. That is what lets the test suite drive it directly at
full speed with no transport in the way.

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
```

`HIGH`/`MEDIUM`/`LOW` are parsed at the API edge into these values. The engine only
ever sees numbers.

### 2.2 Queue key

```go
type QueueKey struct {
    Org  string
    Name string
}

func (k QueueKey) String() string { return k.Org + "/" + k.Name }
```

A queue is identified by org and name together. The engine treats the pair as opaque
— it never parses either half — but the pair is what the slot and
placement-group hashes are salted with, so two orgs using the same queue name never share a slot.

### 2.3 Message

```go
type Message struct {
    ID           string
    Payload      []byte
    Priority     Priority
    GroupID      string      // "" means the message is its own group
    Seq          uint64      // queue-wide submission order
    EnqueuedAt   time.Time
    ExpiresAt    time.Time   // zero: no TTL
    DeliverAfter time.Time   // zero: available immediately
    Attempts     uint32
}

func (m *Message) expired(now time.Time) bool {
    return !m.ExpiresAt.IsZero() && !now.Before(m.ExpiresAt)
}
```

`Seq` comes from one counter per queue, assigned at enqueue. It exists only to
compare messages of equal priority sitting in different slots (HLD §12). It is not
used for ordering inside a group — that is positional.

### 2.4 Receipt

```go
type Receipt struct {
    Slot        uint16
    MessageID   string
    Epoch       uint64   // lease generation, rejects a stale ack
    Incarnation uint64   // restart generation, rejects an ack from before a replay
}
```

`Epoch` counts leases within one run of the process. `Incarnation` counts runs.

Both are needed. After a crash the engine replays its log and `epochSeq` restarts
from zero, so a receipt issued before the crash could match a generation issued
after it — and an acknowledgment for a long-dead delivery would delete a message
another worker is actively processing. The incarnation is read from the log at
startup, incremented, and written back, so any receipt from an earlier run is
rejected outright.

### 2.5 Errors

```go
var (
    ErrNotInFlight  = errors.New("queue: message not in flight")
    ErrLeaseExpired = errors.New("queue: lease expired, message was redelivered")
    ErrBadReceipt   = errors.New("queue: malformed receipt")
    ErrBadPriority  = errors.New("queue: priority out of range")
    ErrQueueFull    = errors.New("queue: at maxDepth")
)
```

---

## 3. The deque

Bands and groups are both FIFO. One type serves both.

```go
type deque[T any] struct {
    buf  []T
    head int
}

func (d *deque[T]) len() int  { return len(d.buf) - d.head }
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
    d.buf[d.head] = zero        // release the reference so GC can collect
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

Two details that matter.

**Zeroing on pop.** Without it the backing array keeps pointers to popped messages
alive and a busy queue leaks memory in proportion to throughput.

**Compaction on pop.** `d.buf[d.head:]` alone never reclaims the front of the array.
Copying down once the dead prefix is half the array keeps it amortised O(1) with
bounded waste.

`pushFront` on a full front is O(n), but it only happens on retry, and only when the
group has never been popped from — rare enough not to matter.

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

A group with no `groupID` is created with `id = message.ID`, so it contains exactly
one message and can never block anything.

`version` is the mechanism that avoids removing entries from the middle of a band
deque. A band entry is valid only if its recorded version matches the group's current
version. Bumping the version makes every older entry dead without touching them.

---

## 5. Slot

```go
type slot struct {
    id uint16

    mu       sync.Mutex
    bandMask [2]uint64                        // which priorities are non-empty
    bands    map[Priority]*deque[bandEntry]   // only priorities actually in use
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

type bandEntry struct {
    g   *group
    ver uint64
}

type lease struct {
    msg      *Message
    g        *group
    epoch    uint64
    deadline time.Time
}
```

One lock covers everything above it. Rationale in HLD §8: handing out a message
mutates five structures and they must move together.

**A slot belongs to one queue, and therefore to one org.** Nothing above is ever
shared between tenants, so no hash collision can put two orgs behind the same mutex.

**All 16 slots of a normal queue are in this process**, so the dispatcher in §8 can
compare every one of them. That is what makes priority and FIFO exact rather than
best-effort.

**Why `bands` is a map and not `[101]deque`.** A dense array costs 101 deque headers
— 3,232 bytes — per slot, allocated whether used or not. Real queues use a handful of
distinct priorities, so almost all of it is waste, and it is paid 16 times per queue.
At a million queues that is 218 GB of empty arrays before a single message exists. A
map costs about 200 bytes for three priorities and shrinks when they empty.

The bitmap stays dense, because it is 16 bytes and it is what makes selection
constant time. The map is only consulted after the bitmap has already named the
priority, so the extra cost is one map lookup (~20 ns) on a path whose next step is a
network hop.

### 5.1 Bitmap

```go
func (s *slot) setBand(p Priority)   { s.bandMask[p>>6] |= 1 << (p & 63) }
func (s *slot) clearBand(p Priority) { s.bandMask[p>>6] &^= 1 << (p & 63) }

func (s *slot) highestBand() (Priority, bool) {
    if w := s.bandMask[1]; w != 0 {
        return Priority(127 - bits.LeadingZeros64(w)), true    // covers 64..100
    }
    if w := s.bandMask[0]; w != 0 {
        return Priority(63 - bits.LeadingZeros64(w)), true     // covers 0..63
    }
    return 0, false
}
```

Word 1 holds priorities 64–100, word 0 holds 0–63. Two instructions, constant time
regardless of how many priority levels exist.

**The bitmap is a superset.** A set bit means the band deque is non-empty; it does
not mean the band will yield a message, because the entries may all be stale or
belong to locked groups. There are never false negatives, so nothing is ever missed.
Callers handle a band that yields nothing by moving to the next one.

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
    if d == nil {
        d = &deque[bandEntry]{}
        s.bands[m.Priority] = d
    }
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
        if e.ver != e.g.version {
            continue                 // stale entry, the group moved or was taken
        }
        e.g.inBand = false           // this was the live entry
        if e.g.locked || e.g.msgs.empty() {
            continue
        }
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

Setting `inBand = false` **only after** the version check is load-bearing. A stale
entry means the group already has a newer live entry elsewhere; clearing the flag
there would let `pushGroup` add a second one, and the group would be dispatched
twice.

---

## 6. Operations

### 6.1 Enqueue

```go
// caller holds s.mu
func (s *slot) enqueue(m *Message, now time.Time) {
    if !m.DeliverAfter.IsZero() && m.DeliverAfter.After(now) {
        heap.Push(&s.delayed, delayEntry{msg: m, at: m.DeliverAfter})
        return
    }
    gid := m.GroupID
    if gid == "" { gid = m.ID }

    g := s.groups[gid]
    if g == nil {
        g = &group{id: gid}
        s.groups[gid] = g
    }
    g.msgs.pushBack(m)
    s.st.enqueued++

    if !g.locked && !g.inBand {
        s.pushGroup(g)
    }
}
```

O(1). A group already in a band or already locked needs no band work — its existing
entry or its eventual unlock will pick the message up.

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
        if v.expired(now) { s.st.expired++; continue }
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
    s.refreshHint()

    return m, Receipt{Slot: s.id, MessageID: m.ID, Epoch: l.epoch}, true
}
```

TTL is checked here rather than by a scan, so an expired message costs one pop.
The active TTL sweep in §7.2 exists for the metrics, not for correctness.

### 6.3 Acknowledge

```go
// caller holds s.mu
func (s *slot) ack(r Receipt) error {
    l, ok := s.inflight[r.MessageID]
    if !ok {
        return ErrNotInFlight       // already acked, dead-lettered, or never existed
    }
    if l.epoch != r.Epoch {
        return ErrLeaseExpired      // redelivered; do NOT delete the new delivery
    }
    delete(s.inflight, r.MessageID)
    s.st.acked++
    s.unlock(l.g)
    return nil
}

// caller holds s.mu
func (s *slot) unlock(g *group) {
    g.locked = false
    if g.msgs.empty() {
        s.dropGroupIfIdle(g)
        return
    }
    if !g.inBand { s.pushGroup(g) }
}

// caller holds s.mu
func (s *slot) dropGroupIfIdle(g *group) {
    if !g.locked && g.msgs.empty() && !g.inBand {
        delete(s.groups, g.id)
    }
}
```

The epoch check is the whole stale-acknowledgment defence. Without it, a worker that
stalls past its lease and acknowledges late deletes a message another worker is
actively processing.

Groups are deleted once idle. Without that, a queue with unique group IDs per
message leaks one map entry per message forever.

### 6.4 Negative acknowledge

```go
// caller holds s.mu
func (s *slot) nack(r Receipt, delay time.Duration, now time.Time, maxRetries uint32) (*Message, error) {
    l, ok := s.inflight[r.MessageID]
    if !ok { return nil, ErrNotInFlight }
    if l.epoch != r.Epoch { return nil, ErrLeaseExpired }
    delete(s.inflight, r.MessageID)
    return s.retire(l, now, maxRetries, delay), nil
}
```

---

## 7. Sweeps

### 7.1 Lease expiry

```go
// caller holds s.mu
func (s *slot) sweepLeases(now time.Time, maxRetries uint32) (dead []*Message) {
    for s.timers.Len() > 0 && !s.timers[0].at.After(now) {
        e := heap.Pop(&s.timers).(timerEntry)
        l, ok := s.inflight[e.id]
        if !ok || l.epoch != e.epoch {
            continue                 // acked already, or re-leased since
        }
        delete(s.inflight, e.id)
        if m := s.retire(l, now, maxRetries, 0); m != nil {
            dead = append(dead, m)
        }
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
        s.st.expired++
        s.dropGroupIfIdle(g)
        return nil
    case m.Attempts >= maxRetries:
        s.st.deadLettered++
        s.dropGroupIfIdle(g)
        return m
    }

    if delay > 0 {
        m.DeliverAfter = now.Add(delay)
        heap.Push(&s.delayed, delayEntry{msg: m, at: m.DeliverAfter})
        s.dropGroupIfIdle(g)
        s.st.requeued++
        return nil
    }

    g.msgs.pushFront(m)              // ← FRONT of the group
    s.st.requeued++
    if !g.inBand { s.pushGroup(g) }  // ← TAIL of the band
    return nil
}
```

**The asymmetry in `retire` is the important part.** A retried message goes back to
the **front of its group**, because it was the group's head and group ordering is
strict. The **group** goes to the **tail of its band**, so a repeatedly failing
message does not block the head of its priority level on every cycle.

Both halves of HLD §14's retry rule are in these three lines.

The timer heap uses lazy deletion. An acknowledged message leaves its timer entry in
place; the entry is discarded when it surfaces and its epoch no longer matches. Heap
size is therefore bounded by leases ever created, not by leases outstanding, and it
drains as time passes.

### 7.2 TTL

```go
// caller holds s.mu
func (s *slot) sweepTTL(now time.Time) int {
    n := 0
    for _, g := range s.groups {
        for {
            m, ok := g.msgs.front()
            if !ok || !m.expired(now) { break }
            g.msgs.popFront()
            s.st.expired++
            n++
        }
        if !g.inBand && !g.locked && !g.msgs.empty() { s.pushGroup(g) }
        s.dropGroupIfIdle(g)
    }
    s.refreshHint()
    return n
}
```

Only the front of each group is examined. Messages behind a live head cannot be
older than it within a group, and they are removed when they reach the front. This
keeps the oldest-message-age metric honest without walking every message.

### 7.3 Delayed release

```go
// caller holds s.mu
func (s *slot) releaseDelayed(now time.Time) int {
    n := 0
    for s.delayed.Len() > 0 && !s.delayed[0].at.After(now) {
        e := heap.Pop(&s.delayed).(delayEntry)
        e.msg.DeliverAfter = time.Time{}
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
    Name                string
    VisibilityTimeout   time.Duration
    MaxRetries          uint32
    DefaultTTL          time.Duration
    StarvationThreshold time.Duration
    StarvationReserve   float64        // 0..1, default 0.2
    MaxDepth            int64
    Distributed         bool           // false: all 16 slots on one machine
}

type Queue struct {
    key     QueueKey
    cfg     Config
    clock   Clock
    cluster Cluster                // which slots do I own

    slots   map[uint16]*slot       // lazily created
    slotsMu sync.RWMutex

    seq      atomic.Uint64         // submission sequence
    dispatch atomic.Uint64         // dispatch counter, drives the reserve
    rr       atomic.Uint64         // round robin for ungrouped messages
    cursor   atomic.Uint32         // rotating start for the starvation scan
    depth    atomic.Int64

    dlq *Queue
}

type Cluster interface {
    // SlotFor maps a group key to one of the queue's 16 slots.
    SlotFor(q QueueKey, groupID string) uint16
    // PlacementGroupFor maps a queue -- or a single slot of a distributed
    // queue -- to one of the 4096 placement groups.
    PlacementGroupFor(q QueueKey, slot uint16, distributed bool) uint16
    // LocalSlots lists the slots of this queue that this process owns.
    LocalSlots(q QueueKey) []uint16
}

const (
    SlotsPerQueue      = 16
    NumPlacementGroups = 4096
)
```

`Cluster` is the seam described in HLD §18, and it is where multi-tenancy lives.

```go
func (c *single) SlotFor(q QueueKey, groupID string) uint16 {
    return uint16(xxhash.Sum64String(q.Org+"\x00"+q.Name+"\x00"+groupID) % SlotsPerQueue)
}

func (c *single) PlacementGroupFor(q QueueKey, slot uint16, distributed bool) uint16 {
    key := q.Org + "\x00" + q.Name
    if distributed {
        key += "\x00" + strconv.Itoa(int(slot))   // each slot placed independently
    }
    return uint16(xxhash.Sum64String(key) % NumPlacementGroups)
}

func (c *single) LocalSlots(q QueueKey) []uint16 { return c.all }   // owns everything
```

The separator byte matters. Without it, org `a` with queue `bc` and org `ab` with
queue `c` hash identically, and two tenants would share slots.

`PlacementGroupFor` is unused in a single instance. It exists so the distributed
implementation is a different body for the same signature: look up
`pgMap[PlacementGroupFor(q, slot, cfg.Distributed)]` and proxy if the answer is not
this node.

**The `distributed` flag is the whole difference.** When false, the slot is left out
of the hash, so all 16 slots of a queue resolve to the same placement group and
therefore the same machine. When true, each slot hashes separately and lands
wherever. One line, and it is what decides whether a queue gets exact ordering and
exact counts or trades them for throughput.

**A slot belongs to exactly one queue, and therefore to exactly one org.** This is
what keeps tenants off each other's locks: no arrangement of hashes can put two orgs'
messages behind the same mutex. Placement groups are only about which machine holds
a slot.

### 8.1 Slot selection at enqueue

```go
func (q *Queue) slotFor(m *Message) uint16 {
    if m.GroupID != "" {
        return q.cluster.SlotFor(q.key, m.GroupID)   // same group, same slot, always
    }
    local := q.cluster.LocalSlots(q.key)
    return local[q.rr.Add(1)%uint64(q.fanout(len(local)))]
}

// fanout grows the number of slots an ungrouped message can land in, in
// proportion to how deep the queue is.
func (q *Queue) fanout(n int) int {
    d := q.depth.Load()
    want := 1
    for want < n && int64(want)*msgsPerSlotTarget < d {
        want *= 2
    }
    return want
}

const msgsPerSlotTarget = 1000
```

Round-robin across all 16 slots from the first message is wrong. A queue holding a
thousand messages would scatter them over 16 lock-striped structures for parallelism
it does not need, materialise all 16 slots, and make cross-slot FIFO comparison
harder for nothing. Any queue that ever held 16 ungrouped messages would pay the full
memory cost forever.

Growing the fan-out with depth means a small queue uses one slot and a queue holding
millions uses all of them. This is safe only because ungrouped messages carry no
ordering constraint — nothing breaks when the fan-out changes underneath them.
Grouped messages always hash, and are never affected.

### 8.2 The dispatcher

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

### 8.3 Most urgent, without locking every slot

```go
func (q *Queue) takeUrgent(now time.Time) (*Message, Receipt, bool) {
    for attempt := 0; attempt < maxDispatchRetries; attempt++ {
        var best *slot
        var bestBand Priority
        var bestSeq uint64 = math.MaxUint64

        for _, s := range q.localSlots() {
            h := s.hint.Load()
            if h == 0 { continue }
            band := Priority(h - 1)
            seq := s.headSeq.Load()
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

Two atomic loads per slot, then one lock on the winner. Sixty-four atomic loads is a
few dozen nanoseconds; sixty-four mutex acquisitions would not be.

The `hint` and `headSeq` are read separately and can be momentarily inconsistent.
That is acceptable: they steer the choice, and `take` verifies under the lock. A
mismatch costs one retry.

Comparing `headSeq` at equal priority is what makes FIFO **exact**. Every slot of a
normal queue is in this process, so the loop above sees all 16 of them and picks the
genuinely oldest message at the highest non-empty priority. No hints, no
approximation.

This is exactly what a `distributed` queue gives up: its slots are on different
machines, `LocalSlots` returns only the ones here, and the comparison cannot span the
rest. That is the whole cost of the flag, and it appears in this one loop.

### 8.4 The starvation path

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

`oldestBandBefore` walks set bits from the lowest priority upward and returns the
first band whose head message was enqueued before the cutoff. Low priorities are
checked first because that is where starvation happens.

The scan is capped at `starvationScanLimit` (16) and starts from a rotating cursor,
so the work is bounded while every slot is still reached over time. This path runs
on `StarvationReserve` of dispatches, so the amortised cost is small.

---

## 9. Invariants

Asserted by the property tests in LLD 6.

1. A message is in exactly one of: a group deque, `inflight`, or `delayed`.
2. `g.locked` is true if and only if some lease in `inflight` points at `g`.
3. If `g.inBand`, exactly one entry in `bands[g.band]` has `ver == g.version`.
4. `bandMask` bit `p` is set if `bands[p]` is non-empty. The converse may not hold —
   the bitmap is a superset, never a subset.
5. A group in `s.groups` has at least one message, or is locked, or is in a band.
6. Within a group, `msgs` is in submission order at all times, including after retry.
7. `epochSeq` never decreases, so an older receipt can never match a newer lease.

---

## 10. Complexity

| Operation | Cost |
|---|---|
| Enqueue | O(1) |
| Dequeue, urgent path | O(S) atomic loads + O(1) under one lock |
| Dequeue, starvation path | O(min(S, 16)) locks, on a fraction of dispatches |
| Acknowledge | O(1) |
| Negative acknowledge | O(1), or O(log D) with a delay |
| Lease sweep | O(k log T) for k expired |
| TTL sweep | O(G), groups in the slot |
| Delayed release | O(k log D) |

S = local slots (16), T = timer heap, D = delay heap, G = groups.

### Memory

### Counters

Every slot maintains these under its own lock, so they cost nothing beyond the
arithmetic:

```go
type slotStats struct {
    ready    [3]int64   // bucketed low / medium / high, for the depth metric
    inflight int64
    delayed  int64
    bytes    int64

    // cumulative, only ever increase
    enqueued, acked, expired, requeued, deadLettered, escapes uint64
}
```

`ready` is bucketed into three rather than kept per priority level, because that is
the granularity metrics are reported at (HLD §15), and 101 counters per slot would
cost more than the bands themselves.

None of this is written to the log. After a crash the node replays and rebuilds, and
the counters come out of the rebuilt state, so there is no second thing to keep
consistent.

A queue's depth is the sum across its slots: a local walk on a single instance, and
the fan-out described in HLD §4 for a cluster.

### Memory

| Part of a live slot | Bytes |
|---|---|
| `bandMask` | 16 |
| `bands`, three priorities in use | ~200 |
| `groups`, `inflight` map headers | ~96 |
| `timers`, `delayed` slice headers | 48 |
| mutex, counters, hints, id | ~80 |
| **Total, before any message** | **~440** |

A dense `[101]deque` would add 3,232 bytes to every one of these. Slots are created
on first use, so an idle queue costs nothing at all.

Nothing scales with queue depth. That is the property the latency budget in HLD §16
depends on.

---

## 11. Edge cases

| Case | Behaviour |
|---|---|
| Acknowledge after the lease expired | `ErrLeaseExpired`, epoch mismatch |
| Acknowledge twice | `ErrNotInFlight`; the API layer maps it to 200 |
| Acknowledge a dead-lettered message | `ErrNotInFlight` |
| Every group in a slot is locked | `take` returns false; the dispatcher moves on |
| Head of a group is TTL-expired | Popped and skipped inside `take` |
| Every message in a group expires | Group deleted, `take` returns false, caller retries |
| Retry on the last attempt | Returned from `retire` for dead-lettering |
| `MaxRetries` is 0 | Dead-letters on the first lease expiry |
| Ungrouped messages | `gid = m.ID`, a group of one, never blocked |
| Two orgs, same queue name | Different `QueueKey`, different slots, no shared state |
| Queue at `MaxDepth` | `ErrQueueFull` at enqueue; the API layer returns 503 |
| Group spanning priorities | Group sits in the band of its current head, re-banded on unlock |
| Band holds only stale entries | `takeGroup` drains them and clears the bit |
| Delay longer than the TTL | Released, then dropped as expired at `take` |
| Clock moves backwards | Leases use monotonic readings, unaffected |

---

## 12. Reading counters out

The engine never talks to a metrics library. It exposes a snapshot and something
above it decides what to do with the numbers.

```go
// Stats returns a consistent snapshot of one queue's counters.
// Every slot is read under its own lock, sequentially, so the whole call
// takes microseconds and the queue is never frozen.
func (q *Queue) Stats(now time.Time) QueueStats

type QueueStats struct {
    Ready        [3]int64      // bucketed low / medium / high
    InFlight     int64
    Delayed      int64
    Bytes        int64
    OldestAge    time.Duration // max across slots, clamped at >= 0
    Enqueued     uint64
    Acknowledged uint64
    Expired      uint64
    Requeued     uint64
    DeadLettered uint64
    Escapes      uint64
}
```

For a normal queue this is **exact**. All 16 slots are here, each counter is read
under the lock that maintains it, and the whole call is microseconds — far shorter
than the time the answer spends travelling back to the caller.

Reads are sequential, not simultaneous, because holding two slot locks at once is
forbidden (§10). The gap between the first and last slot read is a few microseconds,
which at any realistic rate is under one message.

`OldestAge` aggregates as a **maximum** across slots, never a sum, and is clamped at
zero so a clock adjustment cannot produce a negative age.

The node layer serves this over its internal RPC; the gateway collects from every
node it talks to and serves both `/metrics` and the stats endpoint from one cache
(HLD §15). The engine knows about none of that.

---

## 13. What LLD 2 has to provide

The engine calls into storage at exactly three points, and they are the only places
the engine is not self-contained:

```go
type Journal interface {
    // Incarnation is read once at startup, after replay.
    Incarnation() uint64
    // AppendEnqueue writes and flushes before returning. Its error fails the
    // enqueue, so nothing becomes visible that is not already in the log.
    AppendEnqueue(m *Message) error
    // The rest are fire-and-forget: buffered, flushed on the journal's own
    // schedule. Losing them costs at most one extra retry or one redelivery.
    AppendAttempt(id string, n uint32, epoch uint64)
    AppendTerminal(id string, kind TerminalKind)   // ack, expire, dead-letter
}
```

Only `AppendEnqueue` is on the critical path, and it is what makes the ordering in
HLD §10 real: write, flush, then apply in memory. The others are buffered because
flushing every delivery would double the cost of the most common operation, and the
worst case after a crash is a message getting one extra retry.

The no-op implementation satisfies this interface, which is what lets the engine be
tested with no disk at all.
