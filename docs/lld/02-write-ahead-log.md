# LLD 2 — Write-Ahead Log

Durable storage under the queue engine. Implements HLD Section 10, and satisfies the
`Journal` interface from LLD 1 §13.

---

## 1. What it guarantees

One thing, and everything else follows from it:

> A message is never visible to a consumer unless its record is already durable.

The engine enforces the ordering (LLD 1 §13): `AppendEnqueue` returns before the
message enters the in-memory structures. If the append fails, the submission fails
and the producer is told so.

Everything else the log does — attempt counts, terminal records, snapshots — exists
so recovery reconstructs the same state. It is not what makes submission safe.

**What it does not guarantee.** Acknowledgments and attempt counts are written
without waiting (HLD §10). Losing the tail of the log costs a redelivery, and
possibly one extra retry before dead-lettering. Neither loses a message.

---

## 2. Where files live

```
data/
  q-00000042/                  ← numeric queue id, not the name
    slot-0017/
      000001.log               ← segments, rotated at 64 MB
      000002.log
      000002.snap              ← snapshot taken at the start of segment 2
```

**Paths use a numeric queue id, never the org or queue name.** Names are chosen by
tenants, so a name-derived path is a traversal bug waiting to happen, and it also
runs into filename length limits, case-insensitive filesystems, and Unicode
normalisation. Postgres assigns an immutable `queue_id BIGINT` at creation and the
config carries it. Renaming a queue, if we ever allow it, then touches no files.

One directory per slot, because a slot is the unit of movement (HLD §9). Shipping a
slot means shipping a directory.

---

## 3. Record format

```
 0        4        8        9                        len+8
 +--------+--------+--------+------------------------+
 | len    | crc32c | type   | payload                |
 | uint32 | uint32 | uint8  | len-1 bytes            |
 +--------+--------+--------+------------------------+

 len = 1 + payload length      (covers type and payload)
 crc = Castagnoli over type || payload
```

Little-endian throughout.

**A checksum per record, not per segment.** A crash mid-write leaves a partial record
at the end of the file. Per-record checksums say exactly where the good data stops. A
per-segment checksum would only say the segment is bad, without saying where.

**Hand-rolled binary rather than protobuf.** The format is read only by us, and
encoding sits on the submission path. Two rules substitute for what protobuf would
give for free:

1. Fields are only ever appended, never reordered or removed.
2. A layout change allocates a new type byte. Old records keep replaying under the
   old one.

If another language ever needs to read these files, that trade should be revisited.

---

## 4. Record types

```go
const (
    recEnqueue    uint8 = 1
    recAttempt    uint8 = 2
    recAck        uint8 = 3
    recExpire     uint8 = 4
    recDeadLetter uint8 = 5
    recRequeue    uint8 = 6   // nack carrying a delay
)
```

| Type | Payload | Waits for durability |
|---|---|---|
| `recEnqueue` | id, priority, seq, group, timestamps, payload | **Yes** |
| `recDeadLetter` | id | **Yes** |
| `recAttempt` | id, attempts, epoch | No |
| `recAck` | id | No |
| `recExpire` | id | No |
| `recRequeue` | id, attempts, deliverAfter | No |

```go
// recEnqueue
//   id           [16]byte
//   priority     uint8
//   seq          uint64
//   enqueuedAt   int64      unix nanos
//   expiresAt    int64      0 = none
//   deliverAfter int64      0 = none
//   groupLen     uvarint    then groupLen bytes
//   payloadLen   uvarint    then payloadLen bytes
```

**Every time value is in the record.** Recovery and replicas never call `Now()` —
that is the determinism rule from HLD §10, and it is the thing a later change is most
likely to break silently. §14 has the test that catches it.

`recAttempt` carries the epoch so a receipt issued before a crash is still rejected
afterwards. Without it a new lease could reuse an epoch that an outstanding receipt
already holds.

---

## 5. Appending

```go
type Log struct {
    dir string
    seg *os.File
    off int64

    mu      sync.Mutex
    active  *bytes.Buffer     // writers fill this
    spare   *bytes.Buffer     // the syncer is writing this
    waiters []chan error

    policy   SyncPolicy
    interval time.Duration
}

func (l *Log) Append(rec []byte, durable bool) error {
    l.mu.Lock()
    l.active.Write(rec)
    if !durable {
        l.mu.Unlock()
        return nil
    }
    ch := make(chan error, 1)
    l.waiters = append(l.waiters, ch)
    l.mu.Unlock()

    if l.policy == SyncAlways {
        l.flush()
    }
    return <-ch
}
```

Encoding happens before `Append` is called, so no serialisation work happens under
the lock. The engine calls this outside the slot lock (LLD 1 §8), so the two locks
are never held at the same time.

### Group commit

```go
func (l *Log) flush() {
    l.mu.Lock()
    if l.active.Len() == 0 && len(l.waiters) == 0 {
        l.mu.Unlock()
        return
    }
    buf := l.active
    l.active, l.spare = l.spare, buf
    l.active.Reset()
    waiters := l.waiters
    l.waiters = nil
    l.mu.Unlock()

    // I/O runs with the lock released
    _, err := l.seg.Write(buf.Bytes())
    if err == nil {
        err = l.seg.Sync()
    }
    l.off += int64(buf.Len())

    for _, ch := range waiters {
        ch <- err
    }
}
```

**The buffer swap is the point.** An `fsync` takes around a millisecond, and holding
the lock across it would serialise every writer behind the disk. Swapping buffers
lets writers keep filling one while the other is written. Only one goroutine calls
`flush`, so bytes reach the file in order.

A background goroutine calls `flush` every `interval`. Under `SyncAlways` the writer
calls it directly.

### Sync policy

| Policy | Loses on process crash | Loses on power loss | Cost |
|---|---|---|---|
| `SyncAlways` | Nothing | Nothing | 0.5–2 ms per submission |
| `SyncInterval` (default, 100 ms) | Nothing | Up to 100 ms | Amortised to nothing |
| `SyncNever` | Nothing | Everything unflushed | Nothing |

`SyncInterval` is the default because a **process** crash loses nothing under any of
them — the data is in the kernel page cache and survives the process dying. Only
losing the machine's power costs anything, and 100 ms of exposure is a fair trade for
taking `fsync` off the submission path.

This is what HLD §11 means by "None, unless power was lost."

---

## 6. Segments

Rotation at 64 MB. Names are zero-padded and monotonic, so directory order is
chronological.

```go
func (l *Log) maybeRotate() error {
    if l.off < segmentBytes { return nil }
    if err := l.seg.Close(); err != nil { return err }
    l.segNum++
    f, err := os.OpenFile(l.path(l.segNum), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
    if err != nil { return err }
    l.seg, l.off = f, 0
    return nil
}
```

Rotation happens inside `flush`, between records, never inside one. A record never
spans two segments, which is what lets recovery treat each segment independently.

---

## 7. Snapshots

A log that only grows is a log that eventually cannot be replayed. A snapshot is the
slot's live state at a point, so everything before it can be deleted.

### Taking one

The slot lock cannot be held while megabytes are written, and it cannot be released
before a consistent view is captured. So the copy is split:

```go
// under s.mu — cheap, copies only the mutable fields
func (s *slot) snapshotView() []Message {
    out := make([]Message, 0, s.liveCount())
    for _, g := range s.groups {
        for _, m := range g.msgs.all() {
            out = append(out, *m)      // payload slice is shared, not copied
        }
    }
    for _, l := range s.inflight {
        out = append(out, *l.msg)      // in flight becomes ready on recovery
    }
    for _, d := range s.delayed {
        out = append(out, *d.msg)
    }
    return out
}
```

Copying `Message` by value duplicates about eighty bytes each and shares the payload,
which is immutable once written. The lock is held for a slice copy, not for I/O.

The snapshot is written outside the lock to `NNNNNN.snap.tmp`, fsynced, then renamed.
**Rename is atomic on POSIX**, so a snapshot either exists complete or does not exist.
A crash mid-snapshot leaves only a `.tmp` file, which recovery deletes.

```
snapshot header:
  magic     uint32
  version   uint16
  slotID    uint16
  segment   uint32      ← replay resumes from the start of this segment
  count     uint32
  then `count` recEnqueue payloads, each carrying its current attempts
```

Once the snapshot is durable, segments below `segment` are deleted.

### When

Whichever comes first: every 60 seconds if there are new records, or once the
segments since the last snapshot exceed 256 MB. A slot with no traffic never
snapshots, so an idle queue does no background I/O.

---

## 8. Recovery

```go
func Recover(dir string, now time.Time) (*slot, error) {
    snap, from := latestSnapshot(dir)      // nil, 0 if none
    live := map[string]*Message{}
    order := []string{}                    // preserves submission order

    if snap != nil {
        for _, m := range snap.Messages {
            live[m.ID] = &m
            order = append(order, m.ID)
        }
    }

    for _, seg := range segmentsFrom(dir, from) {
        if err := replaySegment(seg, live, &order); err != nil {
            return nil, err
        }
    }

    s := newSlot()
    for _, id := range order {
        m, ok := live[id]
        if !ok || m.expired(now) { continue }
        s.enqueue(m, now)                  // the normal path, so ordering is normal
    }
    return s, nil
}
```

```go
func replaySegment(path string, live map[string]*Message, order *[]string) error {
    r := bufio.NewReader(open(path))
    for {
        rec, err := readRecord(r)
        switch {
        case errors.Is(err, io.EOF):
            return nil
        case errors.Is(err, errTorn), errors.Is(err, errBadChecksum):
            return truncateAt(path, r.consumed())   // expected after a crash
        case err != nil:
            return err
        }

        switch rec.typ {
        case recEnqueue:
            m := decodeEnqueue(rec)
            live[m.ID] = m
            *order = append(*order, m.ID)
        case recAttempt:
            if m := live[rec.id]; m != nil { m.Attempts = rec.attempts }
        case recRequeue:
            if m := live[rec.id]; m != nil {
                m.Attempts = rec.attempts
                m.DeliverAfter = rec.deliverAfter
            }
        case recAck, recExpire, recDeadLetter:
            delete(live, rec.id)
        default:
            // unknown type from a newer version: skipped via the length prefix
        }
    }
}
```

Four properties worth stating.

**Replaying in log order reproduces group ordering.** A group's messages appear in
the log in submission order, and `enqueue` appends to the group's tail, so the
reconstructed order matches. Nothing sorts.

**Records for unknown ids are ignored.** An `recAck` whose `recEnqueue` was compacted
away is normal, not corruption.

**In-flight messages come back as ready.** The snapshot and the replay make no
distinction, because leases do not survive a restart (HLD §11). Attempt counts do
survive, which is what stops a poison message retrying forever.

**Truncating on a bad record is the expected path, not an error path.** A torn tail
is what a crash during a write looks like. Because the log is append-only, nothing
after a torn record can be valid, so truncating there loses only what was never
acknowledged.

A segment missing from the *middle* of the sequence is different. That is real
damage, and the node refuses to open the slot rather than silently serving a hole.

---

## 9. File descriptors and the fsync ceiling

One log per slot is what makes a slot movable, and it has a cost worth stating
plainly.

**Descriptors.** A node holding 10,000 live slots would want 10,000 open files. The
log keeps an LRU cache of open segments capped at 4,096 and reopens on demand. A slot
with no traffic holds no descriptor. Reopening costs one syscall on the first write
after idleness, which is nothing next to the `fsync` that follows it.

**Syncs.** Group commit batches writers *within* one slot's log. It cannot batch
across slots, because they are different files. With `SyncInterval` at 100 ms and
1,000 active slots that is 10,000 `fsync` calls a second, which an NVMe device
handles. At 10,000 active slots it is 100,000 a second, which it does not.

So per-slot logs are correct and simple up to a few thousand active slots per node.
Past that the answer is a **shared node-level log** — one file, one `fsync`, with the
slot id in every record — while snapshots stay per slot so migration still ships a
self-contained directory. Recovery then loads per-slot snapshots and replays the tail
of the shared log filtered by slot.

That is a real change and it is not built. It is written down because "one log per
slot" has a ceiling, and finding it in production is worse than knowing where it is.

---

## 10. Errors

| Condition | Behaviour |
|---|---|
| Disk full on a durable append | Error returned; the submission fails with 503 |
| Disk full on a non-durable append | Logged and counted; delivery continues |
| Write fails mid-record | Record is torn; the next recovery truncates there |
| Checksum mismatch during replay | Truncate at that offset, keep what came before |
| Segment missing from the middle | Refuse to open the slot; needs an operator |
| Snapshot corrupt | Fall back to the previous snapshot, replay more segments |
| `.tmp` snapshot found | Deleted; a crash happened mid-snapshot |
| Unknown record type | Skipped using the length prefix |

The split in the first two rows matters. A failed `recEnqueue` must fail the request,
because we promised nothing is accepted unless it is durable. A failed `recAck` must
not, because the worker really did finish the job — the consequence is a redelivery,
which at-least-once already allows.

---

## 11. Implementing the engine's interface

```go
type journal struct{ log *Log }

func (j *journal) AppendEnqueue(m *queue.Message) error {
    return j.log.Append(encodeEnqueue(m), true)      // durable
}

func (j *journal) AppendAttempt(id string, n uint32, epoch uint64) {
    _ = j.log.Append(encodeAttempt(id, n, epoch), false)
}

func (j *journal) AppendTerminal(id string, k queue.TerminalKind) {
    _ = j.log.Append(encodeTerminal(id, k), k == queue.TerminalDeadLetter)
}
```

Dead-lettering waits for durability; the other terminal kinds do not. Losing an ack
causes a redelivery. Losing a dead-letter record would resurrect a message that has
already exhausted its retries and send it round the loop again.

```go
type NopJournal struct{}

func (NopJournal) AppendEnqueue(*queue.Message) error        { return nil }
func (NopJournal) AppendAttempt(string, uint32, uint64)      {}
func (NopJournal) AppendTerminal(string, queue.TerminalKind) {}
```

`NopJournal` is what the engine's own tests use. It is also a legitimate production
setting for a queue whose work is cheap to regenerate.

---

## 12. Where replication attaches

Not built (HLD §18). The seam is one line in `flush`:

```go
func (l *Log) flush() {
    // ... buffer swap ...
    err := l.writeAndSync(buf)
    if l.repl != nil {
        err = l.repl.Send(buf.Bytes(), l.off)   // returns once a quorum has it
    }
    // ... wake waiters ...
}
```

The bytes going to followers are the same bytes going to disk, in the same order,
which is what makes replicas converge (HLD §10). Followers append what they receive
and apply it through the same `replaySegment` used at startup, so there is one apply
implementation rather than two that can drift apart.

---

## 13. Performance

| Operation | Cost |
|---|---|
| Encode a record | ~200 ns, no allocation after the first |
| Append, not durable | Lock, memcopy, unlock — under 100 ns |
| Append, durable, batched | One `fsync` amortised across every waiter in the window |
| Append, durable, `SyncAlways` | One `fsync`, 0.5–2 ms |
| Replay | ~200 MB/s, dominated by checksums |
| Snapshot | Lock held for a slice copy; I/O outside it |

Under `SyncInterval` the log costs a memcopy on the submission path. That is what
keeps the latency budget in HLD §16 reachable while still writing durably before a
message becomes visible.

---

## 14. Testing

**Round trip.** Encode and decode every record type, including empty payloads, empty
group ids, maximum-length payloads and zero timestamps.

**Torn tail.** Write records, truncate the file at every byte offset inside the last
record, and check that recovery returns exactly the records before it and leaves the
file truncated in the right place.

**Corruption.** Flip one bit in each position of a record and check that recovery
stops at that record rather than accepting it or skipping past it.

**Crash.** Run a producer and a consumer, `SIGKILL` the process, recover, and assert
that every acknowledged message is gone and every unacknowledged message comes back
with its attempt count intact.

**Snapshot equivalence.** Build a slot through random operations, snapshot it, recover
from the snapshot plus the remaining log, and compare the two slots field by field
including group order.

**Determinism.** Replay the same log twice into two slots and require them to be
identical. This is the test that catches a `Now()` creeping into the apply path.

---

## 15. Edge cases

| Case | Behaviour |
|---|---|
| Snapshot while the slot is busy | Lock held only for the slice copy |
| Crash during a snapshot | `.tmp` deleted, previous snapshot used |
| Crash between fsync and rename | Same — the rename is what makes it visible |
| Rotation at a record boundary | Records never span segments |
| Replayed message already expired | Skipped at insert, counted as expired |
| Attempt record with no matching enqueue | Ignored |
| Log for a queue deleted while offline | Directory removed during startup reconciliation |
| Two nodes opening the same directory | Prevented by a lock file naming the owner |
| Clock moved backwards between runs | Timestamps come from records, not the clock |
