# LLD 2 — Write-Ahead Log

Implements HLD §11. Package `backend/queue/repo/walfile`.

The log is what makes a message survive a crash, and it is also the unit that
moves when a slot changes machines. Both come from the same decision: **one file
per slot**, so a slot's log is its complete history.

---

## 1. Layout

```
<dataDir>/
  node-id                       generated once, read forever
  incarnation                   restart counter
  queues/
    <org>/<queue>/
      slot-0000.log
      slot-0007.log             only slots that received a message exist
```

Org and queue names are escaped, not just concatenated, so two different names
can never resolve to one directory:

```go
func sanitise(s string) string   // a-z A-Z 0-9 - _ kept, everything else ~xx
```

---

## 2. Record format

```
┌──────────┬──────────┬──────┬───────────────────┐
│ len (4)  │ crc32(4) │ kind │ JSON payload      │
└──────────┴──────────┴──────┴───────────────────┘
        little endian        1 byte
```

The checksum covers the payload only. JSON rather than a binary encoding because
a log is something you end up reading by hand when something has gone wrong, and
the size difference does not matter next to the payload itself.

| Kind | Written | Flushed before returning |
|---|---|---|
| `enqueue` | Message accepted | **Yes** |
| `attempt` | Message handed out | No |
| `terminal` | Acknowledged, expired, dead-lettered | No |

**Lease deadlines are never written.** Every lease is void after a crash, so
saving them is wasted work. The attempt count is the one lease-related thing that
must survive: without it a message that always fails would retry forever and
never reach the dead-letter queue.

---

## 3. Writing

```go
func (w *WAL) append(id uint16, kind recordKind, v any, durable bool) error {
    buf, _ := encode(kind, v)
    sf, _ := w.slot(id)

    sf.mu.Lock()
    defer sf.mu.Unlock()

    // A single Write reaches the page cache, so the record survives the process
    // dying. Only power loss needs the fsync below.
    if _, err := sf.f.Write(buf); err != nil {
        return err
    }
    if durable && w.opts.Sync == SyncAlways {
        return sf.f.Sync()
    }
    return nil
}
```

One `Write` per record, no user-space buffering. That is deliberate: a
`bufio.Writer` would keep the record in the process, where a panic loses it. Going
straight to the file hands it to the kernel, which survives the process.

| Mode | Survives | Cost |
|---|---|---|
| `always` | Process crash and power loss | An fsync per submission |
| `interval` (default) | Process crash fully, ≤100 ms lost on power loss | A background fsync every 100 ms |
| `never` | Process crash | Nothing |

`interval` is the default because a process crash loses nothing either way.

One lock per slot file, so writes to different slots do not serialise — the same
reasoning as the engine's slot lock, one level down.

---

## 4. Replay

```go
for {
    kind, body, err := readRecord(f)
    if errors.Is(err, io.EOF)   { break }
    if errors.Is(err, errTorn)  { os.Truncate(path, good); break }
    good += int64(headerLen + len(body))
    ...
}
```

Replay builds a map of live messages: `enqueue` adds, `attempt` updates the count,
`terminal` deletes. Insertion order is kept separately so messages come back in
the order they were submitted rather than in map order.

**A torn tail is truncated, not treated as corruption.** A half-written record at
the end of a log is what a crash looks like. `readRecord` returns `errTorn` for a
short read, an implausible length, or a checksum mismatch, and replay truncates at
the last good offset so the next append starts clean.

Anything after a torn record cannot be trusted, so replay stops there rather than
trying to resynchronise.

---

## 5. Compaction and removal

```go
func (w *WAL) Compact(id uint16, msgs []*engine.Message) error
func (w *WAL) Drop(id uint16) error
```

`Compact` writes what is still live to a temporary file and renames it over the
original, which is atomic on any sane filesystem. Without it the log grows with
throughput rather than with depth, because every acknowledgment appends a
tombstone rather than removing anything.

`Drop` deletes a slot's file, used when a slot is handed to another machine.

---

## 6. Identity

```go
func NodeID(root string) (string, error)
func NextIncarnation(root string) (uint64, error)
```

**The node id lives beside the data.** A container hostname changes on every
recreate, and the id's whole job is to say who holds this data, so it is generated
once into `<dataDir>/node-id` and read back forever. Same volume, same identity.
Fresh volume, genuinely a new node.

**The incarnation is bumped at every start.** Lease generations restart from zero
after a replay, so a receipt issued before a crash could match a lease handed out
after it, and an acknowledgment for a long-dead delivery would delete a message
another worker is processing. Every receipt carries the incarnation and one from
an earlier run is rejected outright.

---

## 7. The factory

```go
type Factory struct { root string; opts Options; inc uint64 }

func (f *Factory) Open(key engine.QueueKey) (*WAL, error)
func (f *Factory) Remove(key engine.QueueKey) error
func (f *Factory) List() ([]engine.QueueKey, error)
```

`List` walks the directory tree and is how a restarting node discovers what it
holds before anything tells it — the node recovers from its own disk rather than
waiting to be told what it owns.

---

## 8. Tests

`wal_test.go`, all against a real temporary directory.

| Test | What it proves |
|---|---|
| `TestAppendAndReplay` | Terminals remove, attempt counts are restored |
| `TestReplayPreservesOrder` | Messages come back in submission order |
| `TestTornTailIsTruncated` | Garbage appended after a crash is dropped, and the next append still works |
| `TestCorruptPayloadStopsReplay` | A flipped bit stops replay at that record |
| `TestCompactDropsDeadRecords` | Compaction keeps only what is live |
| `TestDropRemovesSlot` | A handed-off slot leaves nothing behind |

The torn-tail test is the one that matters: it appends three junk bytes to a good
log, replays, and asserts both that the good records survive and that a later
append is readable — which is what proves the truncation actually happened rather
than the read merely stopping.
