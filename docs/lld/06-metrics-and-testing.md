# LLD 6 — Metrics and Testing

Implements HLD §17, §21.

---

## 1. Where the numbers come from

Every counter is maintained **under a slot's lock as messages move**, never
computed by scanning:

```go
type slotStats struct {
    ready    [3]int64   // bucketed low / medium / high
    inflight int64
    delayed  int64
    bytes    int64
    enqueued, acked, expired, requeued, deadLettered uint64
}
```

So reporting does not get slower as queues get deeper.

`ready` is bucketed into three rather than kept per priority level, because that
is the granularity metrics are reported at and 101 counters per slot would cost
more than the priority lists themselves.

**Nothing is written to the log.** After a crash the node replays, rebuilds its
structures, and the counters fall out of the rebuilt state — so there is no second
thing to keep consistent.

`OldestAge` aggregates as a **maximum** across slots, never a sum, and is clamped
at zero so a clock adjustment cannot produce a negative age.

---

## 2. Collection

**The gateway collects. Nothing scrapes a node.**

```go
for _, m := range l.members.Members() {
    nodeID, all, err := l.nodes.StatsAll(ctx, m.Addr)
    ...
}
```

One request per node every five seconds returns every queue that node holds. Fifty
machines cost fifty requests whether the cluster holds ten queues or ten thousand.

Three reasons not to let a monitoring system scrape nodes:

**Nodes have no public listener.** Giving them one for metrics would undo the
boundary between tiers for a monitoring convenience.

**A node holds slots, not queues.** Something has to add them up. Doing it in the
monitoring system means the dashboard and the stats endpoint are two aggregation
paths that can disagree, and when they do nobody knows which to believe.

**Scrape timing differs per target.** Summing across machines scraped at different
moments mixes samples up to a full interval apart.

---

## 3. Three endpoints, one cache

```
GET /v1/queues/{name}/stats     one queue, JSON
GET /v1/metrics                 every queue in the caller's org, JSON
GET /metrics                    the same numbers in Prometheus text format
```

All three read the collector's cache, so they cannot disagree.

`/v1/queues/{name}/stats` is the exception: for a **normal** queue it asks the
owner directly rather than serving the cache, because that queue lives on one
machine and one request gives an exact answer.

```json
{ "messages": 128401, "inFlight": 892, "exact": true, "ownerNode": "node-abc" }
```

For a **distributed** queue it asks every machine holding a slot and sums, reports
`exact: false`, and counts machines it could not reach:

```json
{ "messages": 124800, "exact": false, "unavailableSlots": 3 }
```

Reporting the gap rather than quietly under-reporting is the difference between a
number somebody trusts and one they eventually stop believing.

`/metrics` is hand-written text formatting, about forty lines and no dependency:

```
ryuk_queue_ready_messages{org="org_acme",queue="orders",priority="high"} 400
ryuk_queue_inflight_messages{org="org_acme",queue="orders"} 892
ryuk_queue_oldest_message_age_seconds{org="org_acme",queue="orders"} 43.0
ryuk_queue_enqueued_total{org="org_acme",queue="orders"} 128401
ryuk_queue_starvation_escapes_total{org="org_acme",queue="orders"} 0
```

**Starvation escapes is the number worth alerting on.** If it is climbing, workers
cannot keep up with urgent work and low-priority work is only moving because of
the safety net. None of the required metrics show that.

---

## 4. Testing

### The two decisions that make it testable

**The clock is supplied, not read from the system.** Visibility timeouts, expiry,
delayed release and the starvation threshold are all tested by advancing a fake
clock. Sleeping would be slow, unreliable, and could not test a twelve-hour
timeout at all.

**Background work is callable directly.** `Sweep()` runs the real production path
without waiting for a timer.

### Engine

`engine_test.go` — one test per behaviour, all deterministic:

| Test | What it pins down |
|---|---|
| `TestPriorityOrder` | HIGH before MEDIUM before LOW |
| `TestFIFOWithinPriority` | Fifty messages come out in submission order |
| `TestGroupOrderAndLock` | One message per group in flight; the next waits for the ack |
| `TestVisibilityTimeoutRedelivers` | Redelivery, attempt count, and the stale receipt rejected |
| `TestDeadLetterAfterMaxRetries` | Dead-lettered on the right attempt, not before |
| `TestRetryKeepsGroupOrder` | A retried message returns to the front of its group |
| `TestTTLExpiry` | An expired message is never delivered |
| `TestExpiredInFlightStillAcks` | Expiry stops delivery, not completion |
| `TestStarvationReserve` | A starved low-priority message is served under sustained high load |
| `TestNoStarvationEscapeWhenNothingIsStuck` | Priority stays strict when nothing has waited |
| `TestIncarnationRejectsOldReceipt` | A receipt from a previous run is refused |

### Concurrency

`concurrency_test.go`, run under `-race`. The race detector is the method; a
stress test that happens to pass proves very little.

Eight producers and eight consumers move four thousand messages, then:

1. Everything submitted reached exactly one end state
2. Nothing acknowledged twice
3. Nothing delivered to two workers at once
4. Within a group, delivery order matched submission order
5. The queue drained

A second test has half the workers vanish without acknowledging while a sweeper
drives redelivery, and asserts the same invariants still hold.

**Assertion 4 found a real bug.** Sequence numbers were assigned *before* the slot
lock, so two concurrent producers could take 100 and 101 and then insert in the
opposite order. The fix makes the lock the point that orders both:

```go
s.mu.Lock()
// Seq is assigned here, not earlier: the lock is the point that orders two
// concurrent producers, so sequence numbers and list position have to be
// decided together or a group can end up out of order.
m.Seq = q.nextSeq()
```

### Write-ahead log

`wal_test.go`, against a real temporary directory. The one that matters is
`TestTornTailIsTruncated`: it appends junk to a good log, replays, asserts the
good records survive, then appends again and re-reads — which is what proves the
truncation happened rather than the read merely stopping.

### Gateway

`logic_test.go`, with mockgen-generated repos:

| Test | What it pins down |
|---|---|
| `TestCreateQueueRejectsStarvationAboveTTL` | A config that would silently shred low-priority work is refused |
| `TestConfigCacheAvoidsSecondRead` | Three reads, one database call |
| `TestMissingQueueIsCachedBriefly` | A typo in a loop does not hammer Postgres |
| `TestEnqueueFailsWhenOwnerIsDown` | 503, and no reassignment |
| `TestPriorityParsing` | Numbers and names, and what is rejected |

### End to end

`backend/tests/harness` drives the real gateway API:

```bash
go run ./backend/tests/harness -producers 8 -consumers 8 -messages 500
go run ./backend/tests/harness -queue h2 -distributed -producers 8 -consumers 8 -messages 300
```

It checks the same five invariants against a running cluster, for both queue
types.

**Each producer owns its own groups.** With several producers writing to one
group there is no defined submission order to check delivery against — an early
version of the harness got this wrong and reported inversions that were an
artifact of the test, not the server.

### Cluster behaviour

Verified by hand against `docker compose`:

| Scenario | Result |
|---|---|
| Three nodes, create a queue | Placed on one, exact counts |
| Distributed queue, 64 slots | Spread 22/21/21 across three nodes |
| Scale 3 → 6 | Slots redistributed across all six, messages intact |
| Kill the owning node | Normal queue 503, others unaffected, placement not reassigned |
| Restart it | Replays, queue back with its messages, incarnation bumped |
| Harness, both queue types | All invariants pass |
