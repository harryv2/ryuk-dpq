# LLD 6 — Metrics and Testing

What the system reports, and how it is proven correct.

Implements: HLD Sections 15, 19.

---

# Part A — Metrics

## 1. How counters are kept

Every counter is updated as things happen, under the lock that already protects the
structure it describes. Nothing is computed by walking a queue.

```go
type slotStats struct {
    enqueued, acked, expired, requeued, deadLettered, escapes uint64
    ready [3]int32          // bucketed by priority: low, medium, high
    inflight int32
}
```

`ready` is kept as three buckets rather than 101 counters because that is what gets
reported (§3), and maintaining the aggregate incrementally avoids summing on scrape.

Reading a slot's stats takes its lock briefly. Scraping a node walks its slots one at
a time, never holding two locks, so a scrape cannot block traffic across a queue.

**Oldest message age** is the only value not stored directly. It is the enqueue time
of the head of each non-empty band, minimised across slots:

```go
func (s *slot) oldestReady(now time.Time) time.Duration {
    oldest := time.Duration(0)
    for p := range s.bands {                    // sparse: only bands in use
        g, ok := s.peekGroup(p)
        if !ok { continue }
        m, ok := g.msgs.front()
        if !ok || m.expired(now) { continue }
        if age := now.Sub(m.EnqueuedAt); age > oldest { oldest = age }
    }
    return max(0, oldest)
}
```

Bounded by the number of priorities actually in use, not by queue depth. The clamp at
zero matters after a failover, where a new leader's clock may read earlier than a
message's recorded enqueue time (HLD §11).

---

## 2. What is exported

Required by the specification:

```
ryuk_queue_oldest_message_age_seconds{org, queue}
ryuk_queue_ready_messages{org, queue, priority}      # low | medium | high
ryuk_queue_inflight_messages{org, queue}
ryuk_queue_enqueued_total{org, queue}
ryuk_queue_acknowledged_total{org, queue}
ryuk_queue_dead_lettered_total{org, queue}
```

Everything else:

```
ryuk_queue_starvation_escapes_total{org, queue}      # capacity signal
ryuk_queue_expired_total{org, queue}
ryuk_queue_redelivered_total{org, queue}
ryuk_queue_groups_locked{org, queue}
ryuk_slot_depth{org, queue, slot}                    # finds a hot group
ryuk_sweep_lag_seconds{node}
ryuk_wal_fsync_seconds{node, quantile}
ryuk_wal_pending_bytes{node}
ryuk_placement_version{node}                         # detects a stale watcher
ryuk_gateway_stale_route_total{node}
```

`starvation_escapes_total` is the one to alert on. If it is climbing, consumers
cannot keep up with urgent work and low-priority work is only moving because of the
reserve (HLD §13). No required metric shows this.

`placement_version` is a debugging metric that earns its place: a node or gateway
stuck on an old version is the cause of a whole class of confusing routing failures,
and comparing the gauge across the fleet finds it in seconds.

---

## 3. Cardinality

Priority is reported in **three buckets, not 101 levels**. A hundred label values per
queue is a monitoring problem rather than useful detail.

The org label is the real risk. A thousand orgs with ten queues each is 10,000 series
per metric, and there are ten metrics.

```go
func (r *Registry) orgLabel(org string) string {
    if r.tracked.Has(org) { return org }
    return "other"
}
```

The largest tenants by volume are reported individually and the rest are aggregated,
with the tracked set recomputed hourly. Above a few hundred orgs this is the
difference between a monitoring system that works and one that falls over (HLD §15).

`ryuk_slot_depth` carries a slot label, which is 64 series per queue. It is reported
**only for slots above a depth threshold**, since its purpose is finding a hot group
and an even queue has nothing to say.

---

## 4. Endpoints

```
GET /metrics                      Prometheus text, counters, node-local
GET /v1/queues/{name}/stats       JSON, includes rates over a one-minute window
```

Counters, not rates, on the Prometheus endpoint. Computing rates is the monitoring
system's job, and doing it here loses information and breaks when the scrape interval
changes.

The specification asks for throughput as a rate per second, so the JSON endpoint
returns computed rates as well. A caller who just wants a number gets one without
running Prometheus.

Aggregation across machines is Prometheus's job:

```promql
sum by (org, queue) (ryuk_queue_ready_messages)
max by (org, queue) (ryuk_queue_oldest_message_age_seconds)   # max, not sum
```

Building fan-out aggregation into the service would mean an RPC to every node on
every scrape, duplicating what the monitoring system already does.

---

# Part B — Testing

## 5. Layers

| Layer | Runs against | Catches |
|---|---|---|
| Unit | The engine, no transport | State machine mistakes |
| Concurrency | The engine, with the race detector | Missing synchronisation |
| Property | Engine vs. a naive model | Bugs in the fast structures |
| Chaos | Engine with misbehaving workers | Lease and retry mistakes |
| Recovery | Engine plus a real log | Durability mistakes |
| Integration | Gateway plus nodes | Routing and translation mistakes |
| Benchmark | Everything | Regressions in the latency budget |

The engine has no imports outside the standard library (LLD 1 §1), so the first five
run at full speed with no server, no disk and no network.

---

## 6. Deterministic time

Nothing sleeps.

```go
clk := queue.NewFakeClock()
q := queue.New(cfg, clk, cluster)

id, _ := q.Enqueue(payload, 75, "")
_, receipt, _ := q.Dequeue()

clk.Advance(31 * time.Second)          // past the visibility timeout
q.SweepExpiredLeases()

_, r2, ok := q.Dequeue()
require.True(t, ok)                    // redelivered
require.NotEqual(t, receipt.Epoch, r2.Epoch)
require.Error(t, q.Ack(receipt))       // the old receipt is now stale
```

Two design decisions exist mostly to make this possible: the clock is supplied rather
than read from the system, and sweeps are callable directly instead of only firing on
a timer. Both are in LLD 1.

A sleep-based version of the test above would take 31 seconds, would be flaky under
load, and could not test a 12-hour timeout at all.

---

## 7. Concurrency

```go
func TestConcurrentProducersAndConsumers(t *testing.T) {
    q := queue.New(cfg, queue.SystemClock{}, cluster)

    const (
        producers  = 16
        consumers  = 16
        perProducer = 5000
    )

    sent := &sync.Map{}     // id -> priority
    got  := &sync.Map{}     // id -> ack count

    // ... producers enqueue, consumers dequeue and ack ...

    // 1. conservation
    require.Equal(t, producers*perProducer, count(sent))
    // 2. nothing lost
    sent.Range(func(id, _ any) bool {
        _, ok := got.Load(id)
        require.True(t, ok, "message %v never delivered", id)
        return true
    })
    // 3. nothing acknowledged twice
    got.Range(func(_, n any) bool {
        require.Equal(t, 1, n, "double acknowledgment")
        return true
    })
}
```

Run with `-race`. The race detector reports unsynchronised access directly rather
than waiting for a bug to appear by chance, which is why it is the methodology and
not a stress test that happens to pass.

### Group ordering, the test that finds real bugs

```go
func TestGroupOrderUnderConcurrency(t *testing.T) {
    // 50 groups, 200 messages each, random priorities, 32 consumers
    // every consumer records (group, sequence) as it receives

    for group, seen := range delivered {
        require.IsIncreasing(t, seen, "group %s delivered out of order", group)
    }
}
```

Random priorities are the point. A group whose messages span priorities changes bands
as its head is consumed (LLD 1 §4), which exercises the version-stamped band entries.
That is where the subtle bug lives: clearing `inBand` on a stale entry lets a group be
dispatched twice, and only this test would notice.

---

## 8. Property testing

```go
func TestAgainstModel(t *testing.T) {
    real  := queue.New(cfg, clk, cluster)
    model := newNaiveModel(cfg)     // one lock, one list, no slots, no bitmap

    for i := 0; i < 100000; i++ {
        op := randomOp(rng)
        r1, r2 := op.Apply(real), op.Apply(model)
        require.Equal(t, r2, r1, "diverged at op %d: %v", i, op)
    }
}
```

The model is written to be obviously correct rather than fast: one mutex, one slice,
linear scan for the highest priority. Any divergence is a bug in the fast structures —
the bitmap, the band deques, the version stamps — and a hand-written test would not
have thought to try the sequence that found it.

---

## 9. Chaos

Consumers that behave badly, mixed at random:

| Behaviour | What it should exercise |
|---|---|
| Take a message and never acknowledge | Lease expiry and redelivery |
| Acknowledge twice | Idempotence |
| Acknowledge with a stale receipt | Epoch rejection |
| Negative-acknowledge repeatedly | Retry counting and dead-lettering |
| Acknowledge after the queue is deleted | Graceful rejection |

Every invariant from §7 must still hold, plus one more: **no message is ever out with
two consumers at the same time**. Consumers record intervals and the checker looks for
overlaps on the same message id.

---

## 10. Recovery

```go
func TestSurvivesKill(t *testing.T) {
    dir := t.TempDir()
    // subprocess: enqueue 10k, dequeue and ack half, then SIGKILL mid-flight
    acked := runAndKill(t, dir)

    q := recoverQueue(t, dir)
    remaining := drain(q)

    for id := range acked {
        require.NotContains(t, remaining, id, "acknowledged message came back")
    }
    require.Equal(t, 10000-len(acked), len(remaining))
}
```

A real subprocess and a real `SIGKILL`, because the failure being tested is the
process disappearing without running any cleanup. An in-process simulation would let
deferred code run and would not exercise the torn-tail path.

Paired with the log-level tests in LLD 2 §14: torn tail at every byte offset, single
bit flips, and replaying one log twice to prove the result is identical.

---

## 11. Harnesses

Two commands, which are also the producer and consumer stubs the assignment asks for.

```
cmd/ryuk-load/
    -queues 4 -producers 32 -consumers 32 -rate 5000
    -priorities 25,50,75 -groups 100 -ack-rate 0.95 -crash-rate 0.01
```

Runs real HTTP against a running gateway and prints observed throughput, latency
percentiles, and whether the invariants held.

```
cmd/ryuk-verify/
```

Consumes a queue and checks the invariants continuously, so it can run alongside a
chaos test or a rebalance. This is what proves a slot migration did not lose or
duplicate anything: start it, move slots underneath it, and see whether it complains.

---

## 12. Benchmarks

```
BenchmarkEnqueue                       measures the submission path
BenchmarkDequeue                       measures dispatch
BenchmarkDequeueContended/slots=1      shows why striping exists
BenchmarkDequeueContended/slots=64
BenchmarkDispatchPriorities/n=3        shows the bitmap is flat
BenchmarkDispatchPriorities/n=101
BenchmarkWALAppend/sync=interval
BenchmarkWALAppend/sync=always
```

The two pairs exist to demonstrate specific claims rather than to produce numbers.
`slots=1` against `slots=64` shows lock striping working. `n=3` against `n=101` shows
that priority count does not affect dispatch cost, which is the whole reason for the
bitmap (LLD 1 §5.1).

Benchmarks run in CI with a regression threshold, since the latency budget in HLD §16
is only credible if something checks it.

---

## 13. Integration

Started with a gateway and three nodes in one process, wired over real gRPC:

| Test | Asserts |
|---|---|
| End to end | Create, enqueue, dequeue, acknowledge, metrics all agree |
| Stale placement map | Gateway retries once and succeeds |
| Node dies mid-dequeue | Unconfirmed leases release immediately, not after a timeout |
| Gateway dies mid-dequeue | Same, from the other side |
| Slot migration under load | No loss, no duplication, group order preserved |
| **Tenant isolation** | Two orgs, same queue names, same group keys, no shared slot |
| Queue deleted under load | In-flight work finishes, new work rejected |
| Quota exceeded | 503 with `Retry-After`, nothing already accepted is dropped |

The tenant isolation test is the one that would catch a missing separator byte in the
slot hash (LLD 1 §8), where org `a` queue `bc` and org `ab` queue `c` collide. That
bug is invisible in every single-tenant test and corrupts ordering in production.

---

## 14. What is not tested, and why

| Not covered | Reason |
|---|---|
| Replication and quorum commit | Not built (HLD §18) |
| Coordinator election under partition | Needs multi-process network control |
| Cross-region behaviour | No second region |
| Placement group migration | Slot migration is tested; group-level is coordinator logic |
| Sustained multi-hour load | CI time; the harness supports it manually |

Listing these matters as much as the coverage. A test suite that does not say what it
skips reads as though it covers everything.
