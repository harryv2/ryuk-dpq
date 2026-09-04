# Placement and migration

How a distributed queue chooses its machines, and how work moves between them
when the cluster changes. The approach borrows from Cassandra where a queue
behaves like a keyspace, and deliberately does not where it does not.

---

# What Cassandra does, and what fits

| Cassandra | Fits here? |
|---|---|
| **Vnodes** — many small token ranges per node, so a join draws from everyone rather than one neighbour | **Already have it.** 64 slots per queue *is* the vnode idea |
| **Replica placement** — walk the ring, take the next N distinct nodes | **Yes.** Rendezvous already produces a ranking; take the top N |
| **Bootstrap with a pending endpoint** — a joining node receives new writes while historical data streams to it | **Yes, and it is the missing piece.** Removes the write outage entirely |
| **Hinted handoff** — a write for a down replica is parked and replayed on return | **Yes.** Turns "owner is down" from a 503 into a delay |
| **Anti-entropy** — background comparison converges replicas | **Yes.** Replaces roll-forward/roll-back reconciliation |
| **Leaderless replicas** — any replica serves a read | **No.** Two replicas serving one queue hand the same message to two consumers |
| **Gossip membership** | **No.** etcd is already here and gives stronger agreement, which placement needs |
| **LSM storage with tombstones** | **No** — and this is the famous anti-pattern |

## The anti-pattern, named

"Queues on Cassandra" is a documented failure. Deleting a consumed message
writes a tombstone, and reading the head of the queue scans over every tombstone
before it. Throughput collapses as the queue ages.

This design does not inherit that, because messages live in memory with a
write-ahead log rather than in an LSM tree that the read path scans. But the
lesson does transfer: **acknowledged messages must be reclaimed eagerly**. That
is exactly what `QueueLogic.Compact()` is for, and it is currently dead code that
nothing calls — so logs grow without bound today.

---

# Part 1 — Placement

## One axis now, two later

**`placementWidth W`** — how many machines one queue spans. Bounds blast radius
and fan-out cost. This is what gets built.

Replication is out of scope, as it has been from the start. When it arrives it is
a *second*, independent axis — how many machines hold each slot — chosen from
within the same candidate set. Nothing below should preclude it, which is why
placement is expressed as a ranking rather than a single winner: taking the top
RF instead of the top 1 is then a one-line change.

## Width

Rendezvous already ranks every member for a key. Take the top W:

```go
func CandidatesFor(key string, members []Member, width int) []Member {
    ranked := rankByScore(key, members)
    if width <= 0 || width > len(ranked) {
        width = len(ranked)          // a small cluster is its own limit
    }
    return ranked[:width]
}
```

Then place slots within that set. Still a pure function of the member list, still
no coordination, and a new node only enters a queue's set if it outranks that
queue's current W-th choice.

Measured, on twenty nodes:

| | width 4 | width 20 |
|---|---|---|
| Nodes shared with another queue | 0.8 on average | all 20, always |
| Chance two queues share every node | 0.02% | 100% |
| `Stats` fan-out | 4 gRPC calls | 20 |
| Slots per node | 16 | 3.2 |

Width costs no throughput: the engine does hundreds of thousands a second per
node while the gateway ceiling is ~2,500/s, so the transport binds either way.
Default 6, fixed at creation like `distributed`.

## A note for when replication does arrive

Worth recording now, because it constrains the shape: a queue **cannot** use
Cassandra's leaderless replicas. Cassandra lets any replica answer because its
writes are commutative — last write wins per cell. A queue's read is a
*mutation*: dequeue moves a message to in-flight, increments its attempt count
and locks its group. Two replicas serving independently would hand the same
message to two consumers and break group ordering.

So replication here means one leader per slot with followers for durability and
failover — Kafka's ISR shape, not Cassandra's — and it needs leader election per
slot. That is a larger piece of work than everything in this document combined,
which is why it stays out of scope.

---

# Part 2 — Migration without a write outage

## What is wrong today

```go
transfer, err := Freeze(from, spec, slots)   // DRAINS the slots off the old owner
Absorb(to, transfer)                          // writes them on the new owner
SetOwner(slot, to, nextGen)                   // records the new owner
```

`Freeze` drains, so between step one and step two the only copy is a local
variable in the **stateless** tier. Absorb fails or the gateway dies, and the
messages are gone from both nodes' memory. Nothing recovers it; the code logs a
warning and continues.

Meanwhile the frozen slot refuses writes. Producers get `Moved`, the gateway
retries once, and a migration slower than one round trip surfaces as a 503.

## The pattern, and why the full version is not worth it

Cassandra's bootstrap does not stop writes. A joining node becomes a **pending
endpoint**: coordinators send it new writes immediately while historical data
streams behind, and ownership flips when streaming completes.

That is the right pattern for Cassandra, where a bootstrap streams gigabytes for
minutes. **It is the wrong trade here, and the measurements say so.**

What it would cost:

- **Split routing.** Writes would resolve to `moving_to`, reads and
  acknowledgements to `owner_node`. Today `SlotOwners` is one map resolved in one
  place; this makes every call site pick a side, and an acknowledgement must
  follow the read or it lands where the lease is not.
- **Per-slot lifecycle in the engine.** `frozen` is one `atomic.Bool` on the
  *queue*. Accepting-but-not-serving is a third state, checked on the dequeue hot
  path.
- **A delta catch-up phase**, because the old owner keeps mutating while it
  streams, plus the failure modes that phase brings.

What it would buy, measured: the freeze during a real migration is **2.3 ms per
slot batch, 16 ms for an eight-batch move**. Under load — 400 writes across a live
migration — **zero failed**, because the gateway's existing retry already absorbs
it.

So it is two protocol phases, a routing split and a hot-path change, to close a
2 ms window that a retry already closes. The proportionate fix is **backoff on the
retry**: `withOwner` currently makes two attempts with no delay, so a retry can
land inside the same 2 ms freeze. Three attempts with a short backoff make the
window unhittable, in three lines.

That leaves the migration free to simply freeze for the whole move, which is far
simpler:

| # | Step | Old owner | New owner | Placement |
|---|---|---|---|---|
| 0 | steady | serving | — | old |
| 1 | `PrepareMove` — freeze, **snapshot** (do not drain) | frozen, holds data | — | old |
| 2 | `Absorb` | frozen, holds data | staged on disk, not serving | old |
| 3 | commit | frozen, holds data | staged | **new** |
| 4 | `DiscardMove` | released, log truncated | **serving** | new |

The one thing worth keeping from Cassandra's bootstrap is the ordering mechanism,
and it is already built: `Absorb` merges by `Seq`, and `nextSeq` prefixes the
ownership generation, so an older owner's messages sort ahead of a newer owner's
whatever order they arrive in. That is what makes step 2 safe to retry.

## Recovery is convergence, not a transaction

The previous draft of this document proposed a five-phase commit with a
reconciler that rolled forward or back. That is a distributed transaction, and it
is more machinery than the problem needs.

Anti-entropy is simpler and self-healing. Nodes already report every queue they
hold via `StatsAll`. Compare that against placement on a timer:

| Observed | Action |
|---|---|
| a node holds a slot it does not own | the move committed — drop it |
| a node owns a slot that is frozen | the move did not commit — thaw it |

No roll-back, no stuck states, no coordinator log. Every discrepancy has one
correct resolution and repeating the check is harmless. This also fixes the
**edge-triggered** rebalancer: the sweep is level-triggered by construction, so a
queue that missed its window converges on the next pass instead of waiting for an
unrelated membership change.

## Hinted handoff — optional, and only half a fix

When a slot's owner is down the gateway returns 503, so a dead machine takes part
of the queue offline for writes. Parking the write on the next-ranked node and
replaying it when the owner returns would fix that.

Two things to be clear about before treating it as the answer to node loss:

**It only helps writes.** The backlog already on the dead machine is still
unreachable. Reads of those slots fail either way, so the queue is
write-available and read-unavailable rather than available.

**Sequence numbers need care.** `Seq` is assigned by the owning node under its
slot lock, and it is what orders a group. A hint-holder cannot assign one without
risking a collision or a misorder. The workable shape is to store the hint
without a `Seq` and have the owner assign one as it drains hints on return,
before it opens for new writes — so hinted messages sort ahead of anything
accepted afterwards.

**And the hint itself has one copy.** If the hint-holder also dies, the write is
lost. Hinted handoff is a transient second copy of recent writes, not durability.

Worth doing after the core is solid, not before.

---

# Sequencing

| # | Change | Buys |
|---|---|---|
| 1 | `Freeze` snapshots instead of draining; discard only after the new owner confirms | closes the loss window — the sharpest edge, and small |
| 2 | Anti-entropy sweep: compare `StatsAll` against placement on a timer, reconcile | convergence; fixes the edge-triggered and stuck-in-`migrating` faults together |
| 3 | Lease on the migration lock (`migrating_by`, `migrating_since`) | an abandoned migration can be taken over |
| 4 | `placementWidth` — schema, `CandidatesFor`, API, UI | bounded blast radius and fan-out; fewer queues in the migration path |
| 5 | Retry with backoff in `withOwner` — three attempts, short delays | the 2 ms freeze stops being reachable |
| 6 | Call `Compact` on a timer | bounded log growth — **only after 1 and 2**, since compaction is what makes a lost migration permanent |
| 7 | Hinted handoff for a down owner *(optional)* | writes survive a machine being down; reads still do not |

Steps 1–3 are small and remove the sharpest edges, and 5 is three lines. Step 6
must not come earlier. Step 7 is worth weighing on its own merits once the rest
is in.

## Proving it

Fault injection in `integration/`, on the existing Docker harness: kill the
gateway, the old owner and the new owner at each phase boundary, and after each
assert

- every acknowledged message is delivered at least once
- no message is served by two owners at the same time
- no write is rejected while a migration is in flight, given the retry budget
- the queue drains to empty
- no slot is left pending

Plus a soak: continuous produce and consume while nodes join and leave, with the
invariants checked throughout.

## What is still not solved

There is one copy of the data. A machine that dies holding a slot takes that
slot out of service until it returns, and if its disk is gone so are those
messages. Nothing in this document changes that — it makes the *handoff* safe and
the *placement* deliberate, not the loss of a machine survivable.

Replication is the only answer to that, and it stays out of scope. What this work
does is leave the ground ready for it: placement expressed as a ranking, moves
that are convergent rather than transactional, and a streaming path between
owners that a follower would use unchanged.
