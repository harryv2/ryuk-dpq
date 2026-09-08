# Replicating a Distributed Priority Queue

**High-Level Design — concepts**

---

Ryuk keeps **one copy of every message**. This document is about what the system would
look like if it kept three, and what that costs. It is a design, not a plan: it does not
say what Ryuk would change to get there.

The current limit is recorded in [`RYUK_HLD.md`](RYUK_HLD.md) §11 and §12. A machine that
dies takes its slots out of service until it comes back, and a lost disk is a lost slot.

---

## 1. What One Copy Actually Costs

Three failures, in order of difficulty.

| Failure | With one copy |
|---|---|
| The process is killed | **Survivable.** The disk is fine; a write that reached the OS is still there |
| The machine is away for an hour | **Unavailable.** The messages exist and cannot be served |
| The disk is gone | **Lost.** Nothing recovers them |

Only the first is survivable. The other two need the message to exist in more than one
place *before* the producer is told it was accepted.

That last clause is the definition worth being precise about. **No data loss** does not
mean nothing is ever lost. It means:

> If a producer was told a message was accepted, no single machine failure can remove it.

A producer that never got an answer is owed nothing.

---

## 2. Why a Queue Cannot Be Cassandra

Replicating a key-value store is a solved problem. A queue is a different problem, and
this is the reason a queue needs a leader.

**A dequeue is a write.** Handing a message to a worker changes state: the message becomes
invisible to others, its group locks, its attempt count rises, a lease starts. A store's
read changes nothing. A queue's read changes everything.

**Delivery must be exclusive.** Exactly one worker gets each message. That is mutual
exclusion, and mutual exclusion needs one decision point. Two machines answering "give me
work" independently will hand the same message to two workers, and no later reconciliation
undoes work already done.

**Ordering needs a sequencer.** A group promises its messages arrive in submission order.
An order is not a value two machines can merge. It is a sequence somebody has to decide.

**Acknowledging deletes.** Every processed message is a removal. In a store that resolves
conflicts by timestamp, a delete is a tombstone that has to be kept and replicated until
every replica has seen it.

That last point is why "use Cassandra as a queue" is a documented anti-pattern rather than
a design option. Every acknowledgment writes a tombstone, every read scans past the pile of
them, and throughput collapses as the queue does its job.

So: enqueues alone could be leaderless, because appends commute. **Dequeues cannot**, and
dequeues are what make it a queue rather than a log.

---

## 3. What It Looks Like

Take a queue with 64 slots on a 6-machine cluster, keeping 3 copies of everything.

The instinct is that 6 machines and 3 copies means two groups of three — two writers and
four spares. **That is not the shape.** Each slot picks its own three machines:

```
slot  0  →  n1 (leader)   n2           n3
slot  1  →  n2 (leader)   n3           n4
slot  2  →  n3 (leader)   n4           n5
slot  3  →  n4 (leader)   n5           n6
slot  4  →  n5 (leader)   n6           n1
slot  5  →  n6 (leader)   n1           n2
slot  6  →  n1 (leader)   n3           n5
   ...        64 slots, each choosing its own three
```

The arithmetic:

```
64 slots × 3 copies  = 192 replicas
192 ÷ 6 machines     =  32 replicas per machine
64 leaders ÷ 6       ≈  11 leaderships per machine
```

So every machine looks the same:

```
n1:   leader   of ~11 slots     ← accepts writes for these
      follower of ~21 slots     ← stores copies, votes, ready to take over
      32 replicas in total
```

**All six machines are writers.** Not two. Every machine leads some slots and follows
others. There is no set of "the leaders" and a separate set of "the replicas" — that
distinction exists per slot, never per machine.

### Is that multi-leader?

No. One question separates the three models:

> For **one piece of data**, can two machines accept conflicting writes at the same time?

| Answer | Model | Examples |
|---|---|---|
| No — one machine owns it | **Single-leader** | Kafka, Raft, a primary database |
| Yes, reconciled afterwards | Multi-leader | Multi-datacentre primaries, CouchDB |
| Yes, reconciled at read | Leaderless | Dynamo, Cassandra, Riak |

For slot 42, only its leader accepts writes. Never two. So it is single-leader, per slot.

It *looks* multi-leader because dozens of machines accept writes at once. But they are
writing to different slots. A database sharded across ten primaries is not "multi-leader"
— it is ten single-leader shards. Same here.

---

## 4. Consensus

Consensus is how a group of machines agree on a decision and never disagree afterwards,
even if some crash halfway. For a queue the decision, made over and over, is: *what is
entry number N in this slot's log?*

It guarantees agreement (no two replicas think entry N is different), durability (once
decided it survives a minority failing), and progress (decisions keep happening while a
majority is alive).

Note what is **not** required: every replica agreeing. That would stall on a single
failure. A majority is enough, and that is what makes it usable.

### One write, start to finish

```
producer ──► leader                 "enqueue M"

leader:   appends M as entry N to its own log      (not committed yet)
leader ──► follower A    "store entry N"
leader ──► follower B    "store entry N"

follower A:   writes it, replies OK
follower B:   slow or dead, no reply

leader:   itself + A = 2 of 3 = majority
          entry N is COMMITTED
          apply it — the message becomes visible

leader ──► producer                 "accepted"
```

The producer hears "accepted" only after a majority holds the entry. That one rule is the
whole durability guarantee.

### Why the message survives the leader dying

Say the leader dies a millisecond later. The two survivors hold an election:

```
follower B asks for votes.   A refuses — "my log has entry N, yours does not."
follower A asks for votes.   B agrees.
A becomes leader, carrying entry N.
```

That is not luck. It follows from one property:

> **Any two majorities of the same group overlap.**

Entry N was stored on a majority. A new leader needs votes from a majority. Those two sets
share at least one machine, and that machine refuses to elect anyone missing entry N. So a
committed entry is on every possible future leader. It cannot be lost.

The converse matters as much. If only the leader had stored M, it would never have said
"accepted". Nothing acknowledged is lost. Things not acknowledged may be.

### Consensus is not the same as replication

| | Replication | Consensus |
|---|---|---|
| Does | Copies data to other machines | Agrees on order, and on which writes count, despite failure |
| After failover | Whatever happened to arrive is there | Committed entries are provably there |

Redis Cluster replicates without consensus. The primary answers the client immediately and
ships the write to replicas in the background, so a failover in that window loses an
acknowledged write. **Adding copies is not the same as not losing data.**

---

## 5. Slot and Shard

One more unit sits above the slot. In the simplest configuration they are the same thing,
so it is worth knowing when they are not.

```
  group key "user-123"
        │ hash
        ▼
    slot 42            the ordering unit
        │ belongs to
        ▼
    shard 7            the replication unit — one log, one leader, three copies
        │ lives on
        ▼
    n3 (leader), n8, n11
```

| | **Slot** | **Shard** |
|---|---|---|
| Is | A bucket a group key hashes into | A replica group: one log, one leader, R copies |
| How many | Fixed when the queue is created | At most one per slot |
| Exists to | Keep a group ordered; spread keys | Replicate; accept writes; be placed on machines |
| Has a leader | No | **Yes** |
| Has a log | No | **Yes** |
| Can be split | **No** — that breaks group ordering | **Yes**, and cheaply |

A slot never spans two shards. A shard owns a whole number of slots.

**Set shard count equal to slot count and the second word disappears** — one shard per
slot, which is the sensible default. The only reason to put several slots in one shard is
overhead: every shard runs its own heartbeats, election timer, log and memory, and a large
cluster with many queues can reach tens of thousands of them. Systems that live there batch
heartbeats per machine-pair and share one storage engine.

**Slot count is the ceiling on everything.** A slot cannot be split without breaking the
ordering it exists for, so a queue can never have more shards than slots, more leaders than
slots, or more write parallelism than slots. That number is chosen at creation and fixed
for the queue's life. It is the most consequential number in the design.

---

## 6. What Gets Replicated

This decides whether the queue stays fast.

| Replicated — costs a round trip | Leader-local — costs nothing |
|---|---|
| Enqueue | Which messages are in flight |
| Acknowledge | Lease deadlines |
| Expiry, dead-lettering | Group locks |
| Delayed messages | Dispatch hints, starvation cursor |

**Enqueue and acknowledge must be replicated.** Losing an enqueue loses a message. Losing
an acknowledgment resurrects finished work.

**Leases must not be.** Replicating a lease puts consensus on the dequeue path — a network
round trip for every message handed out, on the hottest operation there is.

The consequence: a failover forgets what was in flight. Everything unacknowledged becomes
ready and is redelivered. That is a burst of duplicates, bounded by the visibility timeout.

It is the right trade, because **at-least-once already promises this.** A worker that
crashes without acknowledging causes exactly the same redelivery today. Consumers must
already be idempotent. Paying a round trip on every dequeue to reduce something consumers
must handle anyway is a bad bargain.

There is a neat consequence. Because followers never see a dequeue, their copy still holds
the message in its group. A new leader therefore already has every unacknowledged message
as ready, with no recovery step. Redelivery falls out of the design rather than being
built.

### Attempt counts

Attempts drive dead-lettering, so they cannot reset on every failover or a poison message
never retires. But they do not need replicating on every delivery either. An attempt count
only has to be durable when a failure is recorded — which is exactly when a negative
acknowledgment or an expiry is already being replicated. Carry the count inside those
entries and it stays durable without touching the dequeue path.

### Every replica must reach the same state

Three ordinary things break that:

- **Generated ids.** An id created while applying an entry differs on each replica. The id
  belongs *in* the entry, decided once by whoever proposed it.
- **Reading the clock.** Submission time, expiry and delayed-delivery time travel inside
  the entry. A replica applying it a second later must not compute a different expiry.
- **Timers.** Replicas must not each discover expiry independently. The leader decides
  which messages expired and proposes that, naming them.

Per-queue sequence numbers become unnecessary — the log index is already a total order
within a shard, which is what they were for.

---

## 7. Adding Machines Does Not Add Writes

This is the part that surprises people, so here it is with numbers.

Only a shard's leader accepts writes. A queue with **5 shards has 5 writers**, no matter
how large the cluster:

```
15 machines, 5 shards:     5 writing, 10 holding copies
30 machines, 5 shards:     5 writing, 25 holding copies    ← nothing improved
```

Doubling the machines changed nothing, because **writers are shards, not machines.**

Three separate operations fix it, with very different costs.

### Splitting buys writers — and copies nothing

A shard's three replicas already hold all of its data. Telling them to treat it as two
shards from an agreed point in the log is a bookkeeping change applied identically on each.
**No data crosses the network.**

```
BEFORE — 15 machines, 5 shards

  n1  n2  n3      n4  n5  n6      n7  n8  n9    n10 n11 n12   n13 n14 n15
  L   F   F       L   F   F       L   F   F     L   F   F     L   F   F
  └── shard 0 ──┘ └── shard 1 ──┘ └─ shard 2 ─┘ └─ shard 3 ─┘ └─ shard 4 ─┘

  5 writers. 10 machines holding copies and doing nothing.
```

n2 and n3 already have a complete copy of shard 0. They are simply not allowed to write.
Split shard 0 into three, and they are:

```
AFTER SPLIT — same 15 machines, 15 shards

  n1  n2  n3      n4  n5  n6      ...
  L   L   L       L   L   L

  15 writers. Nothing idle. Three times the write throughput.
  Data copied: zero.  Time taken: seconds.
```

### Moving buys machines — and copies half the data

Relocating a replica means copying it. Doubling a cluster moves about **half the data**, no
matter how clever the placement, because half the replicas belong somewhere new.

A replica moves without ever weakening the shard:

```
1. add the new machine as a LEARNER — it receives data but does not vote
2. it downloads a snapshot from a follower, not the leader, which keeps serving
3. it replays the entries that arrived during the download
4. only now: a config entry makes it a voter, and the old replica leaves
5. the old replica deletes its copy
```

Promoting straight to voter would move the majority threshold before the newcomer held any
data — shrinking fault tolerance during exactly the risky window.

### Transferring leadership buys balance — and copies nothing

One message telling another replica to take over. Sub-second, no data moves. Balance by
**load, not count**: a machine leading one busy shard is busier than one leading three idle
ones.

### The three, side by side

| Operation | Moves data | Takes | Buys |
|---|---|---|---|
| **Split** | nothing | seconds | more writers |
| **Move** | ~half the data | hours, throttled | use of new machines |
| **Transfer leadership** | nothing | seconds | even load |

**The way to avoid all of this is to create enough shards at the start.** A queue created
with one shard per slot never needs splitting; adding machines is an ordinary rebalance.
Shards are cheap at creation and awkward to add later, so provision for the cluster expected
in a year, not the one running today.

---

## 8. Something Has to Decide Where Shards Live

One component decides placement. Nothing else may.

| Decides | Does not do |
|---|---|
| Which machines hold each shard | Touch message data |
| When to add or remove a replica | Route client requests |
| When to suggest a leadership transfer | Elect leaders — the replicas do that |
| When to split a shard | Sit on the read or write path |

Its loop, in strict order:

```
every few seconds:
  1. REPAIR   a shard below its replication factor  → add a replica
  2. BALANCE  a machine holding too many replicas   → move some away
  3. LEADERS  a machine leading too many shards     → transfer some

  throttle: a handful of moves in flight across the whole cluster
```

Repair before balance. Never start a cosmetic rebalance while a shard is one failure from
losing its majority.

It only issues instructions. Told to add a replica, the shard's own members stream the data
between themselves — the coordinator never carries any.

**There must be exactly one.** Two coordinators can decide the same shard should move to
two different places, or remove two different replicas at once and drop the shard below
quorum. Consensus protects the data *inside* a shard; it does nothing about two outsiders
issuing contradictory instructions about who holds it. A lease in the membership store makes
the role singular.

It is **not** a single point of failure. If it dies the cluster keeps serving — shards keep
accepting writes, failed leaders are still replaced by election. Only rebalancing stops. The
role is stateless, so a replacement rebuilds its view from membership and metadata.

Everyone arrives at this component: TiKV calls it the placement driver, Kafka the
controller, Elasticsearch the master node.

---

## 9. When Things Break

| Failure | What happens |
|---|---|
| Leader crashes | New leader within an election timeout. Committed data intact. In-flight leases forgotten, so those messages redeliver |
| Follower crashes | Nothing visible. A majority remains |
| Machine away an hour | The shard serves throughout. On return it is caught up from the leader's log, or given a snapshot if the log has moved past it |
| Disk corrupted | Caught by checksum. That replica refuses to serve the shard, is wiped, and refilled from a peer |
| Majority lost | The shard stops accepting writes — correct, since it cannot know what it missed |
| Network partition | The minority cannot reach a majority and cannot commit. The old leader is fenced by term and cannot acknowledge |

Two capabilities are needed that a single-copy system never needs.

**Telling corruption from lag.** A replica that is behind and a replica whose data is
damaged look similar from outside and need opposite treatment — one waited for, the other
wiped. A torn record at the *end* of a log is a crash and should be truncated. A bad
checksum in the *middle* is corruption and should take the replica out of service.

**Log retention.** A leader keeps entries until every replica has them, bounded by size and
age, then snapshots and discards. Too little strands a returning machine; too much fills the
disk.

**Catching up under load.** A returning machine downloads a snapshot, then replays what
arrived while it downloaded. If writes arrive faster than it catches up, it never converges.
Three things prevent that: snapshot from a follower rather than the leader, throttle the
transfer, and limit how many run at once. A very busy queue may only finish a catch-up
during a quiet period.

---

## 10. What It Costs

**Latency.** One network round trip from the leader to a majority, added to enqueue and
acknowledge. Dequeue is unchanged, because leases are not replicated.

Within a region that round trip is *cheaper* than the local `fsync` a single-copy system
needs for the same durability. Measured on this system, switching from a buffered flush to
an `fsync` per write cost about 2.4 ms at the median and nearly half the throughput — and
bought only survival of a power cut on one machine. A majority acknowledgment costs about
the same and survives losing the machine entirely.

**Storage.** Three copies is three times the disk and three times the write bandwidth.

**Tail latency.** The median barely moves; the tail gets worse. A slow follower, a pause or
an election all land in the tail. An election costs one election timeout of unavailability
for that shard — an availability gap, not a latency tax.

**Topology.** Same-zone round trips are a fraction of a millisecond, cross-zone a small
multiple. Cross-region is tens of milliseconds and will not fit a normal latency budget;
replicate across regions asynchronously, not by consensus.

---

## 11. What Replication Does Not Fix

**A hot group key.** Ordering for a key needs a single writer for that key. A key taking a
large share of traffic pins to one slot, one shard, one leader. Replication changes nothing
about that. Spread comes from having many keys.

**Priority across shards stays approximate.** Within a shard, order is exact. Across shards,
the routing tier asks whichever leader looks busiest — and *looks* is doing real work there,
because the information is a moment old and summarised. More shards means more approximation.
Strict global priority and horizontal write scaling pull against each other, and sharding
chooses scaling. Ordering *within a group* stays exact either way.

**The starvation reserve becomes per shard.** Reserving a share of deliveries for work that
has waited too long is decided where the work is. Spread over many shards it is a per-shard
guarantee, and "the oldest message in the queue" becomes "the oldest message each leader can
see".

**Duplicates.** Replication protects against loss, not duplication. Failover redelivers
in-flight messages by design. The guarantee stays at-least-once, and replication makes the
"at least" more visible, not less.

**Slot count.** Chosen at creation, and no amount of hardware raises it.

---

## 12. Prior Art

| System | Model | Note |
|---|---|---|
| **Kafka** | Single-leader per partition, synchronous replication to in-sync replicas | The closest shape. Partitions cannot be split, so they are over-provisioned at creation |
| **Redis Cluster** | Single-leader per hash slot, **asynchronous** replication | Structurally similar — 16384 hash slots mapped to owners — but a failover can lose acknowledged writes |
| **Cassandra** | Leaderless, quorum reads and writes, last-write-wins | Unsuitable as a queue: no exclusive claim, and acknowledge-as-delete produces tombstones |
| **TiKV / CockroachDB** | Consensus per range, ranges split automatically, a coordinator places them | Where splitting and the placement coordinator come from |
| **Pulsar** | Serving separated from storage | Brokers hold no data, so rebalancing moves none — a different answer to the same problem |
| **SQS** | Replicated internally, details unpublished | Sets the delivery contract most queues are measured against |

The recurring shape: many peers holding data and electing leaders among themselves, plus one
elected coordinator deciding who holds what.

---

## 13. Glossary

**Commit** — an entry stored by a majority. Once committed it is on every possible future
leader.

**Consensus** — agreement among machines on a decision, holding despite a minority failing.

**Fencing** — rejecting a request from a deposed leader by comparing an ever-increasing
term.

**Follower** — a replica that stores entries and votes, but does not accept writes.

**Leader** — the one replica of a shard that accepts writes.

**Learner** — a replica receiving data but not yet voting, used while it catches up.

**Quorum** — a majority; the smallest set that must agree.

**Replication factor (R)** — copies of each shard. R = 3 tolerates one failure.

**Shard** — the unit of replication and placement: one log, one leader, R copies.

**Slot** — the unit of ordering: a bucket a group key hashes into.

**Snapshot** — the state at a point in the log, used to seed a replica too far behind for
the log alone.

**Term** — an increasing number identifying a leadership period.
