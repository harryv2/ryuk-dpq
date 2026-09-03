# Ryuk — A Distributed Priority Queue

**High-Level Design**

---

## 1. What Ryuk Is

Ryuk is a priority queue service. Services submit work items with a priority. Worker
fleets pull those items, do the work, and tell Ryuk when they are done. Ryuk owns the
queues, tracks each message from submission to completion, and reports metrics.

Five requirements shape the design:

- Serve urgent work first, but never let low-priority work wait forever
- Process related messages in order, while unrelated messages run in parallel
- Never silently lose a message a producer was told we had accepted
- Add and remove machines at any time, and have work spread across them
- Serve many tenants from one cluster without letting one disturb another

These requirements fight each other. Serving urgent work first means low-priority work
may never run. Processing messages in order means they cannot run in parallel. Not
losing messages costs time on every write. Spreading a queue across machines costs the
ordering that made it useful. And serving many tenants affordably means sharing
machines, which is exactly what lets one tenant disturb another.

Most of this document is about resolving those conflicts.

---

## 2. The Problem

A queue sits between services that submit work and workers that perform it. The queue
itself is simple. The difficulty is keeping it correct while machines come and go,
while workers crash halfway through a job, and while hundreds of producers and
consumers run at once without coordinating.

Six questions drive the design.

**What does a worker get when it asks for work?** The most urgent item available. But
if urgent work keeps arriving, less urgent work must still run eventually.

**What happens when a worker dies mid-job?** The work goes back in the queue. But if
we put it back too quickly, we duplicate work a merely slow worker is still doing.

**What happens when a machine holding messages dies?** Nothing it accepted may be
lost, and it must recover without an operator.

**What happens when we add a machine?** It must take real work off the existing ones,
not just receive whatever arrives next.

**What ordering can we promise?** More usefully: what ordering still holds when ten
workers pull at once and machines move underneath them.

**What keeps one tenant off another's back?** Many customers share the cluster. They
must not see each other's queues, collide on each other's names, or slow each other
down simply by being busy.

---

## 3. Concepts

### Message

A payload plus metadata. The submitter chooses the payload, the priority, an optional
ordering key and an optional expiry. Ryuk never looks inside a payload.

### Priority

A number from 0 to 100, higher meaning more urgent. Three names sit on that scale —
LOW is 25, MEDIUM is 50, HIGH is 75 — leaving room above HIGH for emergencies and
below LOW for background work.

A number rather than three fixed levels means finer priorities later are a
configuration change, not a rewrite.

### Group

The ordering key. Messages sharing a group are delivered in the order they were
submitted, and **only one message from a group is in flight at a time**. Different
groups run fully in parallel.

A message submitted without a group becomes a group containing only itself, so it can
never be blocked and one mechanism handles both cases.

SQS FIFO queues work this way. Kafka does not, and the difference matters. Kafka binds
a consumer to a partition and keeps it there, so a consumer reading partition 3 cannot
see an urgent message in partition 5 — which would break the main thing we promise.
Ryuk locks a group when it hands out a message and unlocks it on acknowledgment, so no
worker is tied to anything.

### Slot

A partition of one queue, and the thing that actually holds messages. A normal queue
has **16 slots**, a distributed queue **64**. Section 4 explains why the two differ.

| Unit of | Because |
|---|---|
| **Storage** | Messages physically live in it |
| **Locking** | One lock per slot, so concurrent requests do not serialise |
| **Movement** | A slot moves as a whole; its log is its complete history |

A slot holds the ready lists for each priority, the messages of each group, the
messages currently out with workers, the lease timers, one lock and one write-ahead
log.

**A slot belongs to exactly one queue, and therefore to one org.** No two tenants ever
share one. Slots are invisible to clients and their number never changes.

### Node

A machine holding slots. It keeps their messages in memory, tracks their leases and
writes their logs. Nodes have no public listener — only gateways and other nodes reach
them.

### Org

A tenant. Every queue belongs to one org and is identified by the pair, org plus name.
Two orgs can both have a queue called `orders` and they are unrelated.

An org is a boundary in three ways. It **namespaces** names, so tenants pick their own
without coordinating. It **contains access**: a caller reaches only queues in its own
org. And it is a **contention boundary**, because a slot belongs to one queue and
therefore one org, so no hash collision can put two tenants behind the same lock.

The org is never sent in a request. It comes from the caller's credential, so it
cannot be changed by editing a URL.

### Queue

A named container with its own delivery settings. It belongs to one org and its name
need only be unique within that org.

---

## 4. Two Kinds of Queue

This is the central choice in the design, and the only placement decision a caller
makes.

```
POST /v1/queues { "name": "orders", "distributed": false }   ← default
POST /v1/queues { "name": "events", "distributed": true }
```

### Normal queue — one machine

All 16 slots live on one node.

```
   org_7f2a/orders  ──rendezvous hash──▶  node-2
                                          ├── slot 0 ─┐
                                          ├── slot 1  │  16 slots,
                                          ├── ...     │  16 locks,
                                          └── slot 15 ┘  one process
```

Sixteen because slots are only lock stripes here, and a benchmark of the full
enqueue, dequeue and acknowledge cycle shows the gain is flat past four. Sixteen
sits past that with margin for a machine with many cores, and costs a quarter of
the bookkeeping a distributed queue needs.

Because one process holds every slot, the dispatcher compares all of them and picks
the genuinely most urgent, oldest message. Counting is one local walk. Routing is one
hash and one hop.

| | What you get |
|---|---|
| Priority ordering | **Exact** |
| FIFO within a priority | **Exact** |
| Message counts | **Exact**, one request |
| Routing | One hash, one hop |
| Ceiling | **One machine** — a few hundred thousand messages a second |
| If that node dies | The whole queue is unavailable until it returns |

### Distributed queue — slots spread across machines

Each slot is placed independently, so one queue can occupy up to 64 nodes.

```
   org_7f2a/events/slot-0  ──▶ node-3      org_7f2a/events/slot-8  ──▶ node-1
   org_7f2a/events/slot-1  ──▶ node-1      org_7f2a/events/slot-9  ──▶ node-4
   org_7f2a/events/slot-2  ──▶ node-5      ...
```

| | What you give up | What you gain |
|---|---|---|
| Priority ordering | Approximate across machines | Throughput up to 64 machines |
| FIFO within a priority | Approximate across slots | |
| Message counts | Sum across machines, point in time | |
| Routing | Priority summaries, sometimes a second hop | |
| If a node dies | **Only its slots** are unavailable — the rest keep serving | Partial availability |

**Ordering within a group stays exact either way**, because a group never spans slots.
That is the guarantee that survives the choice, and it is the one callers should reach
for.

### Which to use

Default to normal. A single machine handles a few hundred thousand messages a second —
a hundred thousand a second is eight and a half billion messages a day on one queue.
Reach for `distributed` when one queue genuinely outgrows a machine, and accept that
priority and FIFO become approximate for it.

This is the trade every system makes. A database table lives on one node until it does
not fit, and then you shard it and lose cross-shard transactions. Nobody shards every
table by default.

---

## 5. The API

Five operations over HTTP, plus three that are not required but cost very little.

| Operation | Who calls it | What it does |
|---|---|---|
| **Create Queue** | Admin | Makes a named queue with its delivery settings |
| **Enqueue** | Producer | Submits a message, returns a message ID |
| **Dequeue** | Consumer | Returns the most urgent available message plus a receipt |
| **Acknowledge** | Consumer | Confirms the work is done; the message is deleted |
| **Get Metrics** | Monitoring | Per-queue numbers |
| Negative Acknowledge | Consumer | Hands a message back without waiting for its lease |
| Delete Queue | Admin | Marks a queue for deletion |
| Cluster | Admin | Which nodes are live and where each queue sits |

### Two identifiers, not one

Enqueue returns a **message ID**. Dequeue returns a **receipt**. Different jobs,
different lifetimes.

| | Message ID | Receipt |
|---|---|---|
| Lasts | Forever | One delivery attempt |
| Contains location | No | Yes |
| Used for | Producer-side logging and tracking | Routing the acknowledgment |

**Only short-lived identifiers may carry location.** If the message ID said which slot
held the message, every ID a producer had saved would be wrong the moment the queue
moved. A receipt expires with its delivery attempt, so it can never outlive what it
describes.

The specification asks for acknowledgment "by ID". A receipt is that identifier, and
it has to be, for two reasons.

**A bare message ID cannot be routed.** Finding which slot holds a message would need
an index over every message in the cluster, updated on every submission and
acknowledgment — a second distributed system to keep consistent.

**A bare message ID cannot tell two deliveries apart.** If a worker stalls past its
lease and the message goes to somebody else, a late acknowledgment from the first
worker would delete a message the second is still processing. The receipt carries a
lease generation that makes this detectable, and it is needed even on one machine, so
this is correctness rather than routing convenience.

### Requests and responses

```
POST /v1/queues
{ "name": "orders", "visibilityTimeout": "30s", "maxRetries": 3,
  "defaultTTL": "1h", "starvationThreshold": "60s", "starvationReserve": 0.2,
  "deadLetterQueue": "orders-dlq", "maxDepth": 1000000,
  "distributed": false }

201  { "name": "orders", "created": true, "ownerNode": "node-2" }
200  { "name": "orders", "created": false }    exists already, same settings
409  exists already with different settings
```

```
POST /v1/queues/{name}/messages
{ "payload": "...", "payloadEncoding": "text", "priority": 75,
  "groupID": "user-123", "ttl": "30m", "deliverAfter": "10s",
  "deduplicationID": "order-9981" }

201  { "messageID": "8f14e45fceea167a" }
400  priority out of range, expiry already past, payload too large
404  no such queue, or it is being deleted
503  queue at maxDepth, log cannot accept writes, or its owner is down
```

`priority` takes a number from 0 to 100, or `HIGH`, `MEDIUM`, `LOW`, mapping to 75, 50
and 25. The names exist so a caller who needs three levels never thinks about the
scale.

`payloadEncoding` is `text` by default, or `base64` for a body that is not text. The
caller says which rather than the server working it out, because the two overlap: a
message like `m000` is ordinary text and also valid base64, and sniffing would turn it
into three bytes of noise. Responses always carry the payload as base64, since a
payload may be bytes.

```
POST /v1/queues/{name}/messages/dequeue
{ "maxMessages": 1, "waitTime": "20s" }

200  { "messages": [ { "messageID": "...", "payload": "...", "priority": 75,
                       "groupID": "user-123", "attempts": 1,
                       "enqueuedAt": "...", "receipt": "..." } ] }
204  nothing available
404  no such queue
```

```
POST /v1/queues/{name}/messages/ack     { "receipt": "..." }

200  acknowledged, and also returned if it was already acknowledged
409  the lease expired, or the receipt predates a restart or a move
400  malformed receipt
```

```
POST   /v1/queues/{name}/messages/nack  { "receipt": "...", "delay": "5s" }  → 200
DELETE /v1/queues/{name}                                                     → 202
GET    /v1/cluster                       members, and where each queue sits
GET    /v1/metrics                       every queue in the caller's org, JSON
GET    /metrics                          the same numbers in Prometheus text format
```

```
GET /v1/queues/{name}/stats

200 {
  "messages":                128401,
  "inFlight":                   892,
  "delayed":                     40,
  "byPriority": { "high": 400, "medium": 120000, "low": 8001 },
  "oldestMessageAgeSeconds":     43,
  "deadLettered":                12,
  "enqueueRate":             1240.5,
  "ackRate":                 1238.1,
  "ownerNode":          "node-2",
  "asOf": "2026-09-02T14:22:10.412Z",
  "exact": true
}
```

**For a normal queue these numbers are exact.** The queue lives on one machine, its
counters are kept under the slot locks as messages move, and one request reads them.

For a distributed queue the gateway asks every machine holding a slot and adds the
answers together. Those answers describe slightly different instants, so `exact` comes
back false. If a machine is unreachable the response says so rather than quietly
under-reporting:

```
200 { "messages": 124800, "exact": false, "unavailableSlots": 3 }
```

### Identifying the caller

Every request carries a credential, and the gateway resolves it to an org before
anything else happens. Queue lookup is by org and name together.

```
Authorization: Bearer <credential>   →  org_7f2a
POST /v1/queues/orders/messages      →  queue (org_7f2a, "orders")
```

The org is not in the path and not in the body. A caller cannot reach another org's
queue by guessing a name, because the name is only half the identifier.

### What the API leaves out

No partition count, no shard hint, no node names. `distributed` is a boolean, so there
is nothing to size and no way to get it wrong beyond a flag that can be changed later.

SQS works this way. Kafka does not: you pick a partition count at creation, cannot
change it without reprocessing, and have to guess before seeing any traffic.

---

## 6. Architecture

Two tiers, plus Postgres and etcd.

```
     producers                                        consumers
         │                                                │
         └────────────────────┬───────────────────────────┘
                              │  HTTP
                     ┌────────▼─────────┐
                     │  load balancer   │   one address, forever
                     └────────┬─────────┘
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌──────────┐    ┌──────────┐    ┌──────────┐
        │ gateway  │    │ gateway  │    │ gateway  │    stores nothing
        └────┬─────┘    └────┬─────┘    └────┬─────┘    routes, validates,
             └───────────────┼───────────────┘          collects metrics
                             │  gRPC, streaming
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
   ┌─────────┐          ┌─────────┐          ┌─────────┐
   │  node   │          │  node   │          │  node   │   slots, leases,
   │         │          │         │          │         │   write-ahead logs
   └─────────┘          └─────────┘          └─────────┘
        └────────────────────┼────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
     ┌─────────────────┐          ┌──────────────────┐
     │    Postgres     │          │       etcd       │
     │  queue configs  │          │   membership     │
     │  where each     │          │   who is alive   │
     │  queue lives    │          │                  │
     └─────────────────┘          └──────────────────┘
```

Neither store is on the path a message takes.

### Gateways

Gateways store nothing. They accept HTTP, resolve the credential to an org, validate
the request, work out which node holds the queue, and forward over gRPC.

Resolving the credential first matters: everything after it is scoped to one org, so a
request can only reach queues that org owns. There is no later check to forget.

Two rules limit what a gateway may do.

**A gateway computes; it never chooses.** Which slot a message goes to is a hash, and
which node owns a queue is read from Postgres. Since nothing about routing is a
judgement call, we run as many gateways as we like without them talking to each other.

**A gateway never holds a write.** It does not tell a producer a message was accepted
until the node has written it. A gateway that buffered messages would be telling
producers their work was safe while it sat in a tier that stores nothing.

Gateways also collect metrics (§17) and hold long-polling consumers (§9).

### Nodes

Nodes hold the data. Each owns some slots, keeps their messages in memory, tracks
their leases, writes their logs, and pushes work notifications to gateways.

A node needs exactly one piece of configuration: the etcd address. It takes its
identity from its hostname and registers itself, which is what lets a container
orchestrator add nodes with no configuration change anywhere.

### Postgres — queue configs and where queues live

```sql
CREATE TABLE queues (
    org_id      TEXT NOT NULL,
    name        TEXT NOT NULL,
    settings    JSONB NOT NULL,
    distributed BOOLEAN NOT NULL DEFAULT false,
    state       TEXT NOT NULL DEFAULT 'active',   -- active | migrating | deleting
    owner_node  TEXT,          -- normal queues only
    generation  BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (org_id, name)
);

CREATE TABLE slot_placement (                      -- distributed queues only
    org_id     TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    slot       SMALLINT NOT NULL,
    owner_node TEXT NOT NULL,
    generation BIGINT NOT NULL,
    state      TEXT NOT NULL DEFAULT 'active',
    PRIMARY KEY (org_id, queue_name, slot)
);
```

**The primary key is the pair, not the name**, which is what lets tenants pick their
own names. It also does the concurrency work: two gateways creating `orders` for the
same org cannot both succeed. One wins, the other reads back the row and returns 200
if the settings match or 409 if they do not.

**Two tables because there are two placement units.** A normal queue is placed as a
whole, so its owner is one column on its row. A distributed queue is placed per slot,
so it gets sixty-four rows. Storing sixteen identical rows for a normal queue would
be recording the same fact sixteen times.

Section 7 explains why placement is stored at all rather than computed.

### etcd — membership only

```
/ryuk/members/{nodeID}  →  {"addr": "node-3:9090"}     lease TTL 10s, kept alive
```

Stop renewing — crash, kill, scale down — and the key disappears. Gateways and nodes
watch the prefix, so the live member list is current everywhere within milliseconds.

etcd is used for this and nothing else. It gives two things a database does not:
**watches**, so a membership change is pushed rather than polled, and **keys that
expire on their own**, so liveness needs no heartbeat table and no reaper.

### Caching

Both tiers cache queue configs and placement in an in-memory LRU.

**A cache miss means go and look, not "no such queue."** Someone creates a queue and
submits to it immediately; treating a miss as a 404 would fail. **Negative results are
cached for a second or two**, so a loop with a typo'd name does not hit Postgres every
time, without reintroducing the first problem.

Entries refresh on a thirty-second timer, and are evicted immediately when a node
replies that a queue has moved (§13).

### Why two tiers

**Restarting a node causes redeliveries.** Every message it had in flight goes back in
the queue and is delivered again. That is correct, but it means every deploy causes a
burst of duplicate work. Gateways hold no messages, so shipping an API change never
disturbs work in progress — and the API tier changes far more often than the storage
tier.

**The tiers grow for different reasons.** Gateway load tracks open connections, driven
by idle workers waiting. Node load tracks data volume and message rate.

**Nodes get no public listener.** The boundary between tiers becomes a real security
boundary rather than a convention.

The costs are one extra network hop, well under a millisecond in-region against a
100 ms budget, and one extra failure case (§12).

---

## 7. Placement

### Choosing an owner

**Placement is a pure function of the member list, so nobody decides it.**

```go
func OwnerFor(key string, members []Member) Member {
    best, bestScore := Member{}, uint64(0)
    for _, m := range members {
        if s := hash(key + "\x00" + m.ID); s > bestScore {
            best, bestScore = m, s
        }
    }
    return best
}
```

```
normal queue        key = "org_7f2a/orders"          → one owner for all 16 slots
distributed queue   key = "org_7f2a/events/slot-7"   → an owner per slot
```

This is **rendezvous hashing**. Every gateway and every node computes the same answer
from the same member list, so there is no coordinator, no election and no map to keep
consistent. Adding a machine moves only the keys that machine now wins — about
`1/(n+1)` of them — where a plain modulo would move nearly everything.

Queues spread uniformly because the hash does.

### Why placement is also stored

If placement is a pure function, why write it to Postgres at all?

Because **the hash is a function of who is alive, and ownership must follow the data.**

```
node-2 dies with 40,000 messages of org_7f2a/orders on its disk

  the hash now says      → node-4 owns it
  reality says           → the messages are on node-2
```

Recomputing on every request would hand the queue to node-4, which would create an
empty queue and start accepting messages while the real ones sat on node-2's disk.
Ordering would split across two machines and the depth metric would read zero.

So the hash decides where something goes **when it moves**, and the stored record says
where it **is**. They agree except while a migration is in flight, which is exactly
what makes rebalancing observable rather than magic.

### Generations

Every change of ownership increments a generation counter on the placement record.
Message sequence numbers are prefixed with it:

```
Seq = (generation << 40) | counter
```

Sequence numbers are what cross-slot FIFO ordering compares, so two owners handing out
overlapping values would corrupt it. The prefix makes them monotonic across owners
with no coordination, and it means messages from an older generation sort correctly
when a returning node merges its data back (§12).

---

## 8. Submitting a Message

```
producer → gateway → node that owns the slot → its log
```

1. The gateway resolves the credential to an org and checks the request against its
   cached config: the queue exists in that org and is not being deleted, the priority
   is in range, the expiry is not already past, and the payload is not too large.
   Rejecting an oversized payload here keeps it off the internal network. Depth is not
   checked here, because the gateway does not know how deep a queue is.

2. The gateway works out the slot. With a group key it is a hash of the org, the queue
   name and the key together, so **every message in a group goes to the same slot** —
   which is what makes ordering possible at all. Hashing the org and queue name in as
   well stops two tenants who happen to use the same group key from sharing a slot.
   Without a group key the slot is picked in turn, across a number of slots that grows
   with queue depth, so a small queue stays in one slot instead of scattering.

3. The gateway looks up the owner — one field for a normal queue, the slot's row for a
   distributed one — and forwards.

4. The node assigns a message ID and a sequence number, and records the submission and
   expiry times. **The node stamps every time value** and they travel in the record, so
   replaying the log produces the same state whatever the clock says later.

5. The node appends the record to the slot's write-ahead log and flushes it according
   to the queue's durability setting.

6. Only then does it add the message to the in-memory structures workers read.

7. The message ID goes back to the producer.

The gateway's check in step 1 rejects obvious mistakes early, so a loop with a typo'd
name never reaches the nodes. It reads a cache, so the node checks again: the queue
must exist there too and must not be at its depth limit, which only the node knows.
Where they disagree the node wins.

**The order of steps 5 and 6 is the durability guarantee.** A worker can only be handed
a message that is already in the log. Reverse them and a crash erases a message we
already gave out.

### Delayed and duplicate submissions

A message can specify a time to become available, in which case the node holds it in a
timer. A message can carry a duplicate-detection key, in which case resubmitting
within a few minutes returns the original message ID instead of creating a second copy
— which turns a producer's retry after a network timeout into a harmless no-op.

---

## 9. Getting a Message

### Choosing within a node

Each slot keeps its available messages as first-in-first-out lists, one per priority
level. Those lists hold **groups, not individual messages**.

Holding groups keeps the choice fast. If each list held messages directly, we would
have to skip any message whose group already had one in flight, and a single group
with a large backlog would slow every request as it scanned past. Pointing at groups
means we never scan.

```
take the next group from the most urgent list that has anything in it
  → take that group's oldest message
  → lock the group so its next message waits
```

Finding the most urgent non-empty list across 101 levels cannot be a loop. Each slot
keeps a bitmap with one bit per level, and finding the highest set bit is a single
machine instruction — so the number of priority levels does not affect how long a
request takes. Operating system schedulers use the same trick.

### Choosing the node

**Normal queue:** nothing to choose. One hash, one lookup, one hop. The node compares
all 64 of its slots and returns the genuinely most urgent, oldest message.

**Distributed queue:** the gateway must pick among the machines holding slots, and a
bad pick means a worker gets low-priority work while urgent work sits elsewhere. Each
node pushes a small summary on the stream the gateway already holds — the most urgent
priority it currently has for each distributed queue, sent as a delta when it changes.
The gateway routes to the highest claimant, and falls through to the next if that node
comes back empty.

A stale summary costs one suboptimal pick. Asking every machine on every request would
put a round trip on the hot path and generate traffic growing with the square of the
cluster.

### Leasing

Handing a message to a worker does not delete it. It **moves** the message from the
available list into the in-flight list with a deadline and a lease generation.

Because the message is no longer in the available list, no other worker can see it.
There is no hidden flag for selection to check — a flag would make selection slower as
more messages went out, while moving keeps it constant.

The receipt carries the slot, the message identity, the lease generation and the
node's incarnation number.

### Waiting for work

A worker can hold a request open for up to twenty seconds. Without this, idle workers
poll in a loop and waste capacity in proportion to fleet size.

**The obvious implementation is wrong.** If the gateway forwarded a waiting dequeue to
every machine that might get the message, two could answer, and the second message
would be leased with nobody to process it — invisible until its lease expired. Under
load that would happen constantly.

So notification and dequeue are separate:

```
consumer ──poll──▶ gateway                            node
                   │  1. try now, one dequeue           │
                   │───────────────────────────────────▶│  nothing available
                   │  2. subscribe, park the consumer   │
                   │───────────────────────────────────▶│
                   │  3. ◀──── "this queue has work" ───│  a message arrives
                   │  4. one dequeue ──────────────────▶│
                   │     ◀──── message ─────────────────│
                   ▼
              respond
```

**Only ever one dequeue is issued**, so nothing is leased speculatively.

The connection cost is what makes this work. A gateway keeps **one stream per node**,
not one per queue and certainly not one per consumer. Ten thousand parked consumers
are ten thousand map entries and zero extra sockets.

If several gateways wait on the same queue, the node notifies only as many as it has
messages for, round-robin. A gateway that loses the race keeps its consumer parked.

Step 1 matters: a busy queue never reaches step 2, so the extra round trip exists only
on idle queues where nobody notices it.

This is also why the protocol streams. When a gateway dies its streams break, and the
node drops its subscriptions and releases any leases it held rather than letting those
messages wait out a full visibility timeout.

---

## 10. Concurrency

Producers and consumers run at once with no coordination, so every structure a request
touches must be safe under concurrent access. This is the part of the design most
likely to be wrong in a way ordinary tests do not catch.

### One lock per slot

The slot is the unit of locking. Everything inside it is covered by a single lock: the
per-priority lists, the group lists, the set of locked groups, the in-flight table,
the lease timers and the counters.

A queue has one lock per slot, so sixteen or sixty-four independent locks depending
on its type. Two requests collide only when they touch the same slot.

### Why not something finer

Handing out a message changes five things at once: it takes a group off a priority
list, takes a message off that group's list, marks the group locked, adds an in-flight
entry, and pushes a lease timer. Those must happen together, or another worker sees a
half-finished state — a group marked locked with nothing in flight, or a message that
appears to be in two places.

Splitting the lock would mean acquiring five locks in a fixed order on every request.
That is slower than one uncontended lock, and the ordering rules are the kind of thing
that works until somebody adds a sixth structure.

### Rules that keep it deadlock-free

- **Never hold two slot locks at once.** Nothing needs a transaction spanning slots,
  so the lock graph has no cycles and deadlock is impossible.
- **Never do I/O while holding a slot lock.** Log writes happen before the lock is
  taken, never during.
- **The lookup that finds a slot is held only for the lookup.**

### Group locks are bookkeeping, not locks

A group lock is an entry in a set, added when a message from that group goes out and
removed on acknowledgment or lease expiry. It is read and written only under the
slot's lock, so it needs no synchronisation of its own.

The distinction matters. A real lock held by a crashed worker would need recovery
machinery. Group state is derived from the in-flight table, so a lease expiring
releases the group automatically with nothing separate to get wrong.

### Background work

Expiring leases, dropping expired messages, releasing delayed messages and writing
snapshots all run on timers. Each takes one slot's lock, does its work, and releases it
before moving on. A sweep never holds more than one lock and never blocks the whole
queue.

All of them can also be called directly rather than only on a timer, which is what
makes their behaviour testable without waiting for real time to pass.

---

## 11. Durability

### One log per slot

Every slot has its own write-ahead log. Not one per machine, and not one for the
cluster.

The reason is that slots move. A machine keeping one log for all its slots would have
to pick a slot's records out of a mixed stream to move it. A log per slot means a
slot's log is its whole history, which is what makes the handoff in §13 a matter of
copying bytes.

### Write before visible

```
1. Build the record
2. Append it to the slot's log
3. Flush according to the durability setting
4. Only then apply it in memory
5. Answer the producer
```

Step 4 after step 3 is the entire guarantee. **A worker can only be handed a message
that is already in the log.**

The append happens outside the slot's lock: build and write first, then take the lock
and apply, so disk latency stays off the critical section.

### What gets written

| Record | Flushed before we answer? |
|---|---|
| Message submitted | **Yes** — we never accept what is not written |
| Moved to dead-letter | **Yes** |
| Delivery attempt counted | No |
| Acknowledged | No |
| Expired | No |

The last three are deliberate. Flushing on every delivery would double the cost of the
most common operation, and the worst case after a crash is a message getting one extra
retry before we give up on it. Flushing every acknowledgment would add latency to
prevent a redelivery, which at-least-once already permits.

**Lease deadlines are never written.** Every lease is void after a crash anyway. The
attempt count is the one piece that must survive: without it, a message that always
fails would retry forever and never reach the dead-letter queue.

### How hard we flush

| Setting | Survives | Cost |
|---|---|---|
| `always` | Process crash and machine power loss | ~1 ms per submission |
| `interval` (default, 100 ms) | Process crash fully; up to 100 ms lost on power loss | Amortised to nothing |
| `never` | Process crash only | Nothing |

`interval` is the default because a *process* crash loses nothing either way — the data
is already in the operating system's page cache and survives the process dying. Only
power loss or losing the machine opens the window. Group commit batches concurrent
writers into one flush.

### Recovery

A node coming back reads each slot's most recent snapshot and the records after it,
verifying a checksum on each and stopping at the first that fails. A half-written
record at the end of a log is what a crash looks like, not corruption, and nothing
after it can be valid.

After replay:

- Every in-flight message goes back to available. Leases do not survive a restart.
- The slot's **incarnation number** is incremented and written to the log.

That second point matters more than it looks. Lease generations restart from zero after
a replay, so a receipt issued before the crash could match a generation issued after
it, and an acknowledgment for a long-dead delivery would delete a message another
worker is processing. Every receipt carries the incarnation, and one from an older
incarnation is rejected outright.

### What this survives

| Failure | Survives |
|---|---|
| Process panics, is killed, or is redeployed | **Yes, completely** |
| Machine reboots | **Yes** |
| Disk fills up | **Yes** — submissions rejected, nothing lost |
| Log damaged at the end | **Yes** — truncated at the checksum |
| Machine and its disk destroyed | **No** |

The last row is the honest limit. Messages live in one machine's memory and one
machine's log, so losing that machine permanently loses its queues.

**Replication is what fixes it, and it is not built** (§24). The log is already the
replication stream, so shipping records to followers and waiting for a quorum before
step 5 is an addition rather than a redesign — but a partly-working replication
protocol is worse than none.

A cheaper production answer is worth knowing: put the log on storage that outlives the
machine. Cloud persistent disks detach from a dead instance and attach to a new one,
turning permanent machine loss into a slow restart, with no consensus protocol.

---

## 12. When Things Break

| What breaks | What happens | Data lost |
|---|---|---|
| A node's process crashes | Restarts, replays, resumes | None, or the flush window on power loss |
| A node is destroyed | Its queues are gone | All of them |
| A gateway dies | The load balancer stops sending to it | None |
| Every gateway dies | Nodes are fine; service returns when one comes back | None |
| Postgres is down | Traffic continues from cache; no creates, no moves | None |
| etcd is down | Traffic continues; no joins, no rebalancing | None |
| A disk fills up | Submissions rejected; delivery and acknowledgment continue | None |
| A log is damaged at the end | Truncated at the bad record | The torn tail only |
| A worker crashes mid-job | Its lease expires and the message is redelivered | None |
| A worker stalls past its lease | Redelivered; the late acknowledgment is rejected | None |

### A node goes down — what each queue type sees

```
node-2 dies. It held:
   org_7f2a/orders     a normal queue, all 16 slots
   org_7f2a/events     slots 3, 7, 10 and 13 of a distributed queue
```

| | `orders` (normal) | `events` (distributed) |
|---|---|---|
| Immediately | **100% unavailable** | slots 3, 7, 10, 13 unavailable — **60 of 64 keep serving** |
| Enqueue | 503 | 503 only for group keys hashing to those slots |
| Dequeue | 503 | Served from the surviving 12 slots automatically |
| Data | Safe in node-2's log | Safe in node-2's log |

**Partial availability is the second thing the `distributed` flag buys**, after
throughput. It is often the more valuable of the two.

### Nothing is reassigned automatically

Ten seconds later node-2's etcd lease expires and every gateway sees the member list
change. Rendezvous hashing would now say node-4 owns `orders`.

**We do not move it.** The gateway routes by the stored placement record, which still
says node-2.

```
If we reassigned:
  → node-4 creates an empty queue
  → new messages go there
  → node-2 still holds the old ones on disk
  → group ordering splits across two machines
  → depth reads zero while 40,000 messages sit on a dead node
```

Without replication the data exists in one place, so **the assignment must follow the
data, not the hash**.

### Why we block rather than fail over

Total unavailability is harsh, and the obvious alternative is to let a new owner take
over empty in about ten seconds. Ryuk does not, and the option is not offered at
creation, because failing over costs ordering permanently:

```
t=0    node-2 dies with m1, m2 of group user-123 unprocessed
t=10s  node-4 takes over, empty
t=20s  producer sends m3 → node-4
t=25s  a consumer processes m3            ← m3 delivered before m1
t=90s  node-2 returns and ships m1, m2 to node-4
```

m3 went first and there is no undoing it.

| | Preserved | Broken |
|---|---|---|
| At-least-once | ✓ nothing is lost | |
| Durability | ✓ the log survives | |
| Priority ordering | ✓ within each owner | |
| **Group ordering** | | ✗ across the failure window |
| **FIFO within a priority** | | ✗ across the failure window |
| Depth and age metrics | | ✗ understated while stranded |

Blocking is the behaviour because failing over without the data does not preserve the
queue's contract — it creates a different queue that happens to share a name. A 503 is
honest; a silently reordered queue with a depth of zero is not.

Making this a per-queue choice is listed under what we have not built. It would be
worth offering once replication exists, because a replica has the data and a failover
to it keeps ordering. Without replication the choice is between an honest error and a
broken queue, which is not much of a choice.

**For a distributed queue this matters far less**, because partial availability already
exists. Losing 4 slots of 64 is not the same problem as losing a whole queue.

### When the node comes back

```
1. Replay every log, void leases, bump incarnations
2. Register in etcd
3. For each queue it holds data for, read the placement record:
      owner == me  →  serve it
      owner != me  →  ship the data to the current owner, then drop it locally
```

Step 3's second branch is the merge, and it is **the same migration protocol as
rebalancing** (§13) — only the trigger differs. One mechanism handles a node joining, a
node leaving gracefully, and a node returning to find it no longer owns something.

Merged messages carry sequence numbers from an older generation, so they sort **before**
anything the new owner accepted and are inserted in the right place rather than
appended. That repairs ordering for messages not yet delivered. It cannot repair what
already went out during the outage, which is why `block` is the default.

**A consumer holding a receipt from before the crash gets 409 on acknowledgment** — the
incarnation no longer matches. Its message was already redelivered.

### Clocks

Almost nothing depends on machines agreeing about the time.

**Lease deadlines never leave the machine that set them.** Computed and checked by one
node, never written down, voided on restart. They use elapsed monotonic time, so a
clock adjustment cannot cause a burst of premature redelivery.

**Expiry times are stamped once, by the node that accepted the message**, and stored in
the record. Replay reads them rather than recomputing, so a log replays to the same
state whatever the clock says at the time. That is the invariant that makes replay
deterministic, and the one a later change is most likely to break silently.

**Ordering never reads a clock at all.** Position in a list and a sequence counter
decide it.

---

## 13. Scaling and Rebalancing

Adding a machine must take real work off the existing ones, not merely receive whatever
arrives next.

### A node joins

```
1. The container starts with one setting: the etcd address
2. It writes /ryuk/members/{hostname} with a 10s lease and keeps it alive
3. Every gateway and node sees the member list change within milliseconds
4. Nothing happens for 15 seconds        ← the stability window
```

The window exists because a container in a crash loop would otherwise cause continuous
data movement, which hurts far more than the imbalance it corrects.

### Each node works out what to give up, alone

No coordinator, no election, no messages between nodes:

```go
for _, s := range n.owned() {              // queues and slots this node holds
    want := OwnerFor(s.Key(), members)
    if want.ID != n.id {
        n.migrations <- migration{unit: s, to: want}    // at most 2 at a time
    }
}
```

Every node runs this simultaneously and they agree, because `OwnerFor` is a pure
function and the member list is identical everywhere. Going from 3 machines to 5 moves
roughly 2/5 of each node's work.

### The handoff

```
   losing node                                gaining node
   ───────────                                ────────────
1. mark FROZEN
   enqueue and dequeue → ErrMoved{target}
2. void in-flight leases
   (those messages return to available)
3. snapshot every message with its   ─────▶  4. create the queue or slot,
   attempts, priority, group, seq,            replay the messages into its
   enqueuedAt, expiresAt                      engine and its own log
5.         ◀────── confirm ────────────────
6. bump generation, update the record
7. drop the local copy and its log           8. start serving
```

The payload is just a list of messages, streamed.

**Leases are voided rather than transferred.** A deadline only means something against
the clock that set it, so nothing time-based crosses a machine boundary.

**Freezing both directions** keeps it simple. A message enqueued at the target before
the snapshot arrived would sit in front of snapshot messages in its group and break
ordering. Callers see a retryable error for the length of one transfer over a local
network. Two-pass migration — bulk, then a locked delta — removes even that and is a
later optimisation.

### Gateways follow in one retry

```
enqueue → node-2 → ErrMoved{node-4}
  → evict this queue from the cache
  → re-read the placement record
  → retry against node-4 → 201
```

Producers never see the migration.

### Not thrashing

| Guard | Why |
|---|---|
| Membership stable 15s before any migration | A restart loop would move data continuously |
| At most 2 concurrent migrations per node | Adding a machine must not saturate the network |
| A just-received unit is pinned for 60s | Stops it bouncing between nodes with slightly different member views |
| Only the current owner initiates | Single writer per record, so no conflict |

### What it looks like

```bash
docker compose up --scale node=3      # 30 queues → ~10 per node
docker compose up --scale node=6      # nodes register, ~15 queues migrate,
                                      # settles at ~5 per node in about 20 seconds
```

| Phase | Time |
|---|---|
| Registration and propagation | under 1s |
| Stability window | 15s |
| Migrations, 2 concurrent | 1–2s |

`/v1/cluster` returns the member list and where each queue sits, so the redistribution
is visible while it happens.

### What scaling can and cannot fix

| Situation | Does adding machines help? |
|---|---|
| Many queues, one machine overloaded | **Yes** — whole queues migrate away |
| One deep distributed queue | **Yes** — its slots spread wider |
| One deep **normal** queue | **No** — it migrates as a unit and is still on one machine. Set `distributed` |
| One very hot **group** | **Never** — a group is one slot by definition. Spreading it would break the ordering guarantee that made groups worth having. Use a finer group key |
| A machine that is **down** | **No** — its data is only there, so nothing is reassigned |

---

## 14. Delivery and Ordering

### Delivery

**At least once.** A message is delivered until acknowledged. It can arrive more than
once: when a worker processes it but does not acknowledge in time, when the node
holding it restarts while the message is out, or when a queue moves.

Exactly-once would require the worker's own side effects to be part of the same
transaction as the acknowledgment, and we have no way to enforce that.

### Ordering

| Where | Normal queue | Distributed queue |
|---|---|---|
| Within a group | **Strict, always** | **Strict, always** |
| Between priorities | **Exact** | Approximate |
| FIFO within one priority | **Exact** | Approximate |

**A normal queue gives exact FIFO within a priority.** All 16 slots are on one machine,
so when several hold work at the same priority the node compares their oldest messages
by sequence number and takes the earliest. Nothing crosses a network.

**A distributed queue gives that up.** Comparing sequence numbers across machines would
mean asking every one on every request. Instead each machine reports the most urgent
priority it holds and gateways route on that, so ordering stays exact within a slot
while two equal-priority messages on different machines can come out in either order.

Ordering inside a group is exact in both cases, because a group never spans slots.

The sequence number is a counter, not a clock, so none of this depends on machines
agreeing about the time.

### Why group ordering is the guarantee to reach for

Even where FIFO is exact, be clear what it means. With ten workers pulling at once, the
order messages go *out* is decided by which worker asks first, and the order work
*finishes* by how long each job takes. A queue can promise the order it hands work out;
it cannot promise the order work completes, and the second is usually what callers care
about.

Two messages that must be ordered relative to each other should share a group key. Then
the promise holds end to end, at any cluster size, distributed or not.

### One group that is too busy

All of a group's messages live in one slot on one machine. A group getting far more
traffic than the rest is limited to that machine, and this cannot be fixed without
giving up the guarantee that made groups worth having. Per-slot depth metrics make it
visible; the fix is a more specific group key.

---

## 15. Keeping Low Priority Alive

### The problem

Always serving the most urgent work means less urgent work may never run. With a
hundred levels this is worse than with three: work at priority 40 waits behind anything
at 41 or above. Under steady load, low-priority messages sit until they expire and get
deleted.

**A priority queue that quietly destroys low-priority work is broken, not badly tuned.**
We treat this as a correctness requirement.

### Reserve a share of capacity

```
   for each delivery:

       if this delivery falls in the reserved share
          and something has been waiting past the threshold:
              serve the thing that has waited longest

       otherwise:
              serve the most urgent thing
```

**It costs nothing when nothing is stuck.** The threshold is a gate. If nothing has
waited too long, every delivery serves the most urgent work.

**It cannot flip the problem around.** The obvious version has a bug: departing from
priority whenever anything has waited too long lets a large old backlog take every
delivery, and urgent work becomes the thing that never runs. Capping the departure at a
fixed share means old work drains steadily while urgent work keeps the rest.

**It does not get slower with more priority levels.** The check is constant time, from a
rotating starting point, stopping after a fixed number of checks.

### What this guarantees

> Nothing waits longer than the threshold plus the time to drain the work ahead of it,
> at the reserved share of throughput.

In practice: **set the threshold below the shortest expiry in use and messages can
never expire from waiting.** Queue creation checks this and rejects settings that break
it, because that combination turns a queue into something that silently deletes work.

### Where the idea comes from

| System | How it does it |
|---|---|
| Linux deadline I/O scheduler | Reads can only jump the queue so many times before a write goes |
| Linux real-time throttling | Real-time tasks capped at 95% so normal tasks always get 5% |
| Cisco class-based weighted fair queuing | The priority class is capped so it cannot take the whole link |
| Hadoop YARN fair scheduler | Every queue has a guaranteed minimum share |

### What we did not do

**Raising a message's priority as it waits** is nicer in principle but means either
moving messages between levels, which the structure cannot do cheaply, or one timer per
message.

**Weighted random selection** spreads load smoothly but promises nothing definite. An
operator can act on "nothing waits more than a minute"; they cannot act on a
probability.

---

## 16. A Message's Life

```
                          submitted
                              │
                              ▼
                       ┌─────────────┐   release time
                       │   delayed   │   reached
                       └──────┬──────┘
                              │
      expiry sweep ◄──── [ available ] ◄───────────────┐
           │                  │                        │
           ▼              leased                       │  lease expired or
      ┌─────────┐             │                        │  handed back, retries
      │ expired │             ▼                        │  remaining
      └─────────┘      [  in flight  ] ───────────────┘
                         │           │
              acknowledged           out of retries
                         │           │
                         ▼           ▼
                   ┌──────────┐  ┌─────────────┐
                   │ deleted  │  │ dead-letter │
                   └──────────┘  └─────────────┘
```

A message is either available or in flight, never both. Handing it out moves it.
Acknowledging deletes it. An expired lease moves it back.

### Two rules

**A message that expires while a worker has it can still be acknowledged.** Expiry stops
us handing a message out; it does not mean throwing away work someone has already done.
If the worker's lease expires instead, we drop the message then.

**A message handed back goes to the end of its priority list, not its old place.**
Otherwise a message that keeps failing blocks the front of that list every cycle. Its
place *inside its group* does not change, so group ordering is unaffected.

### The dead-letter queue

A message that runs out of retries moves to a dead-letter queue, which is an ordinary
queue using the same code. It keeps its original payload, its retry count and its
original submission time, so its age shows how long the problem has existed rather than
when we gave up.

---

## 17. Metrics

**The gateway collects from the nodes. Nothing scrapes a node directly.**

Nodes have no public metrics endpoint. They answer an internal request instead.

### How the numbers get out

Each gateway runs a collector. Every ten seconds it asks each node, in one request, for
the counters of every queue that node holds, and caches the answers. All three
endpoints serve from that cache:

```
GET /v1/queues/{name}/stats      one queue, JSON
GET /v1/metrics                  every queue in the caller's org, JSON
GET /metrics                     the same numbers in Prometheus text format
```

That is **one request per node per interval, regardless of how many queues exist**.
Fifty machines cost fifty requests every ten seconds whether the cluster holds ten
queues or ten thousand.

Three reasons to collect rather than scrape nodes:

**Nodes have no public listener.** Giving them one for metrics would undo the security
boundary for a monitoring convenience.

**A node holds slots, not queues.** Something must add them up. Doing it in the
monitoring system means the dashboard and the stats endpoint are two aggregation paths
that can disagree, and when they do nobody knows which to believe.

**Scrape timing differs per target.** Summing across machines scraped at different
moments mixes samples up to a full interval apart. Collecting on one schedule removes
that.

### Counting each slot once

Every slot has exactly one owner, so nothing double-counts. Two rules keep it that way
during a handoff:

- A node that stops owning a slot **stops reporting it**, rather than reporting zero. A
  series that disappears is handled as stale; a zero is treated as real.
- The losing node stops before the gaining node starts.

### What is reported

Required, per queue: oldest available message age, ready count by priority bucket,
in-flight count, cumulative enqueued and acknowledged counts, dead-letter count.

Priority is bucketed into three rather than reported per level. A hundred label values
per queue is a monitoring problem, not useful detail.

Throughput is exposed as **counters**, with rates computed over a one-minute window on
the JSON endpoints. Rates belong in the query, because computing them at collection
time breaks whenever the interval changes.

Beyond the requirements: starvation escapes, expiry and redelivery counts, delayed
count, per-slot depth, sweep lag, log append latency, and which node owns each queue.

**Starvation escapes is the number worth alerting on.** If it is climbing, workers
cannot keep up with urgent work and low-priority work is moving only because of the
safety net in §15. None of the required metrics show that.

### Where the numbers come from

Every counter is maintained under a slot's lock as messages move, never computed by
scanning, so reporting does not get slower as queues get deeper. Nothing is written to
the log: after a crash a node replays, rebuilds, and the counters fall out of the
rebuilt state.

---

## 18. Latency Budget

The target is a 95th percentile under 100 ms, in-region.

| Step | Typical |
|---|---|
| Client to gateway | 1–5 ms |
| Gateway checks, from cache | under 0.1 ms |
| Gateway to node | 0.2–1 ms |
| Slot lock and structure work | under 0.05 ms |
| Log append | around 0.05 ms |
| Flush, if set to `always` | 0.5–2 ms |
| **Submitting, total** | **about 2–8 ms** |

Receiving is cheaper still, because the attempt record is not flushed before we answer.

Almost all of the budget is network time. The parts we control are constant time and
well under a millisecond together, leaving room for a cross-region hop or a slow disk.
Adding replication later would put one more round trip on submission, one to three
milliseconds in-region, which the budget has room for.

The two things that would ruin this are both avoided by design: scanning a structure
whose size grows with queue depth, and asking every machine on every receive.

---

## 19. How Far It Scales

| What | Grows by | Where it stops |
|---|---|---|
| Cluster throughput | Adding machines | Nothing — no shared bottleneck |
| Number of queues | Adding machines | Postgres capacity |
| Number of orgs | Adding machines | Postgres capacity |
| Open connections | Adding gateways | No practical limit |
| Total messages held | Adding machines | Aggregate memory |
| **One normal queue** | **Does not grow** | **One machine** |
| One distributed queue | Adding machines | 64 slots |
| One group | Does not grow | One slot, by design |

Three rows are capped, each for a reason, and two of them have an answer: a normal
queue that outgrows a machine sets `distributed`; a hot group needs a finer group key.
Everything else is linear in machines.

### What a slot costs

Slots are created only when they first receive a message, so an idle queue costs
nothing. A live slot costs roughly 400 bytes of bookkeeping before any message.

| Deployment | Queues | Live slots | Bookkeeping |
|---|---|---|---|
| 1,000 queues | 1,000 | ~16,000 | 7 MB |
| 1,000 orgs, 10 queues each | 10,000 | ~40,000 | 17 MB |
| 1,000 orgs, 1,000 queues each, mostly small | 1,000,000 | ~1,000,000 | 440 MB |

Two decisions keep that number where it is. The per-priority ready lists are held
sparsely, so a queue using three priorities does not pay for 101. And messages without a
group key fan out in proportion to queue depth rather than spreading over every slot
immediately, so a queue holding a thousand messages uses one slot.

---

## 20. What We Build

| Built | Designed, not built |
|---|---|
| The engine: slots, groups, priority bitmap, starvation reserve | Replication and failover to a replica |
| Full lifecycle: leases, retries, dead-letter, expiry, delayed delivery | Cells — machines an org does not share |
| Stale-acknowledgment rejection by lease generation and incarnation | Cluster-wide quotas per org |
| Write-ahead log, flush policy, replay, crash recovery | Load-aware placement |
| Both queue types, and the `distributed` flag | Two-pass migration with no freeze |
| Rendezvous placement, membership in etcd, configs in Postgres | |
| Rebalancing: freeze, ship, confirm, release | |
| The merge when a node returns with stranded data | Failing over to an empty owner instead of blocking |
| Gateway REST, node gRPC, long polling with notifications | |
| Metric collection and all three endpoints | |
| Orgs as a naming, access and contention boundary | |

### The guarantees, as built

| Guarantee | Normal queue | Distributed queue |
|---|---|---|
| At-least-once delivery | Yes | Yes |
| Order within a group | Yes | Yes |
| Exact FIFO within a priority | **Yes** | Approximate |
| Exact priority ordering | **Yes** | Approximate |
| Exact message counts | **Yes** | Point-in-time sum |
| Survives a process crash | Yes, from the log | Yes |
| Survives losing a machine | No | No, but only its slots are affected |
| One tenant cannot read another's queues | Yes | Yes |
| One tenant cannot slow another by locking | Yes | Yes |

---

## 21. Testing

**Time-dependent behaviour, under a clock the test controls.** Visibility timeouts,
expiry, delayed release and the starvation threshold are tested by moving a clock
forward rather than sleeping. Sleeping makes tests slow and unreliable, and cannot test
a twelve-hour timeout at all.

**Concurrency, with a race detector.** Many producers and consumers at once, with a
tool that reports unsynchronised access directly instead of waiting for a bug to appear
by luck. The detector is the method; a stress test that happens to pass proves little.

Six things must hold after every run:

1. Everything submitted ends in exactly one end state
2. No message is acknowledged twice
3. No message is out with two workers at the same time
4. Within a group, the order out matches the order in
5. Within one priority, messages came out in submission order
6. The run finishes instead of deadlocking

The fourth catches the most real bugs. The fifth is assertable only for a normal queue —
it is precisely the test that relaxes when `distributed` is set.

**Comparison against a deliberately simple version.** Random operation sequences run
against both the real implementation and a naive one with a single lock and no slots.
The two must behave identically from the outside.

**Workers that behave badly.** Consumers that take a message and vanish, acknowledge
twice, acknowledge with an old receipt, or hand messages back repeatedly.

**Recovery.** Submit work, kill the process without warning, restart, and check that
acknowledged messages are gone, unacknowledged ones come back, and a receipt from
before the crash is rejected.

**Cluster behaviour.** Scale from three nodes to six and assert that placement evens
out and nothing is lost. Kill a node and assert a normal queue returns 503 while a
distributed queue keeps serving its surviving slots. Restart it and assert its data
comes back or merges to the current owner.

**Tenant isolation.** Two orgs with identically named queues, identical group keys and
overlapping traffic. Neither may see the other's messages, and a credential for one may
not reach the other's queues.

---

## 22. Decisions

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| **Where a queue lives** | **One machine by default** | Every queue spread across machines | Exact ordering and counts, one hop, far less machinery |
| **Spreading a queue** | **A `distributed` flag, default false** | Always on, or automatic | Only the queue that outgrows a machine pays the ordering cost |
| Placement | Rendezvous hashing over live members | A map written by an elected coordinator | Deterministic, so nobody has to decide or agree |
| Placement is also **stored** | Yes, in Postgres | Recomputed per request | Ownership must follow the data, not the hash |
| On owner loss | Block until the owner returns | Always fail over | Failing over without the data breaks group ordering permanently |
| Slots per queue | 16 | 64, or sized to the cluster | Enough locks for one machine; caps fan-out when distributed |
| Slot ownership | One queue per slot | Slots shared across queues | Shared slots would put two tenants behind one lock |
| Message IDs | No location information | ID encodes the slot | Layout must not live in permanent identifiers |
| Acknowledging | By receipt | By message ID alone | An ID cannot be routed, and cannot tell two deliveries apart |
| Sequence numbers | Generation-prefixed counter | A plain counter | Two owners would otherwise hand out overlapping values |
| Ordering mechanism | Position in a list; a counter across slots | Wall-clock timestamps | A counter is not a clock |
| Starvation | A reserved share of capacity | Aging, weighted random | A definite bound an operator can act on |
| Priority selection | Bitmap over levels | Scan or heap | Same speed at 3 levels or 101 |
| Available structure | Lists of groups | Flat list, skip locked ones | One busy group cannot slow everything down |
| Lock granularity | One lock per slot | Finer locks per structure | Handing out a message changes five things together |
| Group locks | Bookkeeping under the slot lock | A real lock per group | A crashed worker would otherwise need recovery machinery |
| Durability | Write-ahead log per slot | Replication, a shared database | Survives everything but losing the machine; replication is an addition |
| Lease deadlines | Never written down | Written to the log | Voided after a crash anyway |
| Attempt counts | Written down | Discarded | Without them a failing message retries forever |
| Metrics | Gateway collects from nodes | Scraping nodes directly | Nodes have no public listener; two aggregation paths can disagree |
| Queue configs and placement | Postgres | etcd | Structure, backups, history, and it grows with queues |
| Membership | etcd | Postgres | Needs watches and keys that expire on their own |
| Queue identity | Org plus name | Globally unique names | Tenants pick their own names without collisions |
| Where the org comes from | The credential | The URL path | A path can be edited; a credential cannot |
| Deleting a queue | Mark, then remove | Delete the row straight away | The deletion must reach the owner first |
| Waiting for work | Notify, then one dequeue | Fan out the dequeue, take the first answer | Fanning out leases messages nobody is processing |
| Rebalancing | Each node moves its own work | A central rebalancer | Deterministic placement means they already agree |

---

## 23. Prior Art

Meta's **FOQS** does this at around a trillion items a day, and Ryuk ends up looking
similar: a fixed set of shards each owned by one machine, a service managing the
assignments, priority selection in memory, integer priorities, delayed delivery, and
lease-based delivery with acknowledgment and negative acknowledgment.

The most useful thing to notice is that **FOQS does not pick messages out of MySQL**. It
keeps an in-memory index sorted by priority and merges across shards in memory. MySQL is
the durable store underneath, not the queue itself. So FOQS is not an argument for
building a queue on a database — it is in-memory priority structures over durable
storage, which is what Ryuk does, with MySQL doing the job Ryuk's log does.

MySQL was right for them and would be wrong here. Meta runs one of the largest MySQL
fleets anywhere, with replication and shard management already solved, so writing a
replicated log would mean redoing work their infrastructure already does. At their
volume the data also does not fit in memory.

Other lineage: **rendezvous hashing** for placement; group ordering from **SQS FIFO
queues**; the two queue types mirroring **SQS Standard versus FIFO**; the priority
bitmap from operating system schedulers; the reserved share from **Linux real-time
throttling** and network traffic shaping.

---

## 24. Future Work

**Replication.** The one thing standing between this and surviving the loss of a
machine. The log is already the replication stream: the owner ships records to
followers, waits for a quorum before answering the producer, and a follower is promoted
when the owner is gone. What it needs is what was deliberately left out — leader
election, a fencing rule so a partitioned owner stops serving, and promotion on
failure. Two cheaper options are worth weighing first: putting the log on storage that
outlives the machine, which needs no protocol at all, or replicating asynchronously and
accepting a small window of loss.

Its absence has one visible consequence: nothing is reassigned away from a machine that
still holds data, so a machine that is gone takes its queues with it until it returns.

**Automatic promotion to `distributed`.** The system can already see a queue's
throughput and depth. Setting the flag automatically is a small step, but it means
moving live data and changing a queue's ordering guarantee underneath its callers,
which deserves more care than a threshold.

**Load-aware placement.** Rendezvous hashing balances by count, not by load. A
coordinator that could see actual traffic would place a hot queue away from other hot
queues. That is what the elected coordinator is for, and it is the first thing to add
if count-based balance proves insufficient.

**Cells.** Grouping machines so orgs in different cells share nothing physical, which
turns tenant isolation from a limit into a boundary.

**Quotas per org.** Cluster-wide limits on messages, bytes and request rate, enforced by
leased budgets rather than counting: each machine gets a slice of an org's allowance to
spend locally, shrinking as the org nears its limit. Per-queue depth limits are exact
today and cover most of what quotas would.

**Two-pass migration.** Ship the bulk, then a locked delta, so a handoff never freezes
enqueues at all.

**Storage beyond memory.** Keeping only the front of each priority list in memory would
move the backlog ceiling from memory to disk.

**Batched submission.** A real throughput win that brings partial failure with it, which
needs a response shape that makes that unambiguous.