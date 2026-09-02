# Ryuk — A Distributed Priority Queue

**High-Level Design**

---

## 1. What Ryuk Is

Ryuk is a distributed priority queue. Services submit work items with a priority.
Worker fleets pull those items, do the work, and tell Ryuk when they are done. Ryuk
stores the queues, tracks each message from submission to completion, and reports
metrics.

Five requirements shape the design:

- Serve urgent work first, but don't let low-priority work wait forever
- Process related messages in order, while unrelated messages run in parallel
- Never silently lose a message a producer was told we had accepted
- Add and remove machines without reconfiguring anything
- Serve many tenants from one cluster without letting one disturb another

These requirements fight each other. Serving urgent work first means low-priority
work may never run. Processing messages in order means they cannot run in parallel.
Never losing a message costs time on every write. Adding machines freely breaks any
design that stores machine names inside message IDs or queue settings. And serving
many tenants affordably means sharing machines, which is precisely what allows one
tenant to disturb another.

Most of this document is about resolving those conflicts.

---

## 2. The Problem

A queue sits between services that submit work and workers that perform it. The
queue itself is simple. The difficulty is keeping it correct while machines fail,
while workers crash halfway through a job, while the cluster grows and shrinks, and
while hundreds of producers and consumers run at once without coordinating.

Six questions drive the design:

**What does a worker get when it asks for work?** The most urgent item available.
But if urgent work keeps arriving, less urgent work must still run eventually.

**What happens when a worker dies mid-job?** The work goes back in the queue. But
if we put it back too quickly, we duplicate work that a slow worker is still doing.

**What happens when a machine holding messages dies?** Everything it had accepted
must still be there when it comes back, and recovery must not need an operator.

**What happens when we add a machine?** Existing queues keep working, and no client
needs to hear about it.

**What ordering can we promise?** More usefully: what ordering can we promise that
still holds when ten workers pull at once and machines come and go.

**What keeps one tenant off another's back?** Many customers share the cluster. They
must not see each other's queues, collide on each other's names, or slow each other
down simply by being busy.

---

## 3. Concepts

### Message

A payload plus some metadata. The submitter chooses the payload, the priority, an
optional ordering key, and an optional expiry. Ryuk never looks inside a payload.

### Priority

A number from 0 to 100. Higher means more urgent. We define three names on that
scale — LOW is 25, MEDIUM is 50, HIGH is 75 — with room above HIGH for emergencies
and below LOW for background work.

Using a number instead of three fixed levels means adding finer priorities later is
a config change, not a rewrite.

### Group

The ordering key. Messages that share a group are delivered in the order they were
submitted, and only one message from a group is ever in flight at a time.
Messages in different groups run in parallel.

A message submitted without a group becomes a group containing only itself. It can
never be blocked behind anything, and one mechanism handles both cases.

SQS FIFO queues work this way. Kafka does not, and the difference matters. Kafka
assigns a consumer to a partition and keeps it there, so a consumer reading
partition 3 cannot see an urgent message sitting in partition 5. That would break
the main thing we promise: give the worker the most urgent item available. Ryuk
locks a group when it hands out a message and unlocks it on acknowledgment, so no
worker is tied to anything.

### Slot

A partition of one queue, and the thing that actually holds messages. Every queue has
16 slots.

A slot is the unit of three separate things:

| Unit of | Because |
|---|---|
| **Storage** | Messages physically live in it |
| **Locking** | One lock per slot, so concurrent requests to a queue do not serialise |
| **Movement** | A slot moves as a whole; its log is its complete history |

A slot holds the ready lists for each priority, the messages of each group, the
messages currently out with workers, the lease timers, one lock, and one write-ahead
log.

**By default all 16 slots of a queue live on one machine.** Sixteen locks, one
process. A queue marked `distributed` spreads them across machines instead, which
Section 9 covers along with what that costs.

**A slot belongs to exactly one queue, and therefore to exactly one org.** No two
tenants ever share one. Slots are invisible to clients, and their number never
changes.

### Org

A tenant. Every queue belongs to exactly one org, and a queue is identified by the
pair — org plus name. Two orgs can both have a queue called `orders` and they are
unrelated.

An org is a boundary in three ways. It **namespaces** queue names, so tenants pick
their own without coordinating. It **contains access**: a caller can only reach
queues in its own org. And it is a **contention boundary**, because a slot belongs to
one queue and therefore one org, so no hash collision can put two tenants behind the
same lock.

The org is never sent in a request. It comes from the caller's credential, so it
cannot be changed by editing a URL.

### Queue

A named container with its own visibility timeout, retry limit, expiry, and starvation
settings. It belongs to one org, and its name only has to be unique within that org.
Creating one requires no information about machines, slots, or placement groups.

---

## 4. The API

Ryuk exposes five operations over HTTP, plus two more that are not required but
cost very little.

| Operation | Who calls it | What it does |
|---|---|---|
| **Create Queue** | Admin | Makes a named queue with its delivery settings |
| **Enqueue** | Producer | Submits a message, returns a message ID |
| **Dequeue** | Consumer | Returns the most urgent available message plus a receipt |
| **Acknowledge** | Consumer | Confirms the work is done; the message is deleted |
| **Get Metrics** | Monitoring | Per-queue numbers |
| Negative Acknowledge | Consumer | Hands a message back without waiting for its lease to expire |
| Delete Queue | Admin | Marks a queue for deletion; removed once every node releases it |

### Two IDs, not one

Enqueue returns a **message ID**. Dequeue returns a **receipt**. They do different
jobs and last for different lengths of time.

| | Message ID | Receipt |
|---|---|---|
| How long it lasts | Forever | One delivery attempt, seconds to minutes |
| Says where the message lives | No | Yes |
| Used for | Logging and tracking by the producer | Routing the acknowledgment |

The rule we follow: only short-lived identifiers may contain location information. If
the message ID said which slot held the message, every ID a producer had saved
would be wrong the moment we changed the slot count. A receipt expires with its
delivery attempt, so it cannot outlive the layout it describes.

The receipt also carries a counter that increments each time a message is handed
out. Section 11 shows what that counter prevents.

The specification asks for acknowledgment "by ID." A receipt is that identifier,
and it has to be, for two reasons.

A bare message ID cannot be routed. Finding which slot holds a given message would
need an index covering every message in the cluster, updated on every submission and
every acknowledgment. That is a second distributed system to keep consistent.

More importantly, a bare message ID cannot tell two deliveries of the same message
apart. If a worker stalls past its lease and the message goes to somebody else, a
late acknowledgment from the first worker would delete a message the second worker
is still processing. The generation number in the receipt makes that detectable, and
it is needed even on a single machine — so this is about correctness, not routing.

### Request and response shapes

```
POST /v1/queues
{ "name": "orders", "visibilityTimeout": "30s", "maxRetries": 3,
  "defaultTTL": "1h", "starvationThreshold": "60s", "starvationReserve": 0.2,
  "deadLetterQueue": "orders-dlq", "maxDepth": 1000000,
  "distributed": false }

201  { "name": "orders", "created": true }
200  { "name": "orders", "created": false }   already exists, same settings
409  already exists with different settings
```

**`distributed` is the one placement decision a caller makes, and it defaults to
false.** A normal queue lives on one machine and gets exact priority ordering, exact
FIFO within a priority, and exact counts. Setting it true spreads the queue across
machines for throughput and gives those three up, for that queue only. Section 9
explains the trade.

It is not a partition count. There is still nothing to size, and no way to get it
wrong beyond a boolean that can be changed later.

```
POST /v1/queues/{name}/messages
{ "payload": "...", "priority": 75, "groupID": "user-123",
  "ttl": "30m", "deliverAfter": "10s", "deduplicationID": "order-9981" }

201  { "messageID": "8f14e45fceea167a" }
400  priority out of range, expiry already past, payload too large
404  no such queue, or it is being deleted
503  queue is at maxDepth, or the log cannot accept writes
```

`priority` takes a number from 0 to 100, or one of `HIGH`, `MEDIUM`, `LOW`, which
map to 75, 50 and 25. The names are there so a caller who only needs three levels
never has to think about the scale.

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
POST /v1/queues/{name}/messages/ack
{ "receipt": "..." }

200  acknowledged, and also returned if it was already acknowledged
409  the lease expired and the message went to another worker
400  malformed receipt
```

```
POST   /v1/queues/{name}/messages/nack   { "receipt": "...", "delay": "5s" }  → 200
DELETE /v1/queues/{name}                                                      → 202
GET    /metrics                    Prometheus format, counters
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
  "asOf": "2026-09-02T14:22:10.412Z",
  "exact": true
}
```

**For a normal queue these numbers are exact.** The queue lives on one machine, its
counters are kept under the slot locks as messages move, and the gateway makes one
request to read them. No sampling, no estimation.

For a `distributed` queue the gateway asks every machine holding a slot and adds the
answers together. Those answers describe slightly different instants, so the sum is a
point-in-time approximation rather than a true snapshot, and `exact` comes back
false.

If a machine cannot be reached the response says so rather than quietly
under-reporting:

```
200 { "messages": 124800, "exact": false, "unavailableSlots": 3 }
```

Gateways cache for one second so a caller polling in a loop cannot turn this into a
storm. `Cache-Control: no-cache` bypasses the cache.

### Identifying the caller

Every request carries a credential, and the gateway resolves it to an org before
doing anything else. Queue lookup is then by org and name together.

```
Authorization: Bearer <token>     →  org_7f2a
POST /v1/queues/orders/messages   →  queue (org_7f2a, "orders")
```

The org is not in the path and not in the body. A caller cannot reach another org's
queue by guessing a name, because the name is only half the identifier and the other
half comes from the credential.

Admin credentials may carry cross-org scope, in which case an explicit
`X-Ryuk-Org` header selects the target. That header is ignored for ordinary
credentials rather than rejected, so a client that sets it by accident sees no
change in behaviour.

### What the API leaves out

Creating a queue takes no partition count, no shard hint, and no placement rule.
How a queue is split up is our problem, not the caller's.

SQS works this way. Kafka does not: you pick a partition count when you create a
topic, you cannot change it later without reprocessing everything, and you have to
guess before you have seen any traffic. We would rather not ask a question the
caller cannot answer yet.

---

## 5. Architecture

Ryuk has two tiers and a small coordination store.

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
        │ gateway  │    │ gateway  │    │ gateway  │     no stored data
        │          │    │          │    │          │     translates and routes
        └────┬─────┘    └────┬─────┘    └────┬─────┘
             └───────────────┼───────────────┘
                             │  internal RPC
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
   ┌─────────┐          ┌─────────┐          ┌─────────┐
   │  node   │          │  node   │          │  node   │    holds messages,
   │ slots + │          │ slots + │          │ slots + │    leases, and
   │  logs   │          │  logs   │          │  logs   │    write-ahead logs
   └─────────┘          └─────────┘          └─────────┘
        └────────────────────┼────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
     ┌─────────────────┐          ┌──────────────────┐
     │    Postgres     │          │       etcd       │
     │                 │          │                  │
     │  queue configs  │          │  placement map   │
     │                 │          │  membership      │
     └─────────────────┘          └──────────────────┘
```

Both tiers read both stores. Neither store sits on the path a message takes.

### Gateways

Gateways store nothing. They accept HTTP from clients, resolve the caller's
credential to an org, check the request, work out which slot the message belongs to,
look up which node holds that slot, and forward the request.

Resolving the credential first matters: everything after it is scoped to one org, so
a request can only ever reach queues that org owns. There is no later check to
forget.

Two rules limit what a gateway may do.

**A gateway works things out; it never chooses.** Which slot a message goes to is a
hash of its group key, so every gateway gets the same answer. Which node holds a
slot comes from the metadata store. Since nothing about routing is a choice, we can
run as many gateways as we like without them talking to each other. If gateways
could choose placement on their own, two of them could send the same group to
different slots and quietly break ordering.

**A gateway never holds a write.** It does not tell a producer the message was
accepted until the node that owns the slot has saved it. If a gateway buffered
messages, we would be telling producers their work is safe while it sits in a tier
that stores nothing.

### Nodes

Nodes hold the data. Each one owns a set of slots, keeps their messages in memory,
tracks their leases, and writes their logs.

Nodes have no public listener. Only gateways and other nodes can reach them, so the
network boundary between the tiers is a real security boundary and not just a
convention.

### Metadata

Ryuk keeps three kinds of metadata. Their needs differ enough that they live in
different places.

| What | Where | Why there |
|---|---|---|
| Queue configs | Postgres | Changes rarely, has real structure, wants backups and history |
| Placement map | etcd | Everyone has to hear about a change immediately |
| Membership | etcd | Liveness is a key with a TTL that a node keeps refreshing |

#### Queue configs in Postgres

A queue config is about ten fields with validation rules. A schema handles that
better than a blob in a key-value store, and schema changes are a solved problem.

```sql
CREATE TABLE queues (
    org_id      TEXT NOT NULL,
    name        TEXT NOT NULL,
    settings    JSONB NOT NULL,
    state       TEXT NOT NULL DEFAULT 'active',   -- active | deleting
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (org_id, name)
);
```

**The primary key is the pair, not the name.** Two orgs can both have a queue called
`orders` and they are unrelated. That is what lets tenants pick their own names
without coordinating with anyone.

The same key does the concurrency work. Two gateways creating `orders` for the same
org at the same moment cannot both succeed. One wins, the other reads back the
existing row and returns 200 if the settings match or 409 if they do not.

Postgres also gives us things we would otherwise write ourselves: backups and
point-in-time restore, a history table recording who changed what and when, and
ad-hoc queries for an admin tool, like which queues have an expiry under five
minutes.

#### Placement map and membership in etcd

These need two things a database does not give us.

**Watches.** Gateways and nodes subscribe to the placement map and are pushed updates.
Polling instead would mean fifty gateways querying every hundred milliseconds and
learning nothing most of the time, and a rebalance would take a full poll interval
to reach everyone.

**Keys that expire on their own.** A node's liveness is a key with a TTL that the
node keeps refreshing. Stop refreshing and the key disappears and everyone notices.
In Postgres this would be a heartbeat column, a background job scanning for stale
rows, and a judgement call about what counts as stale, which is the thing etcd
already does. Electing the coordinator needs the same mechanism, and building a
distributed lock on top of a database is where these things usually go wrong.

FOQS splits it the same way: MySQL for state, Shard Manager for shard assignment.

#### Who writes what

| Data | Written by | Read by |
|---|---|---|
| Queue configs | Gateways | Gateways, nodes |
| Membership | Each node, about itself | Coordinator, gateways |
| Placement map | The coordinator | Gateways, nodes |

A node writes its own liveness because it is the only thing that knows whether it
is alive.

The placement map is written by a **coordinator, which is an elected role among the
nodes and not a separate service**. It watches membership, works out a new
assignment when something changes, and sequences the migrations described in
Section 9. If the coordinator dies, another node takes the lock and carries on.

Gateways own none of this. They are stateless on purpose, placement needs node-side
information they do not have, and they are the tier that scales up and down most
often — electing a coordinator among them would mean an election on every scaling
event.

Gateways do write queue configs. That does not break the rule that gateways never
choose, because a create has one definite outcome enforced by the unique
constraint. There is nothing here for two gateways to disagree about.

**Gateways get scoped credentials**: write access to the queue config table, and
read-only access to etcd. They sit closest to the outside world, and a compromised
gateway should not be able to rewrite the placement map.

#### Caching configs

Gateways and nodes both cache queue configs. Nodes need them anyway to run a queue:
visibility timeout, retry limit, expiry, starvation settings.

Two rules keep the cache from causing trouble.

**A cache miss means go and look, not "no such queue."** Someone creates a queue and
submits to it immediately. Treating a miss as a 404 would fail. On a miss we read
Postgres once and cache the answer.

**Negative results are cached for a second or two, not indefinitely.** Not caching
them at all means a loop with a typo'd name hits Postgres every time. Caching them
for long brings back the problem above.

Configs refresh on a thirty-second timer. Everything a config controls tolerates
that delay: a new visibility timeout applies to new leases, a new retry limit
applies to new failures. If thirty seconds turns out to be too slow, bumping a
version number in etcd whenever a config changes gives immediate invalidation and
reuses a watch that already exists.

#### Deleting a queue

Deletion happens in two steps, for the same reason SQS makes you wait sixty seconds
after deleting a queue before you can reuse the name. The deletion has to reach
every machine holding part of the queue, and that does not happen instantly.

1. **Mark it deleting.** Gateways stop accepting requests for it within a cache
   refresh. Every node holding one of its slots discards the messages and releases
   the slot.

2. **Remove the row.** Once no slots are assigned to the queue, the coordinator
   deletes the row from Postgres and the name is free again — free within that org
   only, since it was never global to begin with.

Deleting a queue discards its messages, the same as SQS does. Waiting for consumers
to drain it instead would mean a delete that never finishes when nobody is
consuming.

The difference from SQS is that step 2 waits for a real condition rather than a
fixed timer. The name becomes reusable when every node has genuinely let go of it,
not sixty seconds after the request. If a node is down while holding one of those
slots, removal waits. When that node comes back, or when its slots are reassigned to
someone else, the new owner sees the queue is deleting and releases it, so the
process always finishes.

An acknowledgment arriving for a message in a queue being deleted returns success.
The message is gone and there is nothing the worker can do about it. Once the row is
removed the queue is unknown, and requests get a 404.

#### When a store is down

Neither store is on the path a message takes, so an outage costs us changes, not
traffic.

| | Postgres down | etcd down |
|---|---|---|
| Enqueue, dequeue, acknowledge | Work, from cached configs | Work, from the cached placement map |
| Creating or changing a queue | Blocked | Works |
| Rebalancing | Works | Blocked |
| Nodes joining or leaving | Works | Blocked |

Both tiers cache what they need. The exception is a cold start: a gateway with an
empty cache cannot serve until it can read configs.

### Why two tiers

We could have one tier where any machine accepts any request and forwards it
internally. Several systems do that. We split them for reasons that matter
specifically to queues.

**Restarting a node causes redeliveries.** When a node restarts, every message it
had in flight goes back in the queue and gets delivered again. That is correct,
but it means every deploy causes a burst of duplicate work. Gateways hold no
messages, so shipping an API change — a new field, a stricter check, a rate limit —
never disturbs work in progress. The API tier changes far more often than the
storage tier, and over a few years that difference adds up.

**The tiers grow for different reasons.** Gateway load depends on how many
connections are open, which is driven by idle workers waiting for work. Node load
depends on how much data there is and how fast messages move. Combining them means
buying extra machines for one job to get enough of the other.

**They need different hardware.** Gateways use network and memory. Nodes use CPU
and disk, because they run timers and write logs.

**One tier cannot slow the other down.** A flood of requests hitting the gateways
cannot slow the timers and log writes that keep delivery correct.

The costs are one extra network hop, well under a millisecond inside a region
against a 100ms budget, and one extra failure case that Section 11 covers.

### How clients find Ryuk

Clients get one address: a DNS name or load balancer that sits in front of the
gateways. They make no routing decisions and store nothing about the cluster.

This works because any gateway can serve any request. Picking one at random is
always correct. Picking a node would not be, because only one node holds each slot,
and knowing which one is exactly what we are hiding.

Adding or removing a gateway changes the load balancer's list. Clients do not
reconnect, refresh anything, or follow redirects.

---

## 6. Submitting a Message

```
producer → gateway → node that owns the slot → its log
```

1. The gateway resolves the caller's credential to an org, then checks the request
   against its cached config: the queue exists in that org and is not marked
   deleted, the priority is in range, the expiry is not already past, and the payload
   is not too large. Rejecting an oversized payload here keeps it off the internal
   network. Depth is not checked here, because the gateway does not know how deep a
   queue is.

2. The gateway works out the slot. If the message has a group key, the slot is a
   hash of the org, the queue name and that key together, so **every message in a
   group goes to the same slot**. That is what makes ordering possible at all.
   Hashing the org and the queue name in as well is what stops two tenants who happen
   to use the same group key from landing on one slot. Without a group key the slot is
   picked in turn, across a number of slots that grows with how deep the queue is, so
   a small queue stays in one slot instead of scattering across all sixteen.

3. The gateway looks up which node owns that slot and forwards the request.

4. The node assigns a message ID, a submission sequence number for the queue, and
   records the submission time and expiry time. The sequence number is what lets us
   compare messages of equal priority sitting in different slots, which Section 12
   explains.
   **The node that owns the slot sets all times**, and those values travel with the
   record, so replaying the log produces the same state whatever the clock says at
   the time. Section 10 explains why that matters.

5. The node appends the record to the slot's write-ahead log and flushes it
   according to the queue's durability setting.

6. The node then adds the message to the in-memory structures that workers read.

7. The message ID goes back to the producer.

The gateway's check in step 1 exists to reject obvious mistakes early, so a loop
with a typo'd queue name never reaches the nodes. It is reading a cache, so the
node checks again: the queue has to exist there too, and it must not be at its
depth limit, which only the node can know. Where the two disagree the node wins,
because the node is where the message actually lands.

The order of steps 5 and 6 is what makes the queue durable. A worker can only see a
message that is already in the log. If we added it to memory first, a crash could
erase a message we had already handed out.

### Delayed and duplicate submissions

Two optional behaviours use the same path. A message can specify a time to become
available, in which case the node holds it in a timer until then. A message can
carry a duplicate-detection key, in which case resubmitting it within a few minutes
returns the original message ID instead of creating a second copy. That turns a
producer retrying after a network timeout into a harmless no-op.

---

## 7. Getting a Message

### What a worker asks for

A worker asks for work from a queue. It does not name a slot, a node, a priority, or
a group. Choosing is our job.

### Choosing within a node

Each slot keeps its available messages as a set of first-in-first-out lists, one per
priority level. Those lists hold **groups, not individual messages**.

Holding groups keeps the choice fast. If each list held messages directly, we would
have to skip over any message whose group already had one in flight. A single group
with a large backlog would then slow down every request, because each one would scan
past the whole backlog. Pointing at groups instead means we never scan.

Choosing a message is then:

```
take the next group from the most urgent list that has anything in it
  → take that group's oldest message
  → lock the group so its next message waits
```

Finding the most urgent non-empty list across 101 levels cannot be a loop. Each slot
keeps a bitmap with one bit per priority level, set when that level has work. Finding
the highest set bit is a single machine instruction, so the number of priority levels
does not affect how long a request takes. Operating system schedulers use the same
trick.

**All 16 slots of a normal queue are on one node**, so the node compares every slot's
head and picks the genuinely most urgent, oldest message. There is no guessing.

### Choosing the node

For a normal queue there is nothing to choose. One hash gives the placement group,
one map lookup gives the node, and the request goes there. One hop.

For a `distributed` queue the gateway has to pick among the machines holding slots,
and a bad pick means a worker gets low-priority work while urgent work sits
elsewhere. Each node publishes a short summary — the most urgent priority it holds
for each distributed queue, a few bits, every hundred milliseconds — and gateways
route on that. An out-of-date summary costs one slightly wrong choice and the next
refresh fixes it. Asking every node on every request would put a round trip on the
hot path and generate traffic that grows with the square of the cluster.

### Leasing a message

Handing a message to a worker does not delete it. It **moves** the message from the
available list into the in-flight list, with a lease deadline and a lease generation.

Because the message is no longer in the available list, no other worker can see it.
There is no "hidden" flag for the selection code to check. A flag would mean
selection gets slower as more messages are out with workers; moving the message keeps
it constant.

The worker gets the message and a receipt containing the slot, the message identity,
the lease generation, and the incarnation number.

### Waiting for work

A worker can hold a request open for up to twenty seconds waiting for something to
arrive. Without this, idle workers poll in a loop and waste capacity in proportion to
how many workers there are.

The obvious implementation is wrong. If the gateway forwarded a waiting dequeue to
every machine that might get the message, two of them could answer, and the second
message would be leased with nobody to process it — invisible until its lease
expired. Under load that would happen constantly.

So notification and dequeue are separate things:

```
consumer ──poll──▶ gateway                            node
                   │                                    │
                   │  1. try now, one dequeue           │
                   │───────────────────────────────────▶│  nothing available
                   │                                    │
                   │  2. subscribe, park the consumer   │
                   │───────────────────────────────────▶│
                   │                                    │
                   │  3. ◀──── "this queue has work" ───│  a message arrives
                   │                                    │
                   │  4. one dequeue ──────────────────▶│
                   │     ◀──── message ─────────────────│
                   ▼
              respond
```

**Only ever one dequeue is issued**, so no message is leased speculatively.

The connection cost is what makes this work. A gateway keeps **one stream per node**,
not one per queue and certainly not one per waiting consumer. Ten thousand parked
consumers are ten thousand entries in a map and zero extra sockets. Subscriptions
ride on streams that already exist.

If several gateways are waiting on the same queue, the node notifies only as many as
it has messages for, chosen round-robin. A gateway that loses the race gets an empty
answer and keeps its consumer parked.

Step 1 matters: a busy queue never reaches step 2, so the extra round trip only
exists on queues that are idle, where nobody notices it.

This is also why the gateway-to-node protocol streams rather than using
request-and-response. When a gateway dies its streams break, and the node drops its
subscriptions and releases any leases it was holding, instead of those messages
waiting out a full visibility timeout.

---


## 8. Concurrency

Producers and consumers run at the same time with no coordination between them, so
every structure a request touches has to be safe under concurrent access. This is
the part of the design most likely to be wrong in a way that ordinary tests do not
catch.

### One lock per slot

The slot is the unit of locking. Everything inside it is covered by a single lock:
the per-priority lists, the group lists, the set of locked groups, the in-flight
table, the lease timers, and the counters.

Contention therefore grows with the number of slots, not with the size of the queue.
A queue has sixteen slots and therefore sixteen independent locks. Two requests
collide only when they touch the same slot, and for a normal queue all sixteen are in
one process, so the dispatcher can compare every one of them without a network call.

### Why not something finer

Splitting that lock further was considered and rejected. Handing out a message
changes five things at once: it takes a group off a priority list, takes a message
off that group's list, marks the group locked, adds an entry to the in-flight table,
and pushes a lease timer. Those changes have to happen together, or another worker
can see a half-finished state — a group marked locked with nothing in flight, or a
message that appears to be in two places.

Splitting the lock would mean acquiring five locks in a fixed order on every
request. That is slower than one uncontended lock, and the ordering rules are the
kind of thing that works until somebody adds a sixth structure.

### Rules that keep it deadlock-free

- **Never hold two slot locks at once.** Nothing in the design needs a transaction
  spanning slots, so the lock graph has no cycles and deadlock is not possible.
- **Never do I/O while holding a slot lock.** Log writes happen before the lock is
  taken, never during.
- **The lookup that finds a slot is held only for the lookup.**

### Group locks are bookkeeping, not locks

A group lock is an entry in a set, added when a message from that group goes out and
removed on acknowledgment or lease expiry. It is read and written only under the
slot's lock, so it needs no synchronisation of its own.

The distinction matters. A real lock held by a crashed worker would need recovery
machinery. Group state is derived from the in-flight table, so a lease expiring
releases the group automatically, with no separate cleanup to get wrong.

### Background work

Several things run on timers rather than on requests: expiring leases, dropping
expired messages, releasing delayed messages, writing snapshots.

Each takes one slot's lock, does its work, and releases it before moving to the next
slot. A sweep never holds more than one lock and never blocks the whole queue. A
sweep falling behind delays redelivery; it does not stop traffic.

All of them can also be called directly instead of only on a timer, which is what
makes their behaviour testable without waiting for real time to pass.

### Writes and the log

A log write happens before the in-memory change, and outside the slot lock. The
sequence is: build the record, append and flush it, then take the lock and apply
it.

Disk and network latency stay outside the critical section, so a slow disk slows one
request rather than blocking every request that touches the same slot.

---

## 9. Placing Queues on Machines

### One queue, one machine

By default a queue and all 16 of its slots live on **one node**, durable through
that node's write-ahead log. That single decision removes a great deal of machinery:

| | Queue on one machine | Queue spread across machines |
|---|---|---|
| Priority ordering | **Exact** | Approximate |
| FIFO within a priority | **Exact** | Approximate |
| Message counts | **Exact, one request** | Sum across machines |
| Routing a dequeue | One hash, one hop | Summaries, sometimes a second hop |
| Waiting for work | One stream, one node | Subscriptions on every holder |

The node still has 16 slots for that queue, so 16 independent locks and no
serialisation. They all live in one process, where the dispatcher can see every one
of them and pick the genuinely most urgent, oldest message.

**The cluster still scales, by spreading queues rather than splitting them.**
Multi-tenancy makes this easy: a thousand orgs with ten queues each is ten thousand
queues across fifty machines, two hundred per machine, balanced without splitting
anything.

### The one thing that does not scale

A single queue cannot exceed one machine — roughly a few hundred thousand messages a
second, with depth bounded by that machine's memory. That is a large queue. A hundred
thousand a second is eight and a half billion messages a day on one queue.

For a queue that genuinely outgrows it:

```
POST /v1/queues   { "name": "events", "distributed": true }
```

That spreads the queue's 16 slots across up to 16 machines. In exchange it gives up
exact priority ordering, exact FIFO within a priority, and single-request counts —
**for that queue only**. Every other queue in the cluster is unaffected.

Ordering within a group stays exact either way, because a group never spans slots.

This is the same trade every system makes. A database table lives on one node until
it does not fit, and then you shard it and lose cross-shard transactions. Nobody
shards every table by default.

**Automatic promotion** — the coordinator noticing a queue is saturating its machine
and setting the flag itself — is a natural extension and is not built.

### What stops one busy queue hurting its neighbours

Two hundred queues on a machine and one goes hot. Three defences, at three
timescales:

| When | What |
|---|---|
| Milliseconds | A per-queue worker limit and a depth cap. One queue cannot take more than its share of a machine, and gets backpressure instead |
| Seconds | The coordinator moves *other* queues off the hot machine. Small queues move quickly |
| Minutes | The coordinator moves the hot queue somewhere emptier, or gives it a machine to itself |

The first is what stops a machine falling over. A node under load must return errors
and get slower, never run out of memory — that is what the depth cap is for, and it
is true regardless of how queues are placed.

Note what spreading a hot queue would actually do: instead of degrading the two
hundred queues that share its machine, it would put a slice of that load on every
machine in the cluster and degrade all ten thousand. Concentration contains the blast
radius; spreading distributes it.

### Placement groups

Which machine holds a queue comes from a map, and that map has to stay small.

A map keyed by queue would have one entry per queue — a million entries for a million
queues, in a coordination store meant for small amounts of data. So there is a layer
in between:

```
   normal queue        (org, queue)        ──hash──▶ placement group
   distributed queue   (org, queue, slot)  ──hash──▶ placement group
   placement group                         ──map───▶ node
```

There are **4096 placement groups, fixed forever**. etcd holds 4096 entries no matter
how many orgs and queues exist. The hash never changes; only the placement group to
node assignment moves.

The name comes from Ceph, which uses the same three levels for the same reason:
objects map to placement groups, and placement groups map to storage daemons.

**Slots are owned by exactly one queue**, and therefore one org. A placement group
holding messages from many queues would put two tenants' traffic behind the same
lock, so a busy org would slow a quiet one for no reason other than a hash collision.
Placement groups decide only which machine holds something.

### How placement groups are divided

The coordinator computes the assignment and publishes it. It starts from **rendezvous
hashing** — for each placement group, score every live node and take the highest —
because adding a machine then moves only the groups that machine now wins, about
`1/(n+1)` of them. A plain modulo would move nearly everything on every change.

Then it corrects for things a hash cannot know: actual observed load rather than
group count, spreading a busy org's queues across racks, and eventually pinning an
org's groups to a subset of machines.

4096 balances well up to roughly a hundred machines. Beyond that the granularity gets
coarse and the number would want to be larger, which is why it is a deployment
constant chosen once.

### When machines join or leave

The coordinator publishes a new version of the placement map. Gateways and nodes pick
it up; a request arriving at a machine that no longer owns something is redirected
rather than served.

**Rebalancing is slowed down on purpose.** A cooldown after any membership change,
and a minimum imbalance before it starts at all. A machine flapping in and out would
otherwise cause continuous data movement, which hurts far more than the imbalance it
was correcting. Migrations are also rate-limited, so adding a machine does not
saturate the network with rebalance traffic.

### Moving a slot

A slot being moved may have messages out with workers and groups locked, so the
handoff has steps:

```
   losing node                              gaining node
   ───────────                              ────────────
   1. freeze the slot
      - dequeues redirect
      - enqueues forward
   2. ship state and log tail  ──────────▶  3. rebuild the slot
                                            4. void inherited leases
   5. confirm                  ◀──────────
   6. release ownership                     7. start serving
```

**Freezing is what keeps ordering correct.** Without it the losing node could still
hold a group's first message while the gaining node hands out its second. The cost is
one slot out of sixteen unavailable, and only while it moves.

**Inherited leases are voided, not carried over.** A deadline only means something
against the clock that set it, and carrying one across machines would make
correctness depend on clocks agreeing. Voiding causes some duplicate delivery, which
at-least-once already permits.

The handoff recovers from either side. If the gaining node fails partway, the losing
node still owns the slot and tries elsewhere. If the losing node fails partway, it
still owns the slot and the move resumes when it comes back — the gaining node
discards what it received rather than serving a partial copy.

### Many tenants

Orgs are a naming and access boundary, and slot ownership makes them a contention
boundary too: no hash collision can put two tenants behind the same lock.

Per-queue depth limits are enforced exactly, on the machine holding the queue.
Cluster-wide quotas per org, and cells that give an org machines nobody else touches,
are in Section 22 and are not built.

---


## 10. Not Losing Messages

### One log per slot

Every slot has its own write-ahead log. Not one per machine, and not one for the
whole cluster.

The reason is that slots move. If a machine kept one log for all its slots, moving a
slot would mean picking that slot's records out of a mixed stream. A log per slot
means a slot's log is its whole history, which is what makes the handoff in Section 9
a matter of copying bytes.

### Write before visible

```
1. Build the record
2. Append it to the slot's log
3. Flush according to the durability setting
4. Only then apply it in memory
5. Answer the producer
```

Step 4 comes after step 3, and that ordering is the entire guarantee. **A worker can
only be handed a message that is already in the log.** Reverse them and a crash can
erase a message we already gave out.

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

The last three are deliberate. Flushing on every delivery would double the cost of
the most common operation, and the worst case after a crash is a message getting one
extra retry before we give up on it. Flushing every acknowledgment would add latency
to prevent a redelivery, which at-least-once already permits.

**Lease deadlines are never written.** Every lease is void after a crash anyway, so
saving them is wasted work. The attempt count is the one piece that must survive:
without it a message that always fails would retry forever and never reach the
dead-letter queue.

### How hard we flush

| Setting | Survives | Cost |
|---|---|---|
| `always` | Process crash and machine power loss | ~1 ms per submission |
| `interval` (default, 100 ms) | Process crash fully; up to 100 ms lost on power loss | Amortised to nothing |
| `never` | Process crash only | Nothing |

`interval` is the default because a *process* crash loses nothing either way — the
data is already in the operating system's page cache and survives the process dying.
Only losing power or the machine itself opens the window, and 100 ms of exposure is
the right trade for most workloads. Group commit batches concurrent writers into one
flush.

### Recovery

A node coming back reads each slot's most recent snapshot and the records after it,
verifying a checksum on each and stopping at the first that fails. A half-written
record at the end of a log is what a crash looks like, not corruption, and nothing
after it can be valid.

After replay:

- Every in-flight message goes back to available. Leases do not survive a restart.
- The slot's **incarnation number** is incremented and written to the log.

That second point matters more than it looks. Lease generations restart from zero
after a replay, so a receipt issued before the crash could match a generation issued
after it, and an acknowledgment for a long-dead delivery would delete a message
another worker is processing. Every receipt carries the incarnation, and one from an
older incarnation is rejected outright.

### What this survives, and what it does not

| Failure | Survives |
|---|---|
| Process panics, is killed, or is redeployed | **Yes, completely** |
| Machine reboots | **Yes** |
| Disk fills up | **Yes** — submissions rejected, nothing lost |
| Log damaged at the end | **Yes** — truncated at the checksum |
| Machine and its disk destroyed | **No** |

The last row is the honest limit. Messages live in one machine's memory and one
machine's log, so losing that machine permanently loses its queues.

**Replication is what fixes it, and it is not built** (Section 22). The design is
straightforward — the log is already the replication stream, so shipping records to
followers and waiting for a quorum before step 5 is an addition rather than a
redesign — but a partly-working replication protocol is worse than none, and this
document would rather say which it has.

There is a cheaper production answer worth knowing: put the log on storage that
outlives the machine. Cloud persistent disks detach from a dead instance and attach
to a new one, which turns permanent machine loss into a slow restart. It costs
nothing in the design and needs no consensus protocol.

---


## 11. When Things Break

| What breaks | What happens | Data lost | Who notices |
|---|---|---|---|
| A node's process crashes | It restarts and replays its logs | None, or up to the flush window on power loss | The queues it held, until replay finishes |
| A node reboots | Same | Same | Same |
| **A node is destroyed** | **Its queues are gone** | **All of them** | Everyone using them |
| A gateway dies | The load balancer stops sending to it | None | Nobody |
| Every gateway dies | Nodes are fine; service returns when one comes back | None | Everybody, briefly |
| The network splits | Nodes keep serving whatever they hold | None | Callers who cannot reach a node |
| A disk fills up | Submissions rejected; delivery and acknowledgment continue | None | Producers |
| A log is damaged at the end | Truncated at the bad record | The torn tail only | Nobody |
| A worker crashes mid-job | Its lease expires and the message is redelivered | None | Nobody |
| A worker stalls past its lease | Redelivered; the late acknowledgment is rejected | None | Nobody |

Row three is the one replication would remove. Everything else is handled.

### What survives a restart

```
  available messages    → replayed from the log
  in-flight messages    → leases voided, messages go back to available
  attempt counts        → replayed
  lease deadlines       → gone, which is what we want
  group locks           → released
  incarnation number    → incremented, invalidating older receipts
```

This is why we promise **at-least-once** and not exactly-once. A worker may still be
processing a message whose lease was just voided, and that message will be delivered
again. Workers must be idempotent, and we say so plainly rather than implying
anything stronger.

### Placement only moves by handoff

Without replicas a node's data exists nowhere else, which makes one rule
non-negotiable:

**A placement group is never reassigned away from a node that still holds its data.**

If the coordinator reassigned a dead node's groups, the new owner would serve an
empty queue while the old node still had every message — and when the old node came
back, two machines would believe they owned the same queue.

So groups move only by **handoff**, with both machines participating (Section 9). A
node that is unreachable keeps its assignment, and its queues stay unavailable until
it returns. An operator can force reassignment and explicitly accept the loss;
nothing does it automatically.

This is the direct cost of having no replicas, and it is why the matrix above says
"unavailable" where a replicated system would say "a few seconds."

### The gateway failure case

Splitting the tiers adds one failure we would not otherwise have. A gateway can die
after a node has handed out a message but before the worker receives it. The message
is not lost, but nobody sees it until its lease expires.

This is why the internal protocol streams. When a gateway dies its connections break,
and the node releases those leases immediately rather than waiting out a full
visibility timeout. The gap shrinks from the whole timeout to however long it takes
to notice a dropped connection.

### Clocks

Almost nothing here depends on machines agreeing about the time.

**Lease deadlines never leave the machine that set them.** They are computed and
checked by one node, never written down, and voided on restart. They use elapsed
monotonic time, so a clock adjustment cannot cause a burst of premature redelivery.

**Expiry times are stamped once, by the node that accepted the message**, and stored
in the record. Replay reads them rather than recomputing, so a log replays to the same
state regardless of what the clock says at the time. That is the invariant that makes
replay deterministic, and the one a later change is most likely to break silently.

**Ordering never reads a clock at all.** Position in a list and a submission counter
decide it.

---


## 12. What We Promise About Delivery and Order

### Delivery

**At least once.** A message is delivered until it is acknowledged. It can arrive
more than once: when a worker processes it but does not acknowledge in time, or
when the node holding it dies while the message is out.

We do not offer exactly-once. Doing so would require the worker's own side effects
to be part of the same transaction as the acknowledgment, and we have no way to
enforce that.

### Order

| Where | Normal queue | `distributed` queue |
|---|---|---|
| Within a group | **Strict, always** | **Strict, always** |
| Between priorities | **Exact** | Approximate |
| FIFO within one priority | **Exact** | Approximate |

**A normal queue gives exact FIFO within a priority.** All 16 slots are on one
machine, so when several of them hold work at the same priority the node compares
their oldest messages by submission sequence number and takes the earliest. Nothing
crosses a network, so the comparison costs nothing.

**A `distributed` queue gives up that guarantee**, and this is the main thing the
flag buys and costs. Comparing sequence numbers across machines would mean asking
every one of them on every request. Instead each machine reports the most urgent
priority it holds and gateways route on that, so ordering stays exact within a slot
while two equal-priority messages on different machines can come out in either order.

Ordering inside a group is exact in both cases, because a group never spans slots.

The sequence number is a counter, not a clock, so none of this depends on machines
agreeing about the time.

### Why whole-queue ordering is not the interesting guarantee

Even where we give exact FIFO, it is worth being clear about what it means. With ten
workers pulling at once, the order messages go *out* is decided by which worker asks
first, and the order work *finishes* is decided by how long each job takes. A queue
can promise the order it hands work out. It cannot promise the order work completes,
and the second is what callers usually care about.

That is why group ordering is the guarantee to reach for. Two messages that must be
ordered relative to each other should share a group key, and then the promise holds
end to end, at any cluster size, distributed or not.

### Why groups are the guarantee that matters

Group ordering holds up in practice. Nobody needs message 1 before message 2 when
the two are unrelated. They need "do not apply this user's update before their
account is created," and that is exactly what group ordering gives, at any cluster
size, without slowing down anything unrelated.

Order inside a group comes from the structure: messages join at the back of the
group's list and leave from the front. **Nothing about ordering reads a clock**, so
clocks disagreeing between machines cannot affect it.

### One group that is too busy

All of a group's messages live in one slot, so they live on one machine. A group
that gets far more traffic than the rest is limited to that machine. We cannot fix
this without giving up the ordering guarantee that made groups worth having. Per-slot
depth metrics make it visible, and the fix is a more specific group key.

---

## 13. Keeping Low Priority Alive

### The problem

Always serving the most urgent work means less urgent work may never run. With a
hundred levels this is worse than with three: work at priority 40 waits behind
anything at 41 or above, not just behind "high priority." Under steady load, low
priority messages sit until they expire and get deleted.

A priority queue that quietly destroys low-priority work is broken, not just badly
tuned. We treat this as a correctness requirement.

### Reserve a share of capacity

Ryuk serves the most urgent work, except that a fixed share of deliveries is set
aside for work that has been waiting too long.

```
   for each delivery:

       if this delivery falls in the reserved share
          and something has been waiting past the threshold:
              serve the thing that has waited longest

       otherwise:
              serve the most urgent thing
```

Three things make this work.

**It costs nothing when nothing is stuck.** The waiting threshold acts as a gate. If
nothing has waited too long, every delivery serves the most urgent work. We only
depart from priority when there is a real problem.

**It cannot flip the problem around.** The obvious version of this has a bug. If we
departed from priority whenever anything had waited too long, a large old backlog
would take every delivery, and urgent work would be the thing that never runs.
Capping the departure at a fixed share means old work drains steadily while urgent
work keeps the rest.

**It does not get slower with more priority levels.** Checking whether a level has
been waiting too long takes constant time. We check from a rotating starting point
and stop after a fixed number of checks.

### What this guarantees

> Nothing waits longer than the threshold plus the time it takes to drain the work
> ahead of it, at the reserved share of throughput.

In practice: **set the waiting threshold below the shortest expiry you use, and
messages can never expire from waiting.** Queue creation checks this and rejects
settings that break it, because that combination turns a queue into something that
silently deletes work.

### Where this idea comes from

Reserving a minimum share for work that strict priority would otherwise ignore is
an old idea:

| System | How it does it |
|---|---|
| Linux deadline I/O scheduler | Reads can only jump the queue so many times before a write goes |
| Linux real-time throttling | Real-time tasks capped at 95% so normal tasks always get 5% |
| Cisco class-based weighted fair queuing | The priority class is capped so it cannot take the whole link |
| Hadoop YARN fair scheduler | Every queue has a guaranteed minimum share |

### What we did not do

**Raising a message's priority as it waits** is a nicer idea in principle, but it
means either moving messages between priority levels, which our structure cannot do
cheaply, or one timer per message. Reserving a share gives a similar guarantee for
much less work.

**Picking randomly with weights** spreads the load more smoothly but promises
nothing definite. An operator can act on "nothing waits more than a minute." They
cannot act on a probability.

---

## 14. A Message's Life

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
      └─────────┘      [  in flight   ] ──────────────┘
                         │           │
              acknowledged           out of retries
                         │           │
                         ▼           ▼
                   ┌──────────┐  ┌─────────────┐
                   │ deleted  │  │ dead-letter │
                   └──────────┘  └─────────────┘
```

A message is either available or in flight, never both. Handing it out moves
it. Acknowledging deletes it. An expired lease moves it back.

### Two rules

**A message that expires while a worker has it can still be acknowledged.** Expiry
stops us handing a message out. It does not mean throwing away work someone has
already done. If the worker's lease expires instead, we drop the message then.

**A message handed back goes to the end of its priority list, not its old place.**
Otherwise a message that keeps failing would block the front of that list every
time it came back. Its place inside its group does not change, so group ordering is
unaffected.

### The dead-letter queue

A message that runs out of retries moves to a dead-letter queue, which is an
ordinary queue using the same code. It keeps its original payload, its retry count,
and its original submission time, so its age shows how long the problem has been
there rather than when we gave up.

---

## 15. Metrics

**Prometheus scrapes the gateways. It never scrapes the nodes.**

Nodes have no public metrics endpoint for queue data. They answer an internal request
instead, and the gateway does the adding up.

### How the numbers get out

Each gateway runs a collector. Every ten seconds it asks each node, in one request,
for the counters of every queue that node holds, and caches the answers. Both
endpoints serve from that cache:

```
GET /metrics                     Prometheus text format, every queue
GET /v1/queues/{name}/stats      JSON, one queue, can bypass the cache
```

That is **one request per node per interval, regardless of how many queues exist**.
Fifty machines cost fifty requests every ten seconds whether the cluster holds ten
queues or ten thousand.

Three reasons to collect rather than let Prometheus scrape nodes:

**Nodes have no public listener.** Giving them one to serve metrics would undo the
security boundary in Section 5 for the sake of a monitoring convenience.

**A node holds slots, not queues.** Something has to add them up. Doing it in
Prometheus means the dashboard and the stats API are two separate aggregation paths
that can disagree, and when they do, nobody knows which to believe.

**Scrape timing differs per target.** Prometheus scrapes each machine on its own
schedule, so a sum across machines mixes samples taken up to a full interval apart.
Collecting on one schedule removes that entirely.

### Counting each slot once

Every slot has exactly one owner, so there is no risk of a replica double-reporting
today. Two rules keep it that way once replication exists, and both matter during a
handoff regardless:

- A node that stops owning a slot **stops exporting its series**, rather than
  exporting zero. Disappearing is what triggers Prometheus staleness handling;
  exporting zero leaves a phantom in every `sum`.
- During a handoff the losing node stops before the gaining node starts, so a slot is
  never counted twice.

When replication arrives, add a third: only the leader reports. If replicas reported,
every number would be three times too high and look entirely plausible.

### What is exported

Required by the specification, per queue:

```
ryuk_queue_ready_messages{org,queue,priority}      oldest-first depth, by bucket
ryuk_queue_inflight_messages{org,queue}
ryuk_queue_oldest_message_age_seconds{org,queue}
ryuk_queue_enqueued_total{org,queue}               counter
ryuk_queue_acknowledged_total{org,queue}           counter
ryuk_queue_dead_lettered_total{org,queue}          counter
```

Priority is reported in three buckets rather than a hundred levels. A hundred label
values per queue is a monitoring problem, not useful detail.

Throughput is exported as **counters**, and the specification asks for a rate. Rates
belong in the query, because computing them here breaks whenever someone changes the
scrape interval. The JSON stats endpoint returns computed rates over a one-minute
window for callers who just want a number without running Prometheus.

Beyond the requirements:

```
ryuk_queue_starvation_escapes_total{org,queue}     ← the one worth alerting on
ryuk_queue_expired_total{org,queue}
ryuk_queue_redelivered_total{org,queue}
ryuk_queue_delayed_messages{org,queue}
ryuk_slot_depth{org,queue,slot}                    reveals a group that is too busy
ryuk_sweep_lag_seconds{node}
ryuk_wal_append_seconds{node,quantile}
```

`starvation_escapes_total` is the most useful number here. If it is climbing, workers
cannot keep up with urgent work and low-priority work is moving only because of the
safety net in Section 13. None of the required metrics show that.

### Querying

```promql
ryuk_queue_ready_messages                                 depth, already summed
max by (org, queue) (ryuk_queue_oldest_message_age_seconds)
rate(ryuk_queue_enqueued_total[1m])                       throughput
```

Depth needs no `sum` — the gateway did it. Oldest age aggregates as a **maximum**,
never a sum, and that is the aggregation most often got wrong.

### Duplicate gateways

Every gateway collects the same numbers, so scraping several and summing multiplies
them. Either scrape one gateway, or aggregate queue metrics with `max by (org,
queue)`. Gateway-local series — request rates, latencies, cache hit ratios — carry a
`gateway` label and sum normally.

### Cardinality

Series count is roughly queues × 3 for the bucketed depth, plus a handful each. Ten
thousand queues is a few tens of thousands of series, which is comfortable. A million
queues is not: above a few thousand, export per-queue series only for queues above a
size threshold, keep per-org rollups always, and use the stats endpoint for anything
specific.

### Where the numbers come from

Every counter is maintained under a slot's lock as messages move, never computed by
scanning, so reporting does not get slower as queues get deeper. Nothing is written
to the log: after a crash a node replays, rebuilds its structures, and the counters
fall out of the rebuilt state.

---


## 16. Latency Budget

The target is a 95th percentile under 100ms for submitting and receiving, within a
region. Roughly where that goes:

| Step | Typical | Notes |
|---|---|---|
| Client to gateway | 1–5 ms | Depends how far away the caller is |
| Gateway checks | under 0.1 ms | Cached config, no I/O |
| Gateway to node | 0.2–1 ms | Same region |
| Slot lock and structure work | under 0.05 ms | Constant time, nothing is scanned |
| Log append | around 0.05 ms | Sequential write to the page cache |
| Flush to disk, if set to always | 0.5–2 ms | Skipped on the default interval setting |
| **Submitting, total** | **about 2–8 ms** | |

Receiving is cheaper still, because the attempt record is not flushed before we
answer.

Adding replication later would put one more round trip on submission — one to three
milliseconds inside a region — which the budget has ample room for.

Almost all of the budget is network time. The parts we control — choosing a message,
taking a lock, appending a record — are constant time and well under a millisecond
together. That leaves enough headroom for a cross-region hop or a slow disk without
missing the target.

The two things that would ruin this are both avoided by design: scanning a structure
whose size grows with queue depth, and asking every machine on every receive. A
normal queue needs neither — one hash gives the machine, and selection is constant
time once there.

---

## 17. How Far It Scales

| What | Grows by | Where it stops |
|---|---|---|
| Cluster throughput | Adding machines | Nothing — no shared bottleneck |
| Number of queues | Adding machines | Postgres capacity |
| Number of orgs | Adding machines | Postgres capacity |
| Open connections | Adding gateways | No practical limit |
| Total messages held | Adding machines | Aggregate memory |
| **One normal queue** | **Does not grow** | **One machine** |
| One `distributed` queue | Adding machines | 16 slots |
| One group's throughput | Does not grow | One slot, by design |
| Placement map size | Does not grow | Fixed at 4096 placement groups |

Only three rows are capped, and each is capped for a reason.

**A normal queue runs on one machine** — a few hundred thousand messages a second,
depth bounded by that machine's memory. In exchange it gets exact ordering and exact
counts. A queue that outgrows it sets `distributed` and spreads over up to 16
machines, giving those up.

**A group runs on one slot**, which is the price of its ordering guarantee and cannot
be removed without removing the guarantee.

Everything else is linear in machines. There is no coordination bottleneck anywhere:
different queues share nothing, and the placement map is read from cache.

### What a slot costs

Slots are created only when they first receive a message, so an idle queue costs
nothing. A live slot costs roughly 400 bytes of bookkeeping before any message is
stored in it.

| Deployment | Queues | Live slots | Bookkeeping |
|---|---|---|---|
| 1,000 queues | 1,000 | ~16,000 | 6 MB |
| 1,000 orgs, 10 queues each | 10,000 | ~40,000 | 16 MB |
| 1,000 orgs, 1,000 queues each, mostly small | 1,000,000 | ~2,000,000 | 800 MB |
| The same, every queue busy | 1,000,000 | 16,000,000 | 6.4 GB |

The last row is the worst case and it spreads across the cluster, so at fifty
machines it is around 130 MB each. Message payloads dominate it as soon as queues
hold anything.

Two decisions keep that number where it is. The per-priority ready lists are held
sparsely, so a queue using three priorities does not pay for a hundred and one. And
messages without a group key fan out in proportion to how deep the queue is rather
than spreading over all 16 slots straight away, so a queue holding a thousand
messages uses one slot and only a queue holding millions uses all of them. Without
that, any queue that ever held 16 messages would materialise every slot it has.

Total memory is what limits how deep a backlog can get. A production version would
add a storage tier that keeps only the front of each priority list in memory. The
change is contained, because choosing a message only ever looks at the front.

---

## 18. What We Build Now, and What We Design For

This document describes a production system. **The implementation is a single process
holding everything in memory**, and the gap is deliberate — the assignment says
correctness over completeness, and a half-finished distributed system reads worse
than a solid single node.

### Built

| Area | What |
|---|---|
| Engine | Slots, groups, priority bitmap, the starvation reserve |
| Lifecycle | Leases, visibility timeouts, retries, dead-letter queue, expiry, delayed delivery |
| Correctness | Stale-acknowledgment rejection by lease generation |
| Durability | Write-ahead log, replay, crash recovery |
| API | The full HTTP surface of Section 4, in one process serving both tiers |
| Metrics | Collector and both endpoints, reading local slots |
| Multi-tenancy | Orgs as a naming, access and contention boundary |
| Tests | Everything in Section 19 |
| Harnesses | Producer and consumer stubs under concurrency |

### Designed, not built

| Area | What |
|---|---|
| Distribution | Placement groups spread over machines, the `distributed` flag |
| Replication | Quorum commit, replicas, failover (Section 22) |
| Coordination | Postgres and etcd as separate stores, coordinator election, rebalancing |
| Movement | Slot handoff between machines |
| Routing | Priority summaries between machines, cross-machine collection |
| Safety | Split-brain fencing, leader leases |

Configs live in an in-memory map with the same interface Postgres would sit behind.
The placement map answers "all slots are mine". Both are real interfaces with a
single-node implementation, not stubs.

### What the guarantees are, as built

| Guarantee | Single process | Designed |
|---|---|---|
| At-least-once delivery | **Yes** | Yes |
| Order within a group | **Yes** | Yes |
| Exact FIFO within a priority | **Yes** | Yes, unless `distributed` |
| Exact priority ordering | **Yes** | Yes, unless `distributed` |
| Exact message counts | **Yes** | Yes, unless `distributed` |
| Survives the process crashing | **Yes**, from the log | Yes |
| Survives losing a machine | No | No — replication is future work |
| One tenant cannot read another's queues | **Yes** | Yes |
| One tenant cannot slow another by locking | **Yes** | Yes |

Note which rows are stronger in the single-process build: because everything is on
one machine, ordering and counts are exact by construction. The distributed design
does not improve those — it trades them for throughput.

The single process exercises every state change in Section 14 and every rule in
Section 8. What it does not exercise is anything that needs a second machine to
exist.

---


## 19. Testing

### How things get tested

**Time-dependent behaviour, under a clock the test controls.** Visibility timeouts,
expiry, delayed release, and the starvation threshold are all tested by moving a
clock forward rather than by sleeping. Sleeping makes tests slow and unreliable, and
it cannot test a twelve-hour timeout at all.

**Concurrency, with a race detector.** Many producers and many consumers at once,
with a tool that reports unsynchronised access directly instead of waiting for a bug
to appear by luck. The detector is the method; a stress test that happens to pass
proves very little.

Six things have to hold after every run:

1. Everything submitted ends up in exactly one end state
2. No message is acknowledged twice
3. No message is out with two workers at the same time
4. Within a group, the order messages went out matches the order they came in
5. Within one priority, messages came out in submission order
6. The run finishes instead of deadlocking

The fourth catches the most real bugs. The fifth is only assertable because a normal
queue keeps all its slots on one machine — it is the test that would have to be
relaxed if a queue were marked `distributed`.

**Comparison against a deliberately simple version.** Random sequences of operations
run against both the real implementation and a naive one with a single lock and no
slots. The two must behave the same from the outside. This finds mistakes in the
fast structures that a hand-written test would not think to try.

**Workers that behave badly.** Consumers that take a message and vanish, acknowledge
twice, acknowledge with an old receipt, or hand messages back over and over. The
five invariants still have to hold.

**Recovery.** Submit work, kill the process without warning, restart, and check that
acknowledged messages are gone and unacknowledged ones come back.

**Tenant isolation.** Two orgs with identically named queues, identical group keys
and overlapping traffic, running at the same time. Neither may see the other's
messages, and no slot may be shared between them. The same run checks that a
credential for one org cannot reach the other's queues by name.

### What makes it testable

Two decisions exist mostly for this. The clock is supplied rather than read from the
system, so anything time-dependent is deterministic. Background work can be called
directly, so a test runs the real code path for expiring a lease without waiting for
a timer to fire.

---

## 20. Decisions

| Decision | What we chose | What we rejected | Why |
|---|---|---|---|
| **Where a queue lives** | **One machine by default** | Every queue spread across machines | Exact ordering and exact counts, one hop, far less machinery |
| **Spreading a queue** | **A `distributed` flag, default false** | Automatic, or always on | Only the rare queue that outgrows a machine pays the ordering cost |
| Split count in the API | Not exposed | Caller picks a partition count | Forces a guess before anyone has seen traffic |
| Slots per queue | 16, fixed | 64, or sized to the cluster | Enough locks for one machine; caps fan-out for a distributed queue |
| Placement | 4096 placement groups | One map entry per queue | A per-queue map would put millions of entries in a coordination store |
| Slot ownership | One queue per slot | Slots shared across queues | Shared slots would put two tenants behind one lock |
| Message IDs | No location information | ID encodes the slot | Layout must not live in permanent identifiers |
| Acknowledging | By receipt | By message ID alone | An ID cannot be routed, and cannot tell two deliveries apart |
| Ordering scope | Per group, plus exact FIFO on a normal queue | Whole-queue order at any scale | Whole-queue order is not meaningful once workers run in parallel |
| Ordering mechanism | Position in a list within a group; a submission counter across slots | Wall-clock timestamps | A counter is not a clock, so clock differences cannot corrupt order |
| Starvation | A reserved share of capacity | Aging, weighted random | A definite bound an operator can act on |
| Priority selection | Bitmap over levels | Scan or heap | Same speed at 3 levels or 101 |
| Available structure | Lists of groups | Flat list, skip locked ones | One busy group cannot slow everything down |
| Lock granularity | One lock per slot | Finer locks per structure | Handing out a message changes five things together |
| Group locks | Bookkeeping under the slot lock | A real lock per group | A crashed worker would otherwise need recovery machinery |
| Lease deadlines | Never written down | Written to the log | Voided after a crash anyway; avoids a clock-skew surface |
| Attempt counts | Written down | Discarded | Without them a failing message retries forever |
| Storage | Memory plus a write-ahead log | A shared database | Keeps ordering and locking where we can reason about them |
| Tiers | Gateway and node separate | One combined tier | Node restarts cause redeliveries; API changes should not |
| **Metrics** | **Gateway collects from nodes** | **Prometheus scrapes nodes** | Nodes have no public listener, and two aggregation paths can disagree |
| Metric aggregation | Gateway sums; Prometheus queries | Sum in Prometheus | One source of truth for depth, shared with the stats API |
| Queue configs | Postgres | etcd | Changes rarely, has structure, wants backups and history |
| Placement map, membership | etcd | Postgres | Needs watches and keys that expire on their own |
| Who owns metadata | Nodes write, gateways read | Gateways coordinate | Gateways scale up and down too often to hold an election |
| Queue identity | Org plus name | Globally unique names | Tenants pick their own names without collisions |
| Where the org comes from | The credential | The URL path | A name in a path can be edited; a credential cannot |
| Deleting a queue | Two steps: mark, then remove | Delete the row straight away | The deletion has to reach every machine first |
| Existence check on enqueue | Gateway cache, node decides | Only one of the two | Rejects mistakes early without trusting a cache |
| Waiting for work | Notify, then one dequeue | Fan out the dequeue and take the first answer | Fanning out leases messages nobody is processing |

---


## 21. What Others Have Built

Meta's **FOQS** does this at around a trillion items a day, and Ryuk ends up looking
similar: a fixed set of shards each owned by one machine, a service that manages the
assignments, priority selection in memory, integer priorities, delayed delivery, and
lease-based delivery with acknowledgment and negative acknowledgment.

The most useful thing to notice is that **FOQS does not pick messages out of
MySQL.** It keeps an in-memory index of items sorted by priority and merges across
shards in memory. MySQL is the durable store underneath, not the queue itself. So
FOQS is not an argument for building a queue on a database. It is in-memory
priority structures on top of durable storage, which is what Ryuk does, with MySQL
doing the job Ryuk's log does.

MySQL was right for them and would be wrong here. Meta runs one of the largest
MySQL fleets anywhere, with replication, backups, and shard management already
solved. Writing a replicated log would mean redoing work their infrastructure
already does. At their volume the data also does not fit in memory. Neither is true
for us, and keeping storage behind an interface means moving to a database later is
one new implementation, not a redesign.

Other things we borrowed: fixed slots from **Redis Cluster**, **Riak**, and
**Cassandra**; group ordering from **SQS FIFO queues**; the priority bitmap from
operating system schedulers; the reserved share from **Linux real-time throttling**
and network traffic shaping.

---

## 22. Later

Things the design accounts for and the implementation does not do.

**Replication.** The single thing standing between this and surviving the loss of a
machine. The log is already the replication stream — the owner ships records to
followers, waits for a quorum before answering the producer, and a follower is
promoted when the owner is gone. The pieces it needs are the ones deliberately left
out: leader election, a fencing rule so a partitioned owner stops serving, and
promotion on failure. Two cheaper options are worth weighing first: putting the log
on storage that outlives the machine, which needs no protocol at all, or replicating
asynchronously and accepting a small window of loss.

Its absence has one visible consequence today: a placement group is never reassigned
away from a node that still holds its data, so a node that is gone takes its queues
with it until it returns (Section 11).

**Automatic promotion to `distributed`.** The coordinator can already see a queue's
throughput and depth. Noticing that a queue is saturating its machine and setting the
flag itself is a small step, but it means moving live data and changing a queue's
ordering guarantee underneath its callers, which deserves more care than a threshold.

**Quotas per org.** Cluster-wide limits on messages, bytes and request rate. The
mechanism is leased budgets rather than counting: the coordinator hands each machine
a slice of an org's allowance to spend locally, shrinking the slice as the org nears
its limit so that overshoot approaches zero exactly where it matters and coordination
approaches zero everywhere else. Per-queue depth limits are exact today and cover
most of what quotas would.

**Cells.** Grouping machines so orgs in different cells share nothing physical, which
turns tenant isolation from a limit into a boundary. Placement groups make this a
change to one function rather than to the data path: an org's queues are restricted
to the groups its cell owns.

**A sorted index across machines.** Gateways route distributed queues on summaries
that are slightly out of date, so priority across machines is close but not exact.
Maintaining a properly sorted index, as FOQS does, would make it exact at the cost of
continuously merging across machines. The approximation holds well past the point
where other limits bind.

**Storage beyond memory.** Keeping only the front of each priority list in memory
would move the backlog ceiling from memory to disk. The change is contained, because
choosing a message only ever looks at the front of a list.

**Batched submission.** A real throughput win that brings partial failure with it —
some messages in a batch accepted and others rejected — which needs a response shape
that makes that unambiguous.

**Rate limits per group.** Group locks already exist, so they are a natural place to
cap how fast one group can move.

**Aging instead of a reserved share.** Worth revisiting if a ready structure turns up
that can change a message's priority cheaply.