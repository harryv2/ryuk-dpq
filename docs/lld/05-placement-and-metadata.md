# LLD 5 — Placement and Metadata

Implements HLD §6, §7, §13.

| Concern | Where |
|---|---|
| `Member`, `OwnerFor`, `OwnerKey` | `gateway/entity/model-member.go` — pure, no dependencies |
| `MembershipRepo` interfaces | `gateway/entity`, `queue/entity` |
| etcd, watching | `gateway/repo/membershipetcd` |
| etcd, registering | `queue/repo/membershipetcd` |
| Queue settings and placement | `gateway/repo/queueconfigpg` |

Placement is domain logic, so the model and the hashing live in `entity` where
they can be tested with no etcd at all. Only the client is a repo. The node
registers and the gateway watches, so each side implements the half it needs
rather than sharing one client.

Two stores, because two kinds of state with different needs. **etcd** holds who is
alive. **Postgres** holds queue settings and where each queue actually is.

---

## 1. Membership in etcd

```
/ryuk/membershipRepo/{nodeID}  →  {"id":"node-abc","addr":"1c0d5eb6:9090"}   lease TTL 10s
```

```go
lease, _ := c.cli.Grant(ctx, ttl)
c.cli.Put(ctx, prefix+m.ID, body, clientv3.WithLease(lease.ID))
ch, _ := c.cli.KeepAlive(ctx, lease.ID)
```

Stop renewing — crash, kill, scale down — and the key disappears. That is the whole
liveness mechanism: no heartbeat table, no reaper, no judgement call about what
counts as stale.

Two things etcd gives that a database does not:

**Watches.** A membership change is pushed. Polling would mean every gateway
querying constantly and learning nothing most of the time, and a change would take
a poll interval to reach everyone.

**Keys that expire on their own.** The TTL is the liveness check.

```go
type MembershipRepo interface {           // gateway: read only
    Watch(ctx context.Context) error
    Members() []Member                    // sorted, so callers are deterministic
    Lookup(id string) (Member, bool)
    Changed() <-chan struct{}
}

type MembershipRepo interface {           // node: write only
    Register(ctx context.Context, id, addr string, ttlSeconds int64) error
    Close() error
}
```

`Members()` returns a sorted slice so two components computing placement from the
same set get the same answer regardless of map iteration order.

---

## 2. Placement is a pure function

```go
func OwnerFor(key string, membershipRepo []Member) (Member, bool) {
    kh := hash64(key)
    for _, m := range membershipRepo {
        if s := mix(kh, hash64(m.ID)); !found || s > bestScore {
            best, bestScore, found = m, s, true
        }
    }
    return best, found
}
```

**The mixing step is not decoration.** Hashing `key + id` in one FNV pass looks
fine and is badly broken: FNV processes bytes in order, so ids differing only in
their last byte — `node-1`, `node-2`, `node-3` — produce correlated scores and one
member wins far more than its share. A test that placed 200 keys over 5 membershipRepo
caught it: one member took 99 of them.

Running the combination through a splitmix64 finaliser avalanches every input bit
across the output, and the spread evens out.

This is **rendezvous hashing**. Every gateway and every node computes the same
owner from the same member list, which is why there is no coordinator, no election
and no placement map to keep consistent.

Adding a machine moves only the keys that machine now wins — about `1/(n+1)` of
them. A plain `hash % nodeCount` would move nearly everything on every change.

```go
func OwnerKey(org, name string, slot int, distributed bool) string {
    k := org + "/" + name
    if !distributed { return k }
    return k + "/slot-" + itoa(slot)
}
```

A normal queue places as a whole; a distributed queue places each slot on its own.
That one difference is what spreads a distributed queue over up to sixty-four
machines.

---

## 3. Why placement is also stored

If placement is a pure function, why write it down?

**Because the hash is a function of who is alive, and ownership has to follow the
data.**

```
node-2 dies holding 40,000 messages

  the hash now says   → node-4 owns it
  reality says        → the messages are on node-2's disk
```

Recomputing on every request would hand the queue to node-4, which would create an
empty one and start accepting messages while the real ones sat on a disk nobody
was reading. Group ordering would split across two machines and the depth metric
would read zero.

So the hash decides where something goes **when it moves**, and the stored record
says where it **is**. They agree except while a migration is in flight, which is
exactly what makes rebalancing observable rather than magic.

---

## 4. Postgres schema

```sql
CREATE TABLE queues (
    org_id      TEXT   NOT NULL,
    name        TEXT   NOT NULL,
    settings    JSONB  NOT NULL,
    distributed BOOLEAN NOT NULL DEFAULT false,
    state       TEXT   NOT NULL DEFAULT 'active',   -- active | migrating | deleting
    owner_node  TEXT,                               -- normal queues
    generation  BIGINT NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, name)
);

CREATE TABLE slot_placement (                       -- distributed queues
    org_id     TEXT     NOT NULL,
    queue_name TEXT     NOT NULL,
    slot       SMALLINT NOT NULL,
    owner_node TEXT     NOT NULL,
    generation BIGINT   NOT NULL DEFAULT 1,
    PRIMARY KEY (org_id, queue_name, slot)
);
```

**The primary key is the pair, not the name.** Two orgs can both have a queue
called `orders` and they are unrelated, which is what lets tenants pick their own
names without coordinating.

The same key does the concurrency work:

```sql
INSERT INTO queues (...) VALUES (...)
ON CONFLICT (org_id, name) DO NOTHING
RETURNING true
```

No rows returned means somebody else won. The caller reads the existing row and
returns 200 if the settings match or 409 if they do not.

**Two tables because there are two placement units.** A normal queue is placed as
a whole, so its owner is one column. A distributed queue is placed per slot, so it
gets sixty-four rows. Storing sixteen identical rows for a normal queue would be
recording the same fact sixteen times.

---

## 5. Generations

Every change of ownership increments a counter, and it is prefixed onto message
sequence numbers:

```go
Seq = (generation << 40) | counter
```

Sequence numbers are what cross-slot FIFO ordering compares, so two owners handing
out overlapping values would corrupt it. The prefix makes them monotonic across
owners with no coordination.

It also makes the merge in HLD §12 correct: a returning node's messages carry an
older generation, so they sort **before** anything the new owner accepted and are
inserted in the right place rather than appended behind it.

---

## 6. One writer per migration

```go
q := `UPDATE queues SET state=$3 WHERE org_id=$1 AND name=$2`
if state == entity.StateMigrating {
    q += ` AND state='active'`
}
if tag.RowsAffected() == 0 { return enterr.New(CodeConflict, "already migrating") }
```

Whichever gateway flips the state owns the move; the others get a conflict and
skip it. This is the only coordination between gateways in the system, and it is a
single conditional update rather than a lock.

---

## 7. Deleting a queue

Two steps, because the deletion has to reach the owner before the name is free.

```
1. state = 'deleting'   gateways stop accepting within a cache refresh
2. drop on the owner    the messages are discarded
3. delete the rows      the name is free, within that org
```

Deleting discards the messages, as SQS does. Waiting for consumers to drain would
mean a delete that never finishes when nobody is consuming.

---

## 8. When a store is down

Neither store is on the path a message takes.

| | Postgres down | etcd down |
|---|---|---|
| Enqueue, dequeue, acknowledge | Work, from cached configs | Work, from the cached member list |
| Creating or changing a queue | Blocked | Works |
| Rebalancing | Blocked | Blocked |
| Nodes joining | Works, unregistered | Blocked |

The exception is a cold start: a gateway with an empty cache cannot serve until it
can read configs.

---

## 9. Tests

`model-member_test.go` runs against no infrastructure, because placement is a
pure function:

| Test | What it pins down |
|---|---|
| `TestOwnerIsStableForTheSameMemberList` | The same list always gives the same owner |
| `TestOwnerSpreadsAcrossMembers` | 200 keys over 5 membershipRepo land in a band, not on one |
| `TestAddingAMemberMovesFewKeys` | Adding one of six moves under 30%, not most |
| `TestRemovingAMemberOnlyMovesItsKeys` | A member leaving never disturbs another's keys |
| `TestOwnerKeyDistinguishesSlots` | A normal queue places whole, a distributed one per slot |

The second and third are the ones that matter. Without the spread test the
correlated-hash bug above would have shipped, and without the movement test a
regression to `hash % nodeCount` would look correct while moving everything on
every membership change.

---

## 10. Deviations from the HLD

**No coordinator election.** The HLD describes an elected coordinator writing a
placement map. Rendezvous hashing removed the need: placement is deterministic, so
nothing has to be decided. The election would come back for **load-aware**
placement, where the choice is no longer a pure function of the member list.

**Rebalancing runs on the gateway, not the node.** The HLD has each node work out
what it should give up. Ownership records live in Postgres, which only the gateway
talks to, so driving it from there avoids giving nodes a database connection. The
outcome is the same because both compute the same target.
