# LLD 5 — Metadata

Queue configuration in Postgres, placement and membership in etcd, and the
coordinator that keeps them consistent.

Implements: HLD Sections 5, 9.

---

## 1. What lives where

| Data | Store | Why there |
|---|---|---|
| Queue configs, org quotas, credentials | **Postgres** | Rarely changes, has structure, wants backups and history |
| Placement map | **etcd** | Everyone must hear about a change at once |
| Membership | **etcd** | Liveness is a key with a TTL the node refreshes |
| Coordinator election | **etcd** | Needs the same lease mechanism |

Neither store is on the path a message takes.

---

## 2. Postgres schema

```sql
CREATE TABLE queues (
    queue_id    BIGSERIAL,                    -- immutable, used for file paths
    org_id      TEXT NOT NULL,
    name        TEXT NOT NULL,
    settings    JSONB NOT NULL,
    state       TEXT NOT NULL DEFAULT 'active',   -- active | deleting
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, name),
    UNIQUE (queue_id)
);

CREATE TABLE queue_history (
    queue_id    BIGINT NOT NULL,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    changed_by  TEXT NOT NULL,
    old_settings JSONB,
    new_settings JSONB
);

CREATE TABLE org_quotas (
    org_id       TEXT PRIMARY KEY,
    max_queues   INT    NOT NULL DEFAULT 100,
    max_messages BIGINT NOT NULL DEFAULT 10000000,
    max_bytes    BIGINT NOT NULL DEFAULT 10737418240,
    max_rps      INT    NOT NULL DEFAULT 1000
);

CREATE TABLE credentials (
    key_hash    BYTEA PRIMARY KEY,            -- never the credential itself
    org_id      TEXT NOT NULL,
    cross_org   BOOLEAN NOT NULL DEFAULT false,
    revoked_at  TIMESTAMPTZ
);
```

Three things about this schema.

**The primary key is `(org_id, name)`, not the name.** Two orgs can both have a queue
called `orders`. That is what lets tenants pick names without coordinating, and it is
also what makes concurrent creation safe — two gateways creating the same queue for
the same org cannot both win.

**`queue_id` is separate and immutable.** It is what the write-ahead log uses for
directory names (LLD 2 §2), so no tenant-chosen string ever reaches the filesystem.
It also means renaming a queue, if we ever allow it, touches no files.

**Credentials are stored hashed.** The plaintext never enters the database, a log
line, or an error message.

### Creating a queue

```sql
INSERT INTO queues (org_id, name, settings)
VALUES ($1, $2, $3)
ON CONFLICT (org_id, name) DO NOTHING
RETURNING queue_id;
```

No rows returned means it already exists. The caller reads the existing row and
returns 200 if the settings match, 409 if they do not. The database does the
concurrency control, so it does not matter which gateway issues the statement.

### Deleting a queue

Two steps (HLD §5):

```sql
-- step 1
UPDATE queues SET state = 'deleting', updated_at = now()
WHERE org_id = $1 AND name = $2;

-- step 2, once no slots remain
DELETE FROM queues WHERE queue_id = $1;
```

Between the two, gateways reject new messages within a cache refresh while nodes
discard the queue's messages and release its slots. The coordinator watches for slots
reaching zero and issues the delete.

If a node is down holding one of those slots, the delete waits. When the node returns,
or when its slots are reassigned, the new owner sees `deleting` and releases. The
process always finishes.

---

## 3. etcd layout

```
/ryuk/members/<node-id>          lease-bound, refreshed every 3s, TTL 10s
    { addr, zone, capacity, started_at, version }

/ryuk/placement/version          monotonic integer
/ryuk/placement/<pg-id>          { leader, followers[], state }

/ryuk/coordinator                lease-bound, held by one node

/ryuk/config-version             bumped on any queue config change
```

Membership keys are bound to a lease the node refreshes. Stop refreshing — because
the process died, or the machine did, or the network partitioned — and the key
disappears on its own. No reaper, no judgement call about what counts as stale.

`/ryuk/placement/version` exists so a watcher can tell whether it is behind without
comparing four thousand entries.

`/ryuk/config-version` is the optional fast path for config invalidation (LLD 4 §4).
Configs live in Postgres; this is a signal to re-read, not a copy.

---

## 4. Coordinator

One node holds `/ryuk/coordinator` on a lease. If it dies, the lease expires and
another node takes it. Nothing else changes — the coordinator is a role, not a
separate service (HLD §5).

```go
func (c *Coordinator) run(ctx context.Context) error {
    sess, err := concurrency.NewSession(c.etcd, concurrency.WithTTL(10))
    if err != nil { return err }
    defer sess.Close()

    el := concurrency.NewElection(sess, "/ryuk/coordinator")
    if err := el.Campaign(ctx, c.nodeID); err != nil { return err }

    for {
        select {
        case <-ctx.Done():       return ctx.Err()
        case <-sess.Done():      return errLostLeadership   // step down immediately
        case ev := <-c.membership:
            c.onMembershipChange(ev)
        case <-c.rebalanceTick:
            c.maybeRebalance()
        }
    }
}
```

`sess.Done()` firing means the lease was lost, which means someone else may already
be coordinating. The response is to stop, not to finish the current operation.

### Assignment

```go
func (c *Coordinator) assign(members []Member) map[PG]Placement {
    out := make(map[PG]Placement, NumPlacementGroups)
    for pg := PG(0); pg < NumPlacementGroups; pg++ {
        ranked := rendezvousRank(pg, members)       // deterministic
        leader := ranked[0]
        followers := pickFollowers(ranked[1:], leader.Zone, replicationFactor-1)
        out[pg] = Placement{Leader: leader.ID, Followers: followers}
    }
    return c.balance(out, members)                  // adjust for load and zones
}

func rendezvousRank(pg PG, members []Member) []Member {
    sort.Slice(members, func(i, j int) bool {
        return hash(pg, members[i].ID) > hash(pg, members[j].ID)
    })
    return members
}
```

**Rendezvous hashing is the base** because of one property: adding a node moves only
the placement groups that node now wins, roughly `1/(n+1)` of them. Nothing else
shuffles. A naive `pg % nodeCount` would move nearly everything on every membership
change.

**Then the coordinator adjusts** for what a hash cannot know: observed load rather
than group count, and keeping a group's replicas out of one zone. The result is
written explicitly so nobody recomputes and disagrees.

### Rebalancing

```go
func (c *Coordinator) maybeRebalance() {
    if time.Since(c.lastChange) < rebalanceCooldown { return }   // 30s
    cur, want := c.current(), c.assign(c.members())
    moves := diff(cur, want)
    if len(moves) == 0 || imbalance(cur) < minImbalance { return }
    c.execute(moves[:min(len(moves), maxConcurrentMoves)])       // 8 at a time
}
```

The cooldown and the imbalance threshold exist because a flapping node would
otherwise cause continuous data movement, which hurts far more than the imbalance it
is correcting. The concurrency cap exists because moving everything at once saturates
the network and makes the cluster slower than the imbalance did.

### Moving a placement group

```
1. Mark the group MIGRATING in etcd, naming both nodes
2. For each slot in the group, one at a time:
     a. losing node freezes the slot
     b. losing node ships the snapshot
     c. gaining node loads it and voids inherited leases
     d. losing node releases the slot
3. Mark the group ACTIVE with the new leader; bump the placement version
```

Ownership flips per group, but data moves **per slot**, so the freeze window is one
slot rather than a whole group (HLD §9). The losing node keeps serving slots it has
not shipped yet.

The `MIGRATING` state makes the in-between observable. Without it, a gateway seeing
an inconsistent answer could not tell a migration from a bug.

If either node dies mid-move, the step is idempotent: the losing node still owns
un-shipped slots and retries elsewhere, or the gaining node's replica is promoted.

---

## 5. Membership

```go
func (n *Node) registerLiveness(ctx context.Context) error {
    lease, err := n.etcd.Grant(ctx, 10)                 // 10s TTL
    if err != nil { return err }
    key := "/ryuk/members/" + string(n.id)
    if _, err := n.etcd.Put(ctx, key, n.describe(), clientv3.WithLease(lease.ID)); err != nil {
        return err
    }
    ch, err := n.etcd.KeepAlive(ctx, lease.ID)
    if err != nil { return err }

    for range ch { /* refreshed */ }
    return errLostLiveness       // channel closed: we are no longer a member
}
```

`errLostLiveness` is not a warning. A node whose lease expired must **stop serving**,
because the coordinator may already have reassigned its slots and a second owner would
hand out the same messages. This is the local half of the split-brain defence in
HLD §11.

Timings: refresh every 3 seconds against a 10-second TTL, so two consecutive failures
are tolerated before a node is declared gone. Detection therefore takes up to 10
seconds, and the election that follows adds a couple more.

---

## 6. The client both tiers use

```go
type Client struct {
    pg   *pgxpool.Pool
    etcd *clientv3.Client

    placement atomic.Pointer[PlacementMap]
    configs   *configCache
}

func (c *Client) watchPlacement(ctx context.Context) {
    pm, rev := c.loadPlacement(ctx)
    c.placement.Store(pm)

    for resp := range c.etcd.Watch(ctx, "/ryuk/placement/", clientv3.WithPrefix(), clientv3.WithRev(rev+1)) {
        if resp.CompactRevision != 0 {          // we fell too far behind
            pm, rev = c.loadPlacement(ctx)      // full reload
            c.placement.Store(pm)
            continue
        }
        c.placement.Store(apply(c.placement.Load(), resp.Events))
    }
}
```

Two details that matter for correctness.

**Watching resumes from a revision**, so a brief disconnect replays what was missed
rather than silently skipping it.

**A compaction is handled by reloading everything.** If we fell behind further than
etcd's history, incremental updates would produce a map with holes. Detecting that and
reloading is the difference between a slow gateway and a wrong one.

The map is swapped atomically, so readers never take a lock (LLD 4 §5.2).

---

## 7. When a store is down

| | Postgres down | etcd down |
|---|---|---|
| Enqueue, dequeue, acknowledge | Work, from cached configs | Work, from the cached placement map |
| Creating or changing a queue | Blocked | Works |
| A queue not in any cache | 503 | Unaffected |
| Rebalancing | Works | Blocked |
| Nodes joining or leaving | Works | Blocked |
| Coordinator election | Unaffected | Blocked; the current one keeps its lease until it expires |

Neither store is on the message path, so an outage costs changes rather than traffic.
The exception is a cold start: a gateway with an empty cache cannot serve until it can
read configs.

This is the difference from putting messages in a shared database. There, the store is
on every request. Here, losing it means the cluster cannot be *changed*, not that it
cannot be *used*.

---

## 8. Quota enforcement

| Limit | Where | Exact |
|---|---|---|
| Queues per org | Postgres count at creation | Yes |
| Requests per second | Gateway token bucket, share per gateway | No |
| Messages and bytes per org | Node, against a periodically shared total | No |
| Depth per queue | Node, counted where the messages are | Yes |

```go
// each node publishes its own contribution; the total is the sum of what it sees
func (q *quotaTracker) Admit(org string, bytes int) error {
    u := q.usage.Load(org)                  // refreshed every 5s from peers
    if u.Messages+q.localDelta(org) >= q.limit(org).MaxMessages {
        return queue.ErrQuotaExceeded
    }
    return nil
}
```

Volume quotas are approximate on purpose (HLD §9). An exact global count would need
every node to agree before accepting a submission, which is a coordination round trip
on the hot path to enforce a capacity-planning limit. An org can exceed its quota for
a few seconds. Depth per queue stays exact because a queue's slots have a bounded set
of owners.

---

## 9. Edge cases

| Case | Behaviour |
|---|---|
| Two gateways create the same queue | Primary key decides; loser reads back, 200 or 409 |
| Two orgs create the same queue name | Both succeed; different rows, different slots |
| Coordinator dies mid-rebalance | New one reads the `MIGRATING` states and resumes |
| Node dies mid-migration | Step is idempotent; retried or the replica promotes |
| Node's lease expires but the process lives | It stops serving and re-registers |
| etcd watch falls behind a compaction | Full reload rather than an incremental patch |
| Placement version goes backwards | Ignored; a stale watcher event |
| Queue deleted while a node is offline | Node sees `deleting` at startup and releases |
| Credential revoked | `revoked_at` set; auth cache expires within 60s |
| Quota lowered below current usage | New submissions rejected; nothing already accepted is dropped |
| Postgres failover | Connections re-established; cached configs cover the gap |
