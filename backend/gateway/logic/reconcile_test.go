package logic

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/gateway/mocks"
	"github.com/harryv2/ryuk-dpq/backend/third_party/logger"
	"go.uber.org/mock/gomock"
)

// The shared setup pins Members() to an empty cluster, which is the one thing
// the reconciler needs to differ.
func setupWithMembers(t *testing.T, ms []entity.Member) (*GatewayLogic, deps) {
	t.Helper()
	ctrl := gomock.NewController(t)
	d := deps{
		queues:  mocks.NewMockQueueTableRepo(ctrl),
		slots:   mocks.NewMockSlotPlacementTableRepo(ctrl),
		nodes:   mocks.NewMockNodeGRPCRepo(ctrl),
		members: mocks.NewMockMembershipRepo(ctrl),
		series:  mocks.NewMockTimeseriesRepo(ctrl),
	}
	d.members.EXPECT().Members().Return(ms).AnyTimes()
	l := New(Config{CacheTTL: time.Minute, CollectEvery: time.Minute},
		logger.Nop(), d.queues, d.slots, d.nodes, d.members, d.series)
	return l, d
}

func distributedCfg() entity.QueueConfig {
	c := cfgFor("org1", "orders", "")
	c.Distributed = true
	c.Settings.PlacementWidth = 4
	return c
}

// The move committed but the old owner never got the call to let go, so it is
// still sitting on data someone else now serves.
func TestReconcileDropsSlotsANodeNoLongerOwns(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}, {ID: "node-2", Addr: "n2:9090"}}
	l, d := setupWithMembers(t, ms)

	d.queues.EXPECT().ListAll(gomock.Any()).Return([]entity.QueueConfig{distributedCfg()}, nil)
	d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "orders").
		Return(map[uint16]string{3: "node-2", 4: "node-1"}, nil)

	d.nodes.EXPECT().Held(gomock.Any(), "n1:9090").Return(entity.HeldResponse{
		NodeID: "node-1",
		Queues: []entity.HeldSlots{{Org: "org1", Name: "orders", Slots: []uint16{3, 4}}},
	}, nil)
	d.nodes.EXPECT().Held(gomock.Any(), "n2:9090").Return(entity.HeldResponse{NodeID: "node-2"}, nil)

	d.nodes.EXPECT().
		DiscardMove(gomock.Any(), "n1:9090", gomock.Any(), []uint16{3}, "").
		Return(nil)

	l.Reconcile(context.Background())
}

// The move never committed, so the slots have to go back into service on the
// node that still owns them.
func TestReconcileThawsSlotsAStalledMoveLeftFrozen(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)

	d.queues.EXPECT().ListAll(gomock.Any()).Return([]entity.QueueConfig{distributedCfg()}, nil)
	d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "orders").
		Return(map[uint16]string{3: "node-1"}, nil)

	d.nodes.EXPECT().Held(gomock.Any(), "n1:9090").Return(entity.HeldResponse{
		NodeID: "node-1",
		Queues: []entity.HeldSlots{{
			Org: "org1", Name: "orders", Slots: []uint16{3}, Frozen: []uint16{3},
		}},
	}, nil)

	d.nodes.EXPECT().
		AbortMove(gomock.Any(), "n1:9090", gomock.Any(), []uint16{3}, "").
		Return(nil)

	l.Reconcile(context.Background())
}

// Placement agreeing with what the node holds is the normal case, and it must
// not touch anything.
func TestReconcileLeavesAHealthyClusterAlone(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)

	d.queues.EXPECT().ListAll(gomock.Any()).Return([]entity.QueueConfig{distributedCfg()}, nil)
	d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "orders").
		Return(map[uint16]string{3: "node-1", 4: "node-1"}, nil)
	d.nodes.EXPECT().Held(gomock.Any(), "n1:9090").Return(entity.HeldResponse{
		NodeID: "node-1",
		Queues: []entity.HeldSlots{{Org: "org1", Name: "orders", Slots: []uint16{3, 4}}},
	}, nil)

	l.Reconcile(context.Background())
}

// A node that cannot be reached is not evidence of anything, so nothing is
// repaired on its behalf.
func TestReconcileSkipsUnreachableNodes(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)

	d.queues.EXPECT().ListAll(gomock.Any()).Return([]entity.QueueConfig{distributedCfg()}, nil)
	d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "orders").
		Return(map[uint16]string{3: "node-2"}, nil)
	d.nodes.EXPECT().Held(gomock.Any(), "n1:9090").
		Return(entity.HeldResponse{}, context.DeadlineExceeded)

	l.Reconcile(context.Background())
}

// A distributed queue's counts are per node, so the page has to show this
// node's share rather than the queue's total.
func TestNodeDetailsReportsOnlyThisNodesShare(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)
	d.members.EXPECT().Lookup("node-1").Return(ms[0], true)

	d.queues.EXPECT().ListByOrg(gomock.Any(), "org1").
		Return([]entity.QueueConfig{distributedCfg()}, nil)
	d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "orders").
		Return(map[uint16]string{3: "node-1", 4: "node-1", 5: "node-2"}, nil)
	d.nodes.EXPECT().StatsAll(gomock.Any(), "n1:9090").Return("node-1",
		[]entity.NodeStats{
			{Org: "org1", Name: "orders", Ready: [3]int64{1, 2, 3}, InFlight: 4},
			{Org: "other", Name: "orders", Ready: [3]int64{9, 9, 9}},
		}, nil)

	out, err := l.GetNodeDetails(context.Background(), "org1", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Queues) != 1 {
		t.Fatalf("reported %d queues, want only this org's", len(out.Queues))
	}
	if out.Queues[0].Slots != 2 {
		t.Fatalf("slots here = %d, want 2 of the 3 placed", out.Queues[0].Slots)
	}
	if out.Ready != 6 || out.InFlight != 4 {
		t.Fatalf("totals wrong: ready %d, in flight %d", out.Ready, out.InFlight)
	}
}

// A node that is registered but not answering still has placement to show.
func TestNodeDetailsFallsBackToPlacementWhenTheNodeIsSilent(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)
	d.members.EXPECT().Lookup("node-1").Return(ms[0], true)

	d.queues.EXPECT().ListByOrg(gomock.Any(), "org1").
		Return([]entity.QueueConfig{distributedCfg()}, nil)
	d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "orders").
		Return(map[uint16]string{3: "node-1"}, nil)
	d.nodes.EXPECT().StatsAll(gomock.Any(), "n1:9090").
		Return("", nil, context.DeadlineExceeded)

	out, err := l.GetNodeDetails(context.Background(), "org1", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if out.Live {
		t.Fatal("a node that did not answer must not read as live")
	}
	if len(out.Queues) != 1 || out.Queues[0].Slots != 1 {
		t.Fatalf("placement was not reported: %+v", out.Queues)
	}
}

func TestNodeDetailsRejectsAnUnknownNode(t *testing.T) {
	l, d := setupWithMembers(t, []entity.Member{{ID: "node-1", Addr: "n1:9090"}})
	d.members.EXPECT().Lookup("ghost").Return(entity.Member{}, false)

	if _, err := l.GetNodeDetails(context.Background(), "org1", "ghost"); enterr.CodeOf(err) != enterr.CodeNotFound {
		t.Fatalf("want a not-found error, got %v", err)
	}
}

// A queue whose machines are gone must still appear, or the cluster view is
// silent about exactly the thing an operator needs to see.
func TestClusterReportsSlotsWhoseOwnerIsGone(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)

	d.queues.EXPECT().ListAll(gomock.Any()).Return([]entity.QueueConfig{distributedCfg()}, nil)
	d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "orders").
		Return(map[uint16]string{3: "node-1", 4: "gone", 5: "gone"}, nil)

	out, err := l.GetClusterDetails(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Nodes) != 1 || len(out.Nodes[0].Queues) != 1 || out.Nodes[0].Queues[0].Slots != 1 {
		t.Fatalf("live placement wrong: %+v", out.Nodes)
	}
	if len(out.Unavailable) != 1 || out.Unavailable[0].Slots != 2 {
		t.Fatalf("slots on the departed machine were not reported: %+v", out.Unavailable)
	}
}

// The registry view exists to make a disagreement visible, so the count the
// gateway believes has to come back alongside what etcd holds.
func TestRegistryReportsEtcdAndWhatTheGatewayBelieves(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)

	d.members.EXPECT().Entries(gomock.Any()).Return([]entity.RegistryEntry{
		{Key: "/ryuk/members/node-1", Value: `{"id":"node-1"}`, TTLSeconds: 7},
		{Key: "/ryuk/members/node-2", Value: `{"id":"node-2"}`, TTLSeconds: 3},
	}, nil)

	out, err := l.GetRegistry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Entries) != 2 {
		t.Fatalf("returned %d entries, want both", len(out.Entries))
	}
	// etcd holds two, the gateway is tracking one: that gap is the point.
	if out.Watching != 1 {
		t.Fatalf("watching = %d, want the gateway's own count", out.Watching)
	}
}

func singleNodeCfg(owner string) entity.QueueConfig {
	c := cfgFor("org1", "payments", owner)
	c.Distributed = false
	return c
}

// A whole-queue move must hold a copy on the old owner until placement has
// moved.
func TestWholeQueueMoveKeepsACopyUntilItCommits(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}, {ID: "node-2", Addr: "n2:9090"}}
	l, d := setupWithMembers(t, ms)

	d.queues.EXPECT().SetState(gomock.Any(), "org1", "payments", entity.StateMigrating).Return(nil)
	d.queues.EXPECT().SetState(gomock.Any(), "org1", "payments", entity.StateActive).Return(nil)

	gomock.InOrder(
		d.nodes.EXPECT().
			PrepareMove(gomock.Any(), "n1:9090", gomock.Any(), gomock.Len(16), gomock.Any()).
			Return(entity.Transfer{}, nil),
		d.nodes.EXPECT().Absorb(gomock.Any(), "n2:9090", gomock.Any()).Return(nil),
		d.queues.EXPECT().SetOwner(gomock.Any(), "org1", "payments", "node-2", uint64(2)).Return(nil),
		d.nodes.EXPECT().
			DiscardMove(gomock.Any(), "n1:9090", gomock.Any(), gomock.Len(16), gomock.Any()).
			Return(nil),
	)

	if err := l.migrate(context.Background(), singleNodeCfg("node-1"), ms[0], ms[1]); err != nil {
		t.Fatal(err)
	}
}

// If the new owner cannot take them, the old owner has to go back to serving.
func TestWholeQueueMovePutsItBackWhenAbsorbFails(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}, {ID: "node-2", Addr: "n2:9090"}}
	l, d := setupWithMembers(t, ms)

	d.queues.EXPECT().SetState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
	d.nodes.EXPECT().PrepareMove(gomock.Any(), "n1:9090", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.Transfer{}, nil)
	d.nodes.EXPECT().Absorb(gomock.Any(), "n2:9090", gomock.Any()).Return(context.DeadlineExceeded)

	// the repair: thaw, not discard
	d.nodes.EXPECT().AbortMove(gomock.Any(), "n1:9090", gomock.Any(), gomock.Len(16), gomock.Any()).Return(nil)

	if err := l.migrate(context.Background(), singleNodeCfg("node-1"), ms[0], ms[1]); err == nil {
		t.Fatal("a failed absorb must not report success")
	}
}

// An interrupted whole-queue move leaves slots frozen. Nothing else thaws them,
// so the queue would serve nothing until the process restarted.
func TestReconcileThawsAnInterruptedWholeQueueMove(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)

	d.queues.EXPECT().ListAll(gomock.Any()).Return([]entity.QueueConfig{singleNodeCfg("node-1")}, nil)
	d.nodes.EXPECT().Held(gomock.Any(), "n1:9090").Return(entity.HeldResponse{
		NodeID: "node-1",
		Queues: []entity.HeldSlots{{
			Org: "org1", Name: "payments", Slots: []uint16{0, 1}, Frozen: []uint16{0, 1},
		}},
	}, nil)

	d.nodes.EXPECT().
		AbortMove(gomock.Any(), "n1:9090", gomock.Any(), []uint16{0, 1}, "").
		Return(nil)

	l.Reconcile(context.Background())
}

// The draining handoff must not come back. Freeze empties the old owner before
// the new one has confirmed, which is the whole bug.
func TestWholeQueueMoveNeverDrainsTheOldOwnerFirst(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}, {ID: "node-2", Addr: "n2:9090"}}
	l, d := setupWithMembers(t, ms)

	d.queues.EXPECT().SetState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	d.queues.EXPECT().SetOwner(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	d.nodes.EXPECT().PrepareMove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.Transfer{}, nil).AnyTimes()
	d.nodes.EXPECT().Absorb(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	d.nodes.EXPECT().DiscardMove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()

	// Times(0): the draining call must never be reached.
	d.nodes.EXPECT().Freeze(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.Transfer{}, nil).Times(0)

	if err := l.migrate(context.Background(), singleNodeCfg("node-1"), ms[0], ms[1]); err != nil {
		t.Fatal(err)
	}
}

func dlqCfg(name, dlq string) entity.QueueConfig {
	c := cfgFor("org1", name, "node-1")
	c.Settings.DeadLetterQueue = dlq
	return c
}

// The node holds what it gave up on; the gateway is the only one that knows
// where it should go, so it pulls, enqueues, and only then lets the node drop it.
func TestDeadLettersAreMovedIntoTheConfiguredQueue(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)

	d.nodes.EXPECT().DeadLetters(gomock.Any(), "n1:9090").Return([]entity.DeadLetterItem{{
		Org: "org1", Name: "orders",
		Message: entity.NodeMessage{MessageID: "m1", Payload: []byte("x"), Priority: 75},
	}}, nil)

	// routing needs the source queue's config, then the target's
	d.queues.EXPECT().Get(gomock.Any(), "org1", "orders").Return(dlqCfg("orders", "orders-dlq"), nil)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "orders-dlq").Return(cfgFor("org1", "orders-dlq", "node-1"), nil)
	d.members.EXPECT().Lookup("node-1").Return(ms[0], true).AnyTimes()

	// it lands in the dead-letter queue as an ordinary message
	d.nodes.EXPECT().
		Enqueue(gomock.Any(), "n1:9090", gomock.Any()).
		Return(entity.NodeEnqueueResult{MessageID: "m2"}, nil)

	// and only now is the node told to let go
	d.nodes.EXPECT().AckDeadLetters(gomock.Any(), "n1:9090", []string{"m1"}).Return(nil)

	l.RouteDeadLetters(context.Background())
}

// If the move fails the node keeps the message, so it is tried again rather
// than being dropped on the floor.
func TestAFailedMoveLeavesTheDeadLetterOnTheNode(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)

	d.nodes.EXPECT().DeadLetters(gomock.Any(), "n1:9090").Return([]entity.DeadLetterItem{{
		Org: "org1", Name: "orders",
		Message: entity.NodeMessage{MessageID: "m1", Payload: []byte("x")},
	}}, nil)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "orders").Return(dlqCfg("orders", "orders-dlq"), nil)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "orders-dlq").Return(cfgFor("org1", "orders-dlq", "node-1"), nil)
	d.members.EXPECT().Lookup("node-1").Return(ms[0], true).AnyTimes()
	d.nodes.EXPECT().Enqueue(gomock.Any(), "n1:9090", gomock.Any()).
		Return(entity.NodeEnqueueResult{}, context.DeadlineExceeded)

	// Times(0): nothing may be dropped while it has not landed.
	d.nodes.EXPECT().AckDeadLetters(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(0)

	l.RouteDeadLetters(context.Background())
}

// A queue that failures are routed to cannot be deleted out from under them.
func TestAQueueUsedAsADeadLetterQueueCannotBeDeleted(t *testing.T) {
	l, d := setupWithMembers(t, []entity.Member{{ID: "node-1", Addr: "n1:9090"}})

	d.queues.EXPECT().Get(gomock.Any(), "org1", "orders-dlq").
		Return(cfgFor("org1", "orders-dlq", "node-1"), nil)
	d.queues.EXPECT().ListByOrg(gomock.Any(), "org1").Return([]entity.QueueConfig{
		dlqCfg("orders", "orders-dlq"),
		dlqCfg("payments", "orders-dlq"),
		cfgFor("org1", "orders-dlq", "node-1"),
	}, nil)

	// A conflict with existing state, not a malformed request, so the caller
	// sees 409 like any other "that does not fit what is already here".
	err := l.DeleteQueue(context.Background(), "org1", "orders-dlq")
	if enterr.CodeOf(err) != enterr.CodeConflict {
		t.Fatalf("want a conflict, got %v", err)
	}
	if !strings.Contains(err.Error(), "orders") || !strings.Contains(err.Error(), "payments") {
		t.Fatalf("the error should name what depends on it, got: %v", err)
	}
}

// A move that commits while a pass is running must not look like a leftover.
// The nodes are asked first and placement read afterwards, so placement is
// never older than the reports it is compared against. Read the other way
// round, the node found holding the slot is the new owner and the repair
// discards the messages the move had just delivered.
func TestReconcileReadsPlacementAfterTheNodesReport(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}, {ID: "node-2", Addr: "n2:9090"}}
	l, d := setupWithMembers(t, ms)

	// node-1 has let slot 3 go, node-2 is serving it.
	held1 := d.nodes.EXPECT().Held(gomock.Any(), "n1:9090").Return(entity.HeldResponse{
		NodeID: "node-1",
		Queues: []entity.HeldSlots{{Org: "org1", Name: "orders"}},
	}, nil)
	held2 := d.nodes.EXPECT().Held(gomock.Any(), "n2:9090").Return(entity.HeldResponse{
		NodeID: "node-2",
		Queues: []entity.HeldSlots{{Org: "org1", Name: "orders", Slots: []uint16{3}}},
	}, nil)

	list := d.queues.EXPECT().ListAll(gomock.Any()).
		Return([]entity.QueueConfig{distributedCfg()}, nil)
	place := d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "orders").
		Return(map[uint16]string{3: "node-2"}, nil)

	gomock.InOrder(held1, held2, list, place)

	// Placement and the holder agree, so nothing is repaired.
	d.nodes.EXPECT().
		DiscardMove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)

	l.Reconcile(context.Background())
}

// The rebalancer marks a queue migrating for the length of a handoff: its slots
// are meant to be frozen and its placement is still moving. Thawing them here
// puts the old owner back to serving messages the new owner is about to serve
// as well.
func TestReconcileLeavesAMigratingQueueAlone(t *testing.T) {
	ms := []entity.Member{{ID: "node-1", Addr: "n1:9090"}}
	l, d := setupWithMembers(t, ms)

	migrating := distributedCfg()
	migrating.State = entity.StateMigrating
	d.queues.EXPECT().ListAll(gomock.Any()).Return([]entity.QueueConfig{migrating}, nil)
	d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "orders").
		Return(map[uint16]string{3: "node-1"}, nil)

	// PrepareMove has frozen slot 3; the handoff is in flight.
	d.nodes.EXPECT().Held(gomock.Any(), "n1:9090").Return(entity.HeldResponse{
		NodeID: "node-1",
		Queues: []entity.HeldSlots{{
			Org: "org1", Name: "orders", Slots: []uint16{3}, Frozen: []uint16{3},
		}},
	}, nil)

	d.nodes.EXPECT().
		AbortMove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)
	d.nodes.EXPECT().
		DiscardMove(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)

	l.Reconcile(context.Background())
}
