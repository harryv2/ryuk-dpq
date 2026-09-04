package logic

import (
	"context"
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
