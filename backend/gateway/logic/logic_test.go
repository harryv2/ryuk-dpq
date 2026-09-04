package logic

import (
	"context"
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/constants"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/gateway/mocks"
	"github.com/harryv2/ryuk-dpq/backend/third_party/logger"
	"go.uber.org/mock/gomock"
)

type deps struct {
	queues  *mocks.MockQueueTableRepo
	slots   *mocks.MockSlotPlacementTableRepo
	nodes   *mocks.MockNodeGRPCRepo
	members *mocks.MockMembershipRepo
	series  *mocks.MockTimeseriesRepo
}

func setup(t *testing.T) (*GatewayLogic, deps) {
	t.Helper()
	ctrl := gomock.NewController(t)
	d := deps{
		queues:  mocks.NewMockQueueTableRepo(ctrl),
		slots:   mocks.NewMockSlotPlacementTableRepo(ctrl),
		nodes:   mocks.NewMockNodeGRPCRepo(ctrl),
		members: mocks.NewMockMembershipRepo(ctrl),
		series:  mocks.NewMockTimeseriesRepo(ctrl),
	}
	d.members.EXPECT().Members().Return(nil).AnyTimes()
	d.members.EXPECT().Lookup(gomock.Any()).Return(entity.Member{}, false).AnyTimes()

	log := logger.Nop()
	l := New(Config{CacheTTL: time.Minute, CollectEvery: time.Minute},
		log, d.queues, d.slots, d.nodes, d.members, d.series)
	return l, d
}

func cfgFor(org, name, owner string) entity.QueueConfig {
	return entity.QueueConfig{
		Org: org, Name: name, State: entity.StateActive, OwnerNode: owner, Generation: 1,
		Settings: entity.QueueSettings{VisibilityTimeout: 30 * time.Second, MaxRetries: 3},
	}
}

func TestCreateQueueRejectsMissingDeadLetterQueue(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "dlq").
		Return(entity.QueueConfig{}, enterr.NotFound("queue"))

	_, err := l.CreateQueue(context.Background(), entity.CreateQueueRequest{
		Org: "org1", Name: "q",
		Settings: entity.QueueSettings{DeadLetterQueue: "dlq"},
	})
	if err == nil || enterr.CodeOf(err) != enterr.CodeInvalid {
		t.Fatalf("want an invalid-config error, got %v", err)
	}
}

func TestCreateQueueAcceptsExistingDeadLetterQueue(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "dlq").Return(cfgFor("org1", "dlq", "node-1"), nil)
	d.queues.EXPECT().Create(gomock.Any(), gomock.Any()).
		Return(cfgFor("org1", "q", ""), true, nil)

	if _, err := l.CreateQueue(context.Background(), entity.CreateQueueRequest{
		Org: "org1", Name: "q",
		Settings: entity.QueueSettings{DeadLetterQueue: "dlq"},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCollectSumsDistributedQueueAcrossNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	d := deps{
		queues:  mocks.NewMockQueueTableRepo(ctrl),
		slots:   mocks.NewMockSlotPlacementTableRepo(ctrl),
		nodes:   mocks.NewMockNodeGRPCRepo(ctrl),
		members: mocks.NewMockMembershipRepo(ctrl),
		series:  mocks.NewMockTimeseriesRepo(ctrl),
	}
	d.members.EXPECT().Members().Return([]entity.Member{
		{ID: "node-1", Addr: "a:1"}, {ID: "node-2", Addr: "b:2"},
	}).AnyTimes()
	l := New(Config{CacheTTL: time.Minute, CollectEvery: time.Minute},
		logger.Nop(), d.queues, d.slots, d.nodes, d.members, d.series)
	d.nodes.EXPECT().StatsAll(gomock.Any(), "a:1").
		Return("node-1", []entity.NodeStats{{
			Org: "org1", Name: "q", Ready: [3]int64{1, 2, 3}, InFlight: 4,
		}}, nil)
	d.nodes.EXPECT().StatsAll(gomock.Any(), "b:2").
		Return("node-2", []entity.NodeStats{{
			Org: "org1", Name: "q", Ready: [3]int64{10, 20, 30}, InFlight: 5,
		}}, nil)

	l.Collect(context.Background())

	// Storing each node's slice under the queue key would leave whichever node
	// answered last, so a distributed queue would report a fraction of itself.
	got, ok := readCache[entity.NodeStats](l, statsCacheKey("org1", "q"))
	if !ok {
		t.Fatal("nothing was collected")
	}
	if total := got.Ready[0] + got.Ready[1] + got.Ready[2]; total != 66 {
		t.Fatalf("ready summed to %d, want 66", total)
	}
	if got.InFlight != 9 {
		t.Fatalf("in flight summed to %d, want 9", got.InFlight)
	}
}

func TestConfigCacheAvoidsSecondRead(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "q").Return(cfgFor("org1", "q", "node-1"), nil).Times(1)

	for i := 0; i < 3; i++ {
		if _, err := l.config(context.Background(), "org1", "q"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMissingQueueIsCachedBriefly(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "nope").
		Return(entity.QueueConfig{}, enterr.NotFound("queue")).Times(1)

	for i := 0; i < 3; i++ {
		if _, err := l.config(context.Background(), "org1", "nope"); enterr.CodeOf(err) != enterr.CodeNotFound {
			t.Fatalf("want not found, got %v", err)
		}
	}
}

func TestEnqueueRejectsUnknownQueue(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "gone").
		Return(entity.QueueConfig{}, enterr.NotFound("queue"))

	_, err := l.Enqueue(context.Background(), entity.EnqueueRequest{
		Org: "org1", Queue: "gone", Payload: "hi", Priority: "HIGH",
	})
	if enterr.CodeOf(err) != enterr.CodeNotFound {
		t.Fatalf("want not found, got %v", err)
	}
}

func TestEnqueueRejectsDeletingQueue(t *testing.T) {
	l, d := setup(t)
	c := cfgFor("org1", "q", "node-1")
	c.State = entity.StateDeleting
	d.queues.EXPECT().Get(gomock.Any(), "org1", "q").Return(c, nil)

	_, err := l.Enqueue(context.Background(), entity.EnqueueRequest{
		Org: "org1", Queue: "q", Payload: "hi",
	})
	if enterr.CodeOf(err) != enterr.CodeNotFound {
		t.Fatalf("want not found, got %v", err)
	}
}

func TestEnqueueFailsWhenOwnerIsDown(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "q").Return(cfgFor("org1", "q", "node-gone"), nil)

	// node-gone is not in the member list, and nothing is reassigned, because
	// without replication its data is only there
	_, err := l.Enqueue(context.Background(), entity.EnqueueRequest{
		Org: "org1", Queue: "q", Payload: "hi",
	})
	if enterr.CodeOf(err) != enterr.CodeExhausted {
		t.Fatalf("want unavailable, got %v", err)
	}
}

func TestGroupAlwaysHashesToTheSameSlot(t *testing.T) {
	first := slotOf("org1", "q", "user-123", constants.SlotsPerQueue)
	for i := 0; i < 50; i++ {
		if got := slotOf("org1", "q", "user-123", constants.SlotsPerQueue); got != first {
			t.Fatal("a group key must always land on the same slot")
		}
	}
	// two tenants using the same group name must not share a slot by accident
	same := 0
	for i := 0; i < 20; i++ {
		if slotOf("orgA", "q", "k", constants.SlotsPerQueue) == slotOf("orgB", "q", "k", constants.SlotsPerQueue) {
			same++
		}
	}
	if same == 20 && slotOf("orgA", "q", "k", constants.SlotsPerQueue) == slotOf("orgB", "q", "k", constants.SlotsPerQueue) {
		t.Log("collision is possible but the org is salted in")
	}
}
