package logic

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/mocks"
	"github.com/harryv2/ryuk-dpq/backend/third_party/logger"
	"go.uber.org/mock/gomock"
)

// waitSetup builds a gateway whose queue is owned by a reachable machine.
func waitSetup(t *testing.T, backstop time.Duration) (*GatewayLogic, deps) {
	t.Helper()
	ctrl := gomock.NewController(t)
	d := deps{
		queues:  mocks.NewMockQueueTableRepo(ctrl),
		slots:   mocks.NewMockSlotPlacementTableRepo(ctrl),
		nodes:   mocks.NewMockNodeGRPCRepo(ctrl),
		members: mocks.NewMockMembershipRepo(ctrl),
		series:  mocks.NewMockTimeseriesRepo(ctrl),
	}
	d.members.EXPECT().Lookup("node-1").
		Return(entity.Member{ID: "node-1", Addr: "a:1"}, true).AnyTimes()
	d.members.EXPECT().Members().
		Return([]entity.Member{{ID: "node-1", Addr: "a:1"}}).AnyTimes()
	d.queues.EXPECT().Get(gomock.Any(), "org1", "orders").
		Return(cfgFor("org1", "orders", "node-1"), nil).AnyTimes()

	l := New(Config{CacheTTL: time.Minute, CollectEvery: time.Minute, BackstopPoll: backstop},
		logger.Nop(), d.queues, d.slots, d.nodes, d.members, d.series)
	return l, d
}

// Aiming every wake at the same waiter leaves the rest asleep until their polls
// time out.
func TestWakeReachesEveryWaiter(t *testing.T) {
	w := newWaiters()
	chans := []chan struct{}{w.park("q"), w.park("q"), w.park("q")}

	for i := 0; i < len(chans); i++ {
		w.wake("q")
	}

	woken := 0
	for _, ch := range chans {
		select {
		case <-ch:
			woken++
		default:
		}
	}
	if woken != len(chans) {
		t.Fatalf("%d wakes reached %d of %d waiters", len(chans), woken, len(chans))
	}
}

// A message arriving while the first dequeue is in flight must not be lost.
func TestDequeueRegistersBeforeTheFirstTake(t *testing.T) {
	l, d := waitSetup(t, time.Hour) // long, so only the wake can end this
	var calls atomic.Int32
	d.nodes.EXPECT().Dequeue(gomock.Any(), "a:1", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ entity.QueueSpec, _ int) ([]entity.NodeMessage, error) {
			if calls.Add(1) == 1 {
				l.wait.wake(key("org1", "orders"))
				return nil, nil
			}
			return []entity.NodeMessage{{MessageID: "m1"}}, nil
		}).AnyTimes()

	start := time.Now()
	got, err := l.Dequeue(context.Background(), entity.DequeueRequest{
		Org: "org1", Queue: "orders", MaxMessages: 1, WaitTime: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(got.Messages))
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waited %v for the timeout instead of taking the wake", elapsed)
	}
}

// Losing a notification must cost a tick, not the whole poll.
func TestDequeuePollsWhenNothingWakesIt(t *testing.T) {
	l, d := waitSetup(t, 50*time.Millisecond)
	var calls atomic.Int32
	d.nodes.EXPECT().Dequeue(gomock.Any(), "a:1", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ entity.QueueSpec, _ int) ([]entity.NodeMessage, error) {
			if calls.Add(1) == 1 {
				return nil, nil
			}
			return []entity.NodeMessage{{MessageID: "m1"}}, nil
		}).AnyTimes()

	start := time.Now()
	got, err := l.Dequeue(context.Background(), entity.DequeueRequest{
		Org: "org1", Queue: "orders", MaxMessages: 1, WaitTime: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("got %d messages, want 1; nothing ever woke it", len(got.Messages))
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("took %v, want about one backstop tick", elapsed)
	}
}

// One owner being down does not mean the queue is broken.
func TestDequeueIsEmptyNotFailedWhenOneOwnerIsDown(t *testing.T) {
	ctrl := gomock.NewController(t)
	d := deps{
		queues:  mocks.NewMockQueueTableRepo(ctrl),
		slots:   mocks.NewMockSlotPlacementTableRepo(ctrl),
		nodes:   mocks.NewMockNodeGRPCRepo(ctrl),
		members: mocks.NewMockMembershipRepo(ctrl),
		series:  mocks.NewMockTimeseriesRepo(ctrl),
	}
	d.members.EXPECT().Lookup("node-1").
		Return(entity.Member{ID: "node-1", Addr: "a:1"}, true).AnyTimes()
	d.members.EXPECT().Lookup("node-2").
		Return(entity.Member{ID: "node-2", Addr: "b:2"}, true).AnyTimes()
	d.queues.EXPECT().Get(gomock.Any(), "org1", "events").Return(entity.QueueConfig{
		Org: "org1", Name: "events", State: entity.StateActive, Generation: 1,
		Distributed: true,
		Settings:    entity.QueueSettings{VisibilityTimeout: 30 * time.Second, MaxRetries: 3},
	}, nil).AnyTimes()
	d.slots.EXPECT().ListByQueue(gomock.Any(), "org1", "events").
		Return(map[uint16]string{0: "node-1", 1: "node-2"}, nil).AnyTimes()

	l := New(Config{CacheTTL: time.Minute, CollectEvery: time.Minute},
		logger.Nop(), d.queues, d.slots, d.nodes, d.members, d.series)

	d.nodes.EXPECT().Dequeue(gomock.Any(), "a:1", gomock.Any(), gomock.Any()).
		Return(nil, errors.New("connection refused")).AnyTimes()
	d.nodes.EXPECT().Dequeue(gomock.Any(), "b:2", gomock.Any(), gomock.Any()).
		Return(nil, nil).AnyTimes()

	got, err := l.Dequeue(context.Background(), entity.DequeueRequest{
		Org: "org1", Queue: "events", MaxMessages: 1,
	})
	if err != nil {
		t.Fatalf("one unreachable owner turned an empty queue into an error: %v", err)
	}
	if len(got.Messages) != 0 {
		t.Fatalf("got %d messages, want 0", len(got.Messages))
	}
}
