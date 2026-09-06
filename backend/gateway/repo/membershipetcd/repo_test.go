package membershipetcd

import (
	"context"
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/third_party/logger"
)

// The rebalancer and the notification subscriber both wait on this.
func TestEveryListenerIsNotified(t *testing.T) {
	r := &Repo{}
	a, b, c := r.Changed(), r.Changed(), r.Changed()

	r.notify()

	for i, ch := range []<-chan struct{}{a, b, c} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("listener %d was not notified", i)
		}
	}
}

// A listener that has not drained its last signal already knows the membership
// moved, so notify must not block on it.
func TestNotifyDoesNotBlockOnASlowListener(t *testing.T) {
	r := &Repo{}
	slow := r.Changed()
	fast := r.Changed()

	r.notify() // fills both buffers
	r.notify() // must not block even though `slow` is unread

	select {
	case <-fast:
	default:
		t.Fatal("the fast listener lost its signal")
	}
	select {
	case <-slow:
	default:
		t.Fatal("the slow listener lost its signal")
	}
}

// After a gap in the watch, a node that left while the stream was down is
// absent from the fresh read.
func TestResyncDropsAMemberThatLeftDuringTheGap(t *testing.T) {
	r := &Repo{members: map[string]entity.Member{
		"node-1": {ID: "node-1", Addr: "n1:9090"},
		"node-2": {ID: "node-2", Addr: "n2:9090"},
	}}
	changed := r.Changed()

	r.setMembers(map[string]entity.Member{"node-1": {ID: "node-1", Addr: "n1:9090"}})

	if _, ok := r.Lookup("node-2"); ok {
		t.Fatal("a member missing from the snapshot was kept")
	}
	if got := len(r.Members()); got != 1 {
		t.Fatalf("member count is %d, want 1", got)
	}
	select {
	case <-changed:
	default:
		t.Fatal("a resync must wake the rebalancer; the cluster it plans against just changed")
	}
}

// A watch that ends must not leave the goroutine dead. The loop only returns
// when the context does.
func TestFollowStopsOnlyWhenTheContextEnds(t *testing.T) {
	r := &Repo{members: map[string]entity.Member{}, log: logger.Nop()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() { r.follow(ctx, 1); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("follow did not return after the context was cancelled")
	}
}
