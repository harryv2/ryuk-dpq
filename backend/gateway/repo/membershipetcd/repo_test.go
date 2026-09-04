package membershipetcd

import (
	"testing"
	"time"
)

// The rebalancer and the notification subscriber both wait on this. A single
// shared channel delivered to whichever received first, so a member joining
// woke one and the other never learned -- and when the rebalancer lost, a
// distributed queue never spread onto the new node.
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
