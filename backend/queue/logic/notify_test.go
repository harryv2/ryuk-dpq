package logic

import (
	"testing"

	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

func told(ch <-chan Notification) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// The node cannot tell which gateway has a consumer parked here, so telling one
// at random leaves the rest asleep until their polls time out.
func TestEnqueueTellsEveryGateway(t *testing.T) {
	l, _ := newTestNode(t)
	spec := testSpec()

	chans := make([]<-chan Notification, 3)
	for i := range chans {
		_, chans[i] = l.Subscribe()
	}

	fill(t, l, spec, 3, 1)

	for i, ch := range chans {
		if !told(ch) {
			t.Fatalf("gateway %d was not told; a consumer parked there waits out its whole poll", i)
		}
	}
}

// A nacked message is available again straight away.
func TestNackTellsTheGateways(t *testing.T) {
	l, _ := newTestNode(t)
	spec := testSpec()
	fill(t, l, spec, 3, 1)

	got, err := l.Dequeue(entity.DequeueRequest{Spec: spec, MaxMessages: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("dequeued %d messages, want 1", len(got.Messages))
	}

	_, ch := l.Subscribe() // after the enqueue, so only the nack can fire it
	if err := l.Nack(entity.NackRequest{Spec: spec, Receipt: got.Messages[0].Receipt}); err != nil {
		t.Fatal(err)
	}
	if !told(ch) {
		t.Fatal("nack put the message back and told nobody")
	}
}

// A handoff lands a whole slot at once on a node nobody has any reason to poll.
func TestAbsorbTellsTheGateways(t *testing.T) {
	l, _ := newTestNode(t)
	spec := testSpec()
	_, ch := l.Subscribe()

	err := l.Absorb(entity.TransferRequest{
		Spec: spec,
		BySlot: map[uint16][]entity.WireMessage{
			3: {entity.ToWire(&engine.Message{ID: "m1", Priority: 1, Seq: 1})},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !told(ch) {
		t.Fatal("a transfer arrived and nobody was told")
	}
}
