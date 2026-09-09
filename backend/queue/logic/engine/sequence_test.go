package engine

import (
	"fmt"
	"testing"
	"time"
)

// A restart replays the log into a fresh queue, which knows nothing of the
// numbering it is inheriting. Without adopting it, new messages get sequence
// numbers below the recovered ones and are served first at the same priority.
func TestRecoveredMessagesKeepTheirPlaceInLine(t *testing.T) {
	clk := NewFakeClock()
	before := New(Config{
		Key: QueueKey{Org: "o", Name: "q"}, VisibilityTimeout: time.Minute,
		MaxRetries: 3, MaxRetriesSet: true,
	}, clk, nil, 7) // generation 7, as placement would say

	for i := 0; i < 5; i++ {
		if _, err := before.EnqueueToSlot(uint16(i), EnqueueOptions{
			Payload: []byte(fmt.Sprintf("old-%d", i)), Priority: Medium,
		}); err != nil {
			t.Fatal(err)
		}
	}
	replayed, err := before.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// What Recover does: a fresh queue built from an empty spec, so generation 0.
	after := New(Config{
		Key: QueueKey{Org: "o", Name: "q"}, VisibilityTimeout: time.Minute,
		MaxRetries: 3, MaxRetriesSet: true,
	}, clk, nil, 0)
	if err := after.Absorb(replayed); err != nil {
		t.Fatal(err)
	}
	if got := after.Generation(); got != 7 {
		t.Errorf("generation after recovery is %d, want the 7 the messages carry", got)
	}

	for i := 0; i < 5; i++ {
		if _, err := after.EnqueueToSlot(uint16(i), EnqueueOptions{
			Payload: []byte(fmt.Sprintf("new-%d", i)), Priority: Medium,
		}); err != nil {
			t.Fatal(err)
		}
	}

	var order []string
	for {
		m, r, ok := after.Dequeue()
		if !ok {
			break
		}
		order = append(order, string(m.Payload))
		if err := after.Ack(r); err != nil {
			t.Fatal(err)
		}
	}
	if len(order) != 10 {
		t.Fatalf("delivered %d of 10: %v", len(order), order)
	}
	for i, p := range order[:5] {
		if p[:3] != "old" {
			t.Fatalf("position %d is %q; every recovered message should come first: %v", i, p, order)
		}
	}
}

// A migration numbers the receiver above the source, so absorbed messages must
// keep their place ahead and the receiver's numbering must not drop to theirs.
func TestAMigrationKeepsOlderMessagesAhead(t *testing.T) {
	clk := NewFakeClock()
	mk := func(gen uint64) *Queue {
		return New(Config{
			Key: QueueKey{Org: "o", Name: "q"}, VisibilityTimeout: time.Minute,
			MaxRetries: 3, MaxRetriesSet: true,
		}, clk, nil, gen)
	}

	src := mk(4)
	for i := 0; i < 5; i++ {
		if _, err := src.EnqueueToSlot(uint16(i), EnqueueOptions{
			Payload: []byte(fmt.Sprintf("old-%d", i)), Priority: Medium,
		}); err != nil {
			t.Fatal(err)
		}
	}

	dst := mk(5) // the gateway numbers the new owner one higher
	if err := dst.Absorb(src.FreezeSlots([]uint16{0, 1, 2, 3, 4})); err != nil {
		t.Fatal(err)
	}
	if got := dst.Generation(); got != 5 {
		t.Fatalf("absorbing older messages dropped the generation to %d, want 5", got)
	}
	for i := 0; i < 5; i++ {
		if _, err := dst.EnqueueToSlot(uint16(i), EnqueueOptions{
			Payload: []byte(fmt.Sprintf("new-%d", i)), Priority: Medium,
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertOldFirst(t, dst, "migration to a higher generation")
}

// The destination may already hold the queue at a lower generation than the
// messages arriving. Its numbering has to move up, not stay under them.
func TestAbsorbingNewerMessagesRaisesTheNumbering(t *testing.T) {
	clk := NewFakeClock()
	mk := func(gen uint64) *Queue {
		return New(Config{
			Key: QueueKey{Org: "o", Name: "q"}, VisibilityTimeout: time.Minute,
			MaxRetries: 3, MaxRetriesSet: true,
		}, clk, nil, gen)
	}

	src := mk(9)
	for i := 0; i < 5; i++ {
		if _, err := src.EnqueueToSlot(uint16(i), EnqueueOptions{
			Payload: []byte(fmt.Sprintf("old-%d", i)), Priority: Medium,
		}); err != nil {
			t.Fatal(err)
		}
	}

	dst := mk(2) // already running, behind the messages it is about to take
	if err := dst.Absorb(src.FreezeSlots([]uint16{0, 1, 2, 3, 4})); err != nil {
		t.Fatal(err)
	}
	if got := dst.Generation(); got != 9 {
		t.Fatalf("generation is %d after absorbing generation-9 messages, want 9", got)
	}
	for i := 0; i < 5; i++ {
		if _, err := dst.EnqueueToSlot(uint16(i), EnqueueOptions{
			Payload: []byte(fmt.Sprintf("new-%d", i)), Priority: Medium,
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertOldFirst(t, dst, "absorb into an older queue")
}

func assertOldFirst(t *testing.T, q *Queue, label string) {
	t.Helper()
	var order []string
	for {
		m, r, ok := q.Dequeue()
		if !ok {
			break
		}
		order = append(order, string(m.Payload))
		if err := q.Ack(r); err != nil {
			t.Fatal(err)
		}
	}
	if len(order) != 10 {
		t.Fatalf("%s: delivered %d of 10: %v", label, len(order), order)
	}
	for i, p := range order[:5] {
		if p[:3] != "old" {
			t.Fatalf("%s: position %d is %q, older messages must come first: %v", label, i, p, order)
		}
	}
}

// Recovery rebuilds a queue from an empty spec, so it starts believing it holds
// sixteen slots. A distributed queue holds sixty-four, and until it learns that
// every enqueue to a slot above fifteen is refused.
func TestARecoveredQueueLearnsItIsDistributed(t *testing.T) {
	q := New(Config{Key: QueueKey{Org: "o", Name: "q"}}, NewFakeClock(), nil, 0)

	if _, err := q.EnqueueToSlot(40, EnqueueOptions{Payload: []byte("x"), Priority: Medium}); err == nil {
		t.Fatal("slot 40 was accepted before the queue knew it was distributed")
	}

	// the first request carries the shape from the queue's row
	q.Reconfigure(Config{VisibilityTimeout: time.Minute, Distributed: true,
		MaxRetries: 3, MaxRetriesSet: true})

	if got := q.slotCount(); got != MaxSlotsPerQueue {
		t.Fatalf("slot count is %d after learning the shape, want %d", got, MaxSlotsPerQueue)
	}
	if _, err := q.EnqueueToSlot(40, EnqueueOptions{Payload: []byte("x"), Priority: Medium}); err != nil {
		t.Fatalf("slot 40 still refused after the shape arrived: %v", err)
	}
}

// The shape may be learned but never unset, or a stray spec would shrink a
// distributed queue and strand every slot above fifteen.
func TestAQueueNeverForgetsItIsDistributed(t *testing.T) {
	q := New(Config{Key: QueueKey{Org: "o", Name: "q"}, Distributed: true},
		NewFakeClock(), nil, 0)

	q.Reconfigure(Config{VisibilityTimeout: time.Minute}) // no shape named

	if got := q.slotCount(); got != MaxSlotsPerQueue {
		t.Fatalf("slot count dropped to %d, want %d", got, MaxSlotsPerQueue)
	}
}
