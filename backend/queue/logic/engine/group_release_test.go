package engine

import (
	"testing"
	"time"
)

// A group whose in-flight message is dead-lettered or expires keeps its
// remaining messages. They have to go back into a band, or nothing dispatches
// them.
func TestGroupStaysServableAfterHeadDeadLetters(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) {
		c.MaxRetries = 1
		c.HasDeadLetter = true
		c.StarvationReserve = 0
	})

	for i := 0; i < 3; i++ {
		if _, err := q.EnqueueToSlot(0, EnqueueOptions{
			Payload: []byte("x"), Priority: High, GroupID: "g"}); err != nil {
			t.Fatal(err)
		}
	}

	_, r, ok := q.Dequeue()
	if !ok {
		t.Fatal("first dequeue failed")
	}
	dead, err := q.Nack(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dead == nil {
		t.Fatal("expected the message to dead-letter")
	}

	if _, _, ok := q.Dequeue(); !ok {
		t.Fatal("the group's remaining messages became undeliverable")
	}
}

func TestGroupStaysServableAfterHeadExpires(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("doomed"), Priority: High, GroupID: "g", TTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("live"), Priority: High, GroupID: "g"}); err != nil {
		t.Fatal(err)
	}

	_, r, ok := q.Dequeue()
	if !ok {
		t.Fatal("dequeue failed")
	}
	clk.Advance(2 * time.Second)
	if _, err := q.Nack(r, 0); err != nil {
		t.Fatal(err)
	}

	m, _, ok := q.Dequeue()
	if !ok {
		t.Fatal("the sibling became undeliverable after the head expired")
	}
	if string(m.Payload) != "live" {
		t.Fatalf("got %q, want \"live\"", m.Payload)
	}
}

// The same applies when the lease runs out rather than being nacked.
func TestGroupStaysServableAfterLeaseExpiryDeadLetters(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) {
		c.MaxRetries = 1
		c.HasDeadLetter = true
		c.VisibilityTimeout = time.Second
		c.StarvationReserve = 0
	})

	for i := 0; i < 3; i++ {
		if _, err := q.EnqueueToSlot(0, EnqueueOptions{
			Payload: []byte("x"), Priority: High, GroupID: "g"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, ok := q.Dequeue(); !ok {
		t.Fatal("dequeue failed")
	}

	clk.Advance(2 * time.Second)
	if res := q.Sweep(); len(res.DeadLettered) != 1 {
		t.Fatalf("dead-lettered %d, want 1", len(res.DeadLettered))
	}

	if _, _, ok := q.Dequeue(); !ok {
		t.Fatal("the group's remaining messages became undeliverable")
	}
}

// A nack with a delay parks the message; the group's other messages must not be
// parked with it.
func TestGroupStaysServableAfterDelayedNack(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("first"), Priority: High, GroupID: "g"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("second"), Priority: High, GroupID: "g"}); err != nil {
		t.Fatal(err)
	}

	_, r, ok := q.Dequeue()
	if !ok {
		t.Fatal("dequeue failed")
	}
	if _, err := q.Nack(r, time.Minute); err != nil {
		t.Fatal(err)
	}

	m, _, ok := q.Dequeue()
	if !ok {
		t.Fatal("the sibling was parked along with the delayed message")
	}
	if string(m.Payload) != "second" {
		t.Fatalf("got %q, want \"second\"", m.Payload)
	}
}
