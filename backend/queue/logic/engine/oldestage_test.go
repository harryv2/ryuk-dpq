package engine

import (
	"testing"
	"time"
)

// Spec: the age of the oldest non-expired, non-in-flight message. A redelivered
// group goes to the back of its band, so a band-head scan loses sight of it.
func TestOldestAgeCountsRedeliveredMessages(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("old"), Priority: Medium, GroupID: "A"}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Minute)
	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("new"), Priority: Medium, GroupID: "B"}); err != nil {
		t.Fatal(err)
	}

	if got := q.Stats().OldestAge; got != 10*time.Minute {
		t.Fatalf("before redelivery: OldestAge = %v, want 10m", got)
	}

	m, r, ok := q.Dequeue()
	if !ok || string(m.Payload) != "old" {
		t.Fatalf("expected to dequeue the old message, got %v ok=%v", m, ok)
	}
	if _, err := q.Nack(r, 0); err != nil {
		t.Fatal(err)
	}

	if got := q.Stats().OldestAge; got != 10*time.Minute {
		t.Fatalf("after redelivery: OldestAge = %v, want 10m", got)
	}
}

func TestOldestAgeExcludesInFlight(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	if _, err := q.EnqueueToSlot(0, EnqueueOptions{Payload: []byte("a"), Priority: Medium}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Minute)
	if _, _, ok := q.Dequeue(); !ok {
		t.Fatal("dequeue failed")
	}

	st := q.Stats()
	if st.OldestAge != 0 {
		t.Fatalf("OldestAge = %v, want 0 while the only message is in flight", st.OldestAge)
	}
	if st.InFlight != 1 {
		t.Fatalf("InFlight = %d, want 1", st.InFlight)
	}
}

func TestOldestAgeExcludesExpired(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("doomed"), Priority: Medium, TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Hour)
	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("live"), Priority: Medium}); err != nil {
		t.Fatal(err)
	}

	if got := q.Stats().OldestAge; got != 0 {
		t.Fatalf("OldestAge = %v, want 0: the older message has expired", got)
	}
}

// A group's later messages are neither expired nor in flight, so they age.
func TestOldestAgeCountsMessagesBlockedBehindAGroupHead(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("head"), Priority: Medium, GroupID: "G"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("tail"), Priority: Medium, GroupID: "G"}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Minute)

	if _, _, ok := q.Dequeue(); !ok {
		t.Fatal("dequeue failed")
	}
	if got := q.Stats().OldestAge; got != 30*time.Minute {
		t.Fatalf("OldestAge = %v, want 30m for the sibling still waiting", got)
	}
}
