package engine

import (
	"fmt"
	"testing"
	"time"
)

func newTestQueue(t *testing.T, mut ...func(*Config)) (*Queue, *FakeClock) {
	t.Helper()
	cfg := Config{
		Key:                 QueueKey{Org: "org1", Name: "q"},
		VisibilityTimeout:   30 * time.Second,
		MaxRetries:          3,
		StarvationThreshold: time.Minute,
		StarvationReserve:   0.2,
	}
	for _, f := range mut {
		f(&cfg)
	}
	clk := NewFakeClock()
	return New(cfg, clk, NewLocalCluster(), NoopJournal{}, 1), clk
}

func enq(t *testing.T, q *Queue, p Priority, group string) *Message {
	t.Helper()
	m, err := q.Enqueue(EnqueueOptions{Payload: []byte("x"), Priority: p, GroupID: group})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return m
}

func mustDequeue(t *testing.T, q *Queue) (*Message, Receipt) {
	t.Helper()
	m, r, ok := q.Dequeue()
	if !ok {
		t.Fatal("expected a message")
	}
	return m, r
}

func TestPriorityOrder(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	lo := enq(t, q, Low, "")
	hi := enq(t, q, High, "")
	mid := enq(t, q, Medium, "")

	for _, want := range []*Message{hi, mid, lo} {
		got, _ := mustDequeue(t, q)
		if got.ID != want.ID {
			t.Fatalf("want priority %d, got %d", want.Priority, got.Priority)
		}
	}
}

func TestFIFOWithinPriority(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	var want []string
	for i := 0; i < 50; i++ {
		want = append(want, enq(t, q, Medium, "").ID)
	}
	for i, id := range want {
		got, _ := mustDequeue(t, q)
		if got.ID != id {
			t.Fatalf("position %d: FIFO broken", i)
		}
	}
}

func TestGroupOrderAndLock(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	a1 := enq(t, q, Medium, "g")
	a2 := enq(t, q, Medium, "g")
	b1 := enq(t, q, Medium, "h")

	// only one message per group is in flight, so g yields a1 then h yields b1
	got1, r1 := mustDequeue(t, q)
	got2, _ := mustDequeue(t, q)
	if got1.ID != a1.ID || got2.ID != b1.ID {
		t.Fatalf("expected a1 then b1, got %v then %v", got1.GroupID, got2.GroupID)
	}
	if _, _, ok := q.Dequeue(); ok {
		t.Fatal("a2 must wait until g is unlocked")
	}

	if err := q.Ack(r1); err != nil {
		t.Fatalf("ack: %v", err)
	}
	got3, _ := mustDequeue(t, q)
	if got3.ID != a2.ID {
		t.Fatal("a2 should follow once the group unlocked")
	}
}

func TestAckRemoves(t *testing.T) {
	q, _ := newTestQueue(t)
	enq(t, q, Medium, "")

	_, r := mustDequeue(t, q)
	if err := q.Ack(r); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := q.Ack(r); err != ErrNotInFlight {
		t.Fatalf("second ack: want ErrNotInFlight, got %v", err)
	}
	if s := q.Stats(); s.ReadyTotal() != 0 || s.InFlight != 0 {
		t.Fatalf("queue not empty: %+v", s)
	}
}

func TestVisibilityTimeoutRedelivers(t *testing.T) {
	q, clk := newTestQueue(t)
	m := enq(t, q, Medium, "")

	_, r1 := mustDequeue(t, q)
	if _, _, ok := q.Dequeue(); ok {
		t.Fatal("in-flight message must be invisible")
	}

	clk.Advance(31 * time.Second)
	q.Sweep()

	again, r2 := mustDequeue(t, q)
	if again.ID != m.ID {
		t.Fatal("expected the same message back")
	}
	if again.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", again.Attempts)
	}

	// the first receipt is stale now
	if err := q.Ack(r1); err != ErrLeaseExpired {
		t.Fatalf("stale ack: want ErrLeaseExpired, got %v", err)
	}
	if err := q.Ack(r2); err != nil {
		t.Fatalf("current ack: %v", err)
	}
}

func TestDeadLetterAfterMaxRetries(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.MaxRetries = 2 })
	m := enq(t, q, Medium, "")

	for i := 0; i < 2; i++ {
		mustDequeue(t, q)
		clk.Advance(31 * time.Second)
		res := q.Sweep()
		if i == 1 {
			if len(res.DeadLettered) != 1 || res.DeadLettered[0].ID != m.ID {
				t.Fatalf("expected dead-letter on attempt 2, got %+v", res)
			}
			return
		}
		if len(res.DeadLettered) != 0 {
			t.Fatalf("dead-lettered too early on attempt %d", i)
		}
	}
}

func TestRetryKeepsGroupOrder(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })
	a1 := enq(t, q, Medium, "g")
	a2 := enq(t, q, Medium, "g")

	// take a1 and let it time out; it must come back ahead of a2
	mustDequeue(t, q)
	clk.Advance(31 * time.Second)
	q.Sweep()

	got, _ := mustDequeue(t, q)
	if got.ID != a1.ID {
		t.Fatal("retried message must stay at the front of its group")
	}
	_ = a2
}

func TestTTLExpiry(t *testing.T) {
	q, clk := newTestQueue(t)
	if _, err := q.Enqueue(EnqueueOptions{Priority: Medium, TTL: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(11 * time.Second)

	if _, _, ok := q.Dequeue(); ok {
		t.Fatal("expired message must not be delivered")
	}
	if s := q.Stats(); s.Expired != 1 {
		t.Fatalf("expired = %d, want 1", s.Expired)
	}
}

func TestExpiredInFlightStillAcks(t *testing.T) {
	q, clk := newTestQueue(t)
	if _, err := q.Enqueue(EnqueueOptions{Priority: Medium, TTL: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	_, r := mustDequeue(t, q)

	clk.Advance(11 * time.Second) // expires while the worker holds it
	if err := q.Ack(r); err != nil {
		t.Fatalf("ack of an expired in-flight message: %v", err)
	}
}

func TestDelayedDelivery(t *testing.T) {
	q, clk := newTestQueue(t)
	if _, err := q.Enqueue(EnqueueOptions{Priority: Medium, DeliverAfter: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := q.Dequeue(); ok {
		t.Fatal("delayed message must not be visible yet")
	}

	clk.Advance(6 * time.Second)
	q.Sweep()
	if _, _, ok := q.Dequeue(); !ok {
		t.Fatal("delayed message should be visible after its release time")
	}
}

func TestNackReturnsImmediately(t *testing.T) {
	q, _ := newTestQueue(t)
	m := enq(t, q, Medium, "")

	_, r := mustDequeue(t, q)
	if _, err := q.Nack(r, 0); err != nil {
		t.Fatalf("nack: %v", err)
	}
	again, _ := mustDequeue(t, q)
	if again.ID != m.ID {
		t.Fatal("nacked message should be available at once")
	}
}

func TestStarvationReserve(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) {
		c.StarvationThreshold = 10 * time.Second
		c.StarvationReserve = 0.5 // every second delivery
	})

	low := enq(t, q, Low, "")
	clk.Advance(11 * time.Second) // low is now starved

	for i := 0; i < 20; i++ {
		enq(t, q, High, "")
	}

	seen := false
	for i := 0; i < 6; i++ {
		m, r := mustDequeue(t, q)
		if m.ID == low.ID {
			seen = true
			break
		}
		_ = q.Ack(r)
	}
	if !seen {
		t.Fatal("starved low-priority message never served")
	}
}

func TestNoStarvationEscapeWhenNothingIsStuck(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) {
		c.StarvationThreshold = time.Hour
		c.StarvationReserve = 0.5
	})
	enq(t, q, Low, "")
	for i := 0; i < 5; i++ {
		enq(t, q, High, "")
	}
	// nothing has waited past the threshold, so priority is strict
	for i := 0; i < 5; i++ {
		m, _ := mustDequeue(t, q)
		if m.Priority != High {
			t.Fatal("departed from priority with nothing starved")
		}
	}
}

func TestIncarnationRejectsOldReceipt(t *testing.T) {
	q, _ := newTestQueue(t)
	enq(t, q, Medium, "")
	_, r := mustDequeue(t, q)

	r.Incarnation++ // as if issued by a different run
	if err := q.Ack(r); err != ErrLeaseExpired {
		t.Fatalf("want ErrLeaseExpired, got %v", err)
	}
}

func TestMaxDepth(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.MaxDepth = 2 })
	enq(t, q, Medium, "")
	enq(t, q, Medium, "")
	if _, err := q.Enqueue(EnqueueOptions{Priority: Medium}); err != ErrQueueFull {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
}

func TestBadPriority(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue(EnqueueOptions{Priority: 200}); err != ErrBadPriority {
		t.Fatalf("want ErrBadPriority, got %v", err)
	}
}

func TestStatsCounts(t *testing.T) {
	q, _ := newTestQueue(t)
	enq(t, q, High, "")
	enq(t, q, Medium, "")
	enq(t, q, Low, "")

	s := q.Stats()
	if s.Ready[0] != 1 || s.Ready[1] != 1 || s.Ready[2] != 1 {
		t.Fatalf("ready buckets = %v", s.Ready)
	}
	if s.ReadyTotal() != 3 {
		t.Fatalf("ready total = %d", s.ReadyTotal())
	}

	_, r := mustDequeue(t, q)
	s = q.Stats()
	if s.InFlight != 1 || s.ReadyTotal() != 2 {
		t.Fatalf("after dequeue: %+v", s)
	}
	_ = q.Ack(r)
	if s = q.Stats(); s.InFlight != 0 || s.Acked != 1 {
		t.Fatalf("after ack: %+v", s)
	}
}

func TestOldestAge(t *testing.T) {
	q, clk := newTestQueue(t)
	enq(t, q, Medium, "")
	clk.Advance(42 * time.Second)
	if got := q.Stats().OldestAge; got != 42*time.Second {
		t.Fatalf("oldest age = %v, want 42s", got)
	}
}

// A migration or a replay moves messages that were already submitted. Counting
// them as new enqueues on the receiving owner makes the handoff look like a
// burst of traffic, and any rate derived from the counter shows a spike that
// never happened.
func TestAbsorbDoesNotCountAsNewEnqueues(t *testing.T) {
	from, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })
	for i := 0; i < 6; i++ {
		enq(t, from, Priority(i*15), fmt.Sprintf("g%d", i%3))
	}
	if _, err := from.Enqueue(EnqueueOptions{
		Payload: []byte("x"), Priority: High, GroupID: "later", DeliverAfter: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	if s := from.Stats(); s.Enqueued != 7 {
		t.Fatalf("the submitting owner counted %d enqueues, want 7", s.Enqueued)
	}

	// What a handoff actually looks like: one owner freezes, another absorbs.
	held := from.Freeze()
	to, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })
	if err := to.Absorb(held); err != nil {
		t.Fatal(err)
	}

	got := to.Stats()
	if got.Enqueued != 0 {
		t.Fatalf("the receiving owner counted %d enqueues; a handed-over message "+
			"was never submitted to it", got.Enqueued)
	}
	if got.ReadyTotal() != 6 || got.Delayed != 1 {
		t.Fatalf("messages lost or duplicated in the handoff: %+v", got)
	}

	// And it still serves them.
	drained := 0
	for {
		_, r, ok := to.Dequeue()
		if !ok {
			break
		}
		if err := to.Ack(r); err != nil {
			t.Fatal(err)
		}
		drained++
	}
	if drained != 6 {
		t.Fatalf("drained %d of the 6 ready messages", drained)
	}
}

// Priority is a number, not three levels: two values inside the same metrics
// band still come out in the right order.
func TestCustomPrioritiesAreOrderedExactly(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	// all three land in the HIGH band, and 100 > 91 > 70
	for _, p := range []Priority{70, 100, 91} {
		enq(t, q, p, "")
	}
	for _, want := range []Priority{100, 91, 70} {
		m, r := mustDequeue(t, q)
		if m.Priority != want {
			t.Fatalf("got priority %d, want %d", m.Priority, want)
		}
		if err := q.Ack(r); err != nil {
			t.Fatal(err)
		}
	}
}

// The bands metrics report are a summary of the same scale.
func TestBandsCoverTheWholeScale(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })
	for _, p := range []Priority{0, 33, 34, 66, 67, 100} {
		enq(t, q, p, "")
	}
	s := q.Stats()
	if s.Ready[0] != 2 || s.Ready[1] != 2 || s.Ready[2] != 2 {
		t.Fatalf("bands are %v; 0-33, 34-66 and 67-100 should hold two each", s.Ready)
	}
	if s.ReadyTotal() != 6 {
		t.Fatalf("total %d, want 6", s.ReadyTotal())
	}
}

// A handoff must not remove anything until the new owner has it. FreezeSlots
// takes copies and holds the slot out of service; if the move never completes,
// thawing puts it back exactly as it was.
func TestFreezeSlotsKeepsTheMessages(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })
	for i := 0; i < 12; i++ {
		enq(t, q, Priority(i*8), fmt.Sprintf("g%d", i%3))
	}
	before := q.Stats()

	held := q.FreezeSlots(allSlots(q))
	moved := 0
	for _, ms := range held {
		moved += len(ms)
	}
	if moved != 12 {
		t.Fatalf("snapshot carried %d messages, want 12", moved)
	}

	// still here, just not being served
	if got := q.Stats(); got.ReadyTotal() != before.ReadyTotal() {
		t.Fatalf("freezing removed messages: %d -> %d", before.ReadyTotal(), got.ReadyTotal())
	}
	if _, _, ok := q.Dequeue(); ok {
		t.Fatal("a frozen slot was served")
	}
	if _, err := q.Enqueue(EnqueueOptions{Payload: []byte("x"), Priority: High}); err != ErrFrozen {
		t.Fatalf("enqueue to a frozen slot returned %v, want ErrFrozen", err)
	}

	// abandon the move: everything comes back
	q.ThawSlots(allSlots(q))
	drained := 0
	for {
		_, r, ok := q.Dequeue()
		if !ok {
			break
		}
		if err := q.Ack(r); err != nil {
			t.Fatal(err)
		}
		drained++
	}
	if drained != 12 {
		t.Fatalf("after thawing, drained %d of 12", drained)
	}
}

// And once the new owner is serving, dropping releases them here.
func TestDropSlotsRemovesThem(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })
	for i := 0; i < 8; i++ {
		enq(t, q, High, fmt.Sprintf("g%d", i))
	}
	ids := allSlots(q)
	q.FreezeSlots(ids)

	if n := q.DropSlots(ids); n != 8 {
		t.Fatalf("dropped %d, want 8", n)
	}
	if s := q.Stats(); s.ReadyTotal() != 0 || s.InFlight != 0 {
		t.Fatalf("slots still hold something after being dropped: %+v", s)
	}
	// dropping thaws, so the slot can be handed back later
	if _, err := q.Enqueue(EnqueueOptions{Payload: []byte("x"), Priority: High}); err != nil {
		t.Fatalf("a dropped slot should accept work again: %v", err)
	}
}

// Freezing one slot must not stop the others.
func TestFreezingOneSlotLeavesTheRestServing(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })
	for slot := 0; slot < 4; slot++ {
		if _, err := q.EnqueueToSlot(uint16(slot), EnqueueOptions{
			Payload: []byte(fmt.Sprintf("s%d", slot)), Priority: High,
		}); err != nil {
			t.Fatal(err)
		}
	}
	q.FreezeSlots([]uint16{1})

	seen := map[string]bool{}
	for {
		m, r, ok := q.Dequeue()
		if !ok {
			break
		}
		seen[string(m.Payload)] = true
		_ = q.Ack(r)
	}
	if seen["s1"] {
		t.Fatal("the frozen slot was served")
	}
	for _, want := range []string{"s0", "s2", "s3"} {
		if !seen[want] {
			t.Fatalf("slot %s stopped serving when a different slot froze", want)
		}
	}
	if held := q.FrozenSlots(); len(held) != 1 || held[0] != 1 {
		t.Fatalf("FrozenSlots reported %v, want [1]", held)
	}
}

func allSlots(q *Queue) []uint16 {
	out := make([]uint16, q.slotCount())
	for i := range out {
		out[i] = uint16(i)
	}
	return out
}

// Settings arrive with every request, so the engine has to tell a real change
// from a spec that simply left blanks for it to default.
func TestReconfigureIgnoresARepeatOfTheSameSettings(t *testing.T) {
	q, _ := newTestQueue(t)
	same := q.Config()
	if q.Reconfigure(same) {
		t.Fatal("re-applying the current settings counted as a change")
	}
	// A blank threshold is not a different threshold; it is the same default.
	blank := same
	blank.StarvationThreshold = 0
	if q.Reconfigure(blank) {
		t.Fatal("a field left for the engine to default counted as a change")
	}
}

func TestReconfigureAppliesToTheNextDeliveryOnly(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.VisibilityTimeout = time.Minute })
	enq(t, q, High, "")
	enq(t, q, High, "")

	// Taken under the old timeout.
	_, first := mustDequeue(t, q)

	c := q.Config()
	c.VisibilityTimeout = 10 * time.Second
	if !q.Reconfigure(c) {
		t.Fatal("a changed timeout should count as a change")
	}
	_, second := mustDequeue(t, q)

	// Past the new timeout, short of the old one: only the second comes back.
	clk.Advance(30 * time.Second)
	if res := q.Sweep(); res.Redelivered != 1 {
		t.Fatalf("redelivered %d, want only the message taken under the new timeout", res.Redelivered)
	}
	if err := q.Ack(first); err != nil {
		t.Fatalf("the in-flight message lost the deadline it was given: %v", err)
	}
	_ = second
}

// The retry limit is read when a delivery fails, not when it is handed out, so
// lowering it reaches messages that are already in flight.
func TestLoweringRetriesAppliesToMessagesAlreadyDelivered(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.MaxRetries = 5 })
	enq(t, q, High, "")

	for i := 0; i < 3; i++ {
		_, r := mustDequeue(t, q)
		if dead, err := q.Nack(r, 0); err != nil || dead != nil {
			t.Fatalf("attempt %d: dead=%v err=%v", i+1, dead, err)
		}
	}

	c := q.Config()
	c.MaxRetries = 3
	q.Reconfigure(c)

	_, r := mustDequeue(t, q)
	dead, err := q.Nack(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dead == nil {
		t.Fatal("a message past the new limit should dead-letter on its next failure")
	}
}
