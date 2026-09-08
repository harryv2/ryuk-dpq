package engine

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// assertStats compares the maintained gauges against what the slots actually
// hold. The gauges are a partition, so any code path that moves a message
// without saying so shows up here.
func assertStats(t *testing.T, q *Queue) {
	t.Helper()
	for _, s := range q.localSlots() {
		s.mu.Lock()
		var ready [3]int64
		var bytes int64
		for _, g := range s.groups {
			for i := 0; i < g.msgs.len(); i++ {
				m := g.msgs.at(i)
				ready[bucketOf(m.Priority)]++
				bytes += int64(len(m.Payload))
			}
		}
		for _, l := range s.inflight {
			bytes += int64(len(l.msg.Payload))
		}
		for _, d := range s.delayed {
			bytes += int64(len(d.msg.Payload))
		}
		inflight, delayed := int64(len(s.inflight)), int64(s.delayed.Len())
		st := s.st
		s.mu.Unlock()

		for b := 0; b < 3; b++ {
			if st.ready[b] != ready[b] {
				t.Errorf("slot %d: ready[%d] says %d, slot holds %d", s.id, b, st.ready[b], ready[b])
			}
		}
		if st.inflight != inflight {
			t.Errorf("slot %d: inflight says %d, slot holds %d", s.id, st.inflight, inflight)
		}
		if st.delayed != delayed {
			t.Errorf("slot %d: delayed says %d, slot holds %d", s.id, st.delayed, delayed)
		}
		if st.bytes != bytes {
			t.Errorf("slot %d: bytes says %d, slot holds %d", s.id, st.bytes, bytes)
		}
	}
}

// Every path a message can take, mixed at random, with the gauges checked
// against reality throughout.
func TestStatsMatchWhatTheSlotsHold(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	clk := NewFakeClock()
	q := New(Config{
		Key: QueueKey{Org: "o", Name: "q"}, VisibilityTimeout: 5 * time.Second,
		MaxRetries: 3, MaxRetriesSet: true, HasDeadLetter: true,
		StarvationThreshold: time.Minute, StarvationReserve: 0.2,
	}, clk, nil, 1)

	var held []Receipt
	for step := 0; step < 4000; step++ {
		switch rng.Intn(6) {
		case 0, 1:
			o := EnqueueOptions{
				Payload:  make([]byte, rng.Intn(50)+1),
				Priority: Priority(rng.Intn(101)),
			}
			if rng.Intn(3) == 0 {
				o.GroupID = fmt.Sprintf("g%d", rng.Intn(5))
			}
			if rng.Intn(4) == 0 {
				o.DeliverAfter = time.Duration(rng.Intn(3)+1) * time.Second
			}
			if rng.Intn(4) == 0 {
				o.TTL = time.Duration(rng.Intn(5)+1) * time.Second
			}
			_, _ = q.EnqueueToSlot(uint16(rng.Intn(SlotsPerQueue)), o)

		case 2, 3:
			if _, r, ok := q.Dequeue(); ok {
				held = append(held, r)
			}

		case 4:
			if len(held) > 0 {
				i := rng.Intn(len(held))
				r := held[i]
				held = append(held[:i], held[i+1:]...)
				if rng.Intn(2) == 0 {
					_ = q.Ack(r)
				} else {
					var d time.Duration
					if rng.Intn(2) == 0 {
						d = time.Second
					}
					_, _ = q.Nack(r, d)
				}
			}

		case 5:
			clk.Advance(time.Duration(rng.Intn(3)) * time.Second)
			q.Sweep()
		}

		if step%200 == 0 {
			assertStats(t, q)
			if t.Failed() {
				t.Fatalf("gauges drifted at step %d", step)
			}
		}
	}
	clk.Advance(time.Hour)
	q.Sweep()
	assertStats(t, q)
}

func migrationFixture(t *testing.T, clk Clock) *Queue {
	t.Helper()
	q := New(Config{
		Key: QueueKey{Org: "o", Name: "q"}, VisibilityTimeout: time.Minute,
		MaxRetries: 3, MaxRetriesSet: true,
	}, clk, nil, 1)
	for i := 0; i < 200; i++ {
		o := EnqueueOptions{Payload: make([]byte, 10), Priority: Priority(i % 101)}
		if i%3 == 0 {
			o.GroupID = fmt.Sprintf("g%d", i%7)
		}
		if i%5 == 0 {
			o.DeliverAfter = 10 * time.Second
		}
		if _, err := q.EnqueueToSlot(uint16(i%SlotsPerQueue), o); err != nil {
			t.Fatal(err)
		}
	}
	return q
}

// A handoff patches the gauges by hand rather than moving messages through the
// ordinary transitions, so it gets its own check.
func TestStatsSurviveAHandoff(t *testing.T) {
	clk := NewFakeClock()
	src := migrationFixture(t, clk)
	dst := New(Config{
		Key: QueueKey{Org: "o", Name: "q"}, VisibilityTimeout: time.Minute,
		MaxRetries: 3, MaxRetriesSet: true,
	}, clk, nil, 1)

	for i := 0; i < 30; i++ {
		src.Dequeue()
	}
	assertStats(t, src)

	ids := make([]uint16, SlotsPerQueue)
	for i := range ids {
		ids[i] = uint16(i)
	}
	if err := dst.Absorb(src.FreezeSlots(ids)); err != nil {
		t.Fatal(err)
	}
	src.DropSlots(ids)

	assertStats(t, src)
	assertStats(t, dst)

	st := dst.Stats()
	if got := st.ReadyTotal() + st.InFlight + st.Delayed; got != 200 {
		t.Fatalf("the new owner holds %d of 200 messages", got)
	}
	// Absorbing is a move, not a submission, so nothing is counted as enqueued.
	if st.Enqueued != 0 {
		t.Fatalf("absorbed messages counted as %d new submissions", st.Enqueued)
	}
}

// Lifetime totals are the queue's history. Handing slots to another node empties
// them but must not rewrite what already happened, or a Prometheus counter goes
// backwards every time a queue moves.
func TestHandingSlotsAwayKeepsLifetimeTotals(t *testing.T) {
	clk := NewFakeClock()
	q := migrationFixture(t, clk)

	for i := 0; i < 40; i++ {
		if _, r, ok := q.Dequeue(); ok {
			if err := q.Ack(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	before := q.Stats()
	if before.Enqueued != 200 || before.Acked != 40 {
		t.Fatalf("fixture wrong: enqueued %d, acked %d", before.Enqueued, before.Acked)
	}

	ids := make([]uint16, SlotsPerQueue)
	for i := range ids {
		ids[i] = uint16(i)
	}
	q.FreezeSlots(ids)
	q.DropSlots(ids)

	after := q.Stats()
	if after.Enqueued != before.Enqueued {
		t.Errorf("enqueued went from %d to %d", before.Enqueued, after.Enqueued)
	}
	if after.Acked != before.Acked {
		t.Errorf("acked went from %d to %d", before.Acked, after.Acked)
	}
	if after.ReadyTotal()+after.InFlight+after.Delayed != 0 {
		t.Errorf("the slots were handed away but still report messages: %+v", after)
	}
}

// Every message enqueued is either still here or accounted for by a terminal
// counter. Nothing may vanish between the two.
func conservation(t *testing.T, q *Queue, label string) {
	t.Helper()
	st := q.Stats()
	held := st.ReadyTotal() + st.InFlight + st.Delayed
	left := int64(st.Acked + st.Expired + st.DeadLettered)
	if int64(st.Enqueued) != held+left {
		t.Errorf("%s: enqueued=%d but held=%d + left=%d = %d (acked=%d expired=%d dl=%d)",
			label, st.Enqueued, held, left, held+left, st.Acked, st.Expired, st.DeadLettered)
	}
}

// TopReady is what the gateway ranks nodes by, so it must name a priority the
// node can actually serve.
func assertTopReady(t *testing.T, q *Queue, label string) {
	t.Helper()
	want := int16(-1)
	for _, s := range q.localSlots() {
		s.mu.Lock()
		for _, g := range s.groups {
			if g.locked {
				continue
			}
			if m, ok := g.msgs.front(); ok && !m.expired(q.clock.Now()) && int16(m.Priority) > want {
				want = int16(m.Priority)
			}
		}
		s.mu.Unlock()
	}
	if got := q.Stats().TopReady; got != want {
		t.Errorf("%s: TopReady=%d, highest ready message is %d", label, got, want)
	}
}

func TestLifetimeCountersAccountForEveryMessage(t *testing.T) {
	clk := NewFakeClock()
	q := New(Config{
		Key: QueueKey{Org: "o", Name: "q"}, VisibilityTimeout: 5 * time.Second,
		MaxRetries: 2, MaxRetriesSet: true, HasDeadLetter: true,
	}, clk, nil, 1)

	// plain enqueue and ack
	for i := 0; i < 10; i++ {
		if _, err := q.EnqueueToSlot(0, EnqueueOptions{Payload: []byte("x"), Priority: Medium}); err != nil {
			t.Fatal(err)
		}
	}
	conservation(t, q, "after 10 enqueues")
	assertTopReady(t, q, "after 10 enqueues")

	for i := 0; i < 4; i++ {
		_, r, _ := q.Dequeue()
		if err := q.Ack(r); err != nil {
			t.Fatal(err)
		}
	}
	conservation(t, q, "after 4 acks")
	if st := q.Stats(); st.Acked != 4 {
		t.Errorf("acked=%d, want 4", st.Acked)
	}

	// a redelivery, then a dead-letter, on a group of its own so the same
	// message comes back both times
	if _, err := q.EnqueueToSlot(5, EnqueueOptions{
		Payload: []byte("poison"), Priority: 100, GroupID: "poison",
	}); err != nil {
		t.Fatal(err)
	}
	_, r, _ := q.Dequeue()
	if _, err := q.Nack(r, 0); err != nil {
		t.Fatal(err)
	}
	if st := q.Stats(); st.Requeued != 1 {
		t.Errorf("requeued=%d, want 1", st.Requeued)
	}
	conservation(t, q, "after a nack")

	_, r2, _ := q.Dequeue()
	dead, err := q.Nack(r2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dead == nil {
		t.Fatal("second failure did not dead-letter with maxRetries=2")
	}
	if st := q.Stats(); st.DeadLettered != 1 {
		t.Errorf("deadLettered=%d, want 1", st.DeadLettered)
	}
	conservation(t, q, "after a dead-letter")

	// TTL expiry, both while ready and while in flight
	if _, err := q.EnqueueToSlot(1, EnqueueOptions{
		Payload: []byte("x"), Priority: High, TTL: time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)
	q.Sweep()
	if st := q.Stats(); st.Expired != 1 {
		t.Errorf("expired=%d, want 1", st.Expired)
	}
	conservation(t, q, "after a TTL expiry")

	// delayed delivery
	if _, err := q.EnqueueToSlot(2, EnqueueOptions{
		Payload: []byte("x"), Priority: Low, DeliverAfter: 10 * time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	conservation(t, q, "while delayed")
	clk.Advance(11 * time.Second)
	q.Sweep()
	conservation(t, q, "after release")
	assertTopReady(t, q, "after release")
}

// A group is listed under its head's priority. When that head expires the group
// moves band, and TopReady has to move with it.
func TestTopReadyFollowsAGroupThatChangesBand(t *testing.T) {
	clk := NewFakeClock()
	q := New(Config{
		Key: QueueKey{Org: "o", Name: "q"}, VisibilityTimeout: time.Minute,
		MaxRetries: 3, MaxRetriesSet: true,
	}, clk, nil, 1)

	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("high"), Priority: 90, GroupID: "g", TTL: time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.EnqueueToSlot(0, EnqueueOptions{
		Payload: []byte("low"), Priority: 10, GroupID: "g",
	}); err != nil {
		t.Fatal(err)
	}
	if got := q.Stats().TopReady; got != 90 {
		t.Fatalf("TopReady=%d before expiry, want 90", got)
	}

	clk.Advance(2 * time.Second)
	q.Sweep()

	assertTopReady(t, q, "after the 90 expired")
}
