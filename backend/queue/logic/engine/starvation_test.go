package engine

import (
	"testing"
	"time"
)

func agedQueue(t *testing.T, on bool) (*Queue, *FakeClock) {
	t.Helper()
	return newTestQueue(t, func(c *Config) {
		c.StarvationThreshold = time.Minute
		c.StarvationReserve = 0.2
		c.StarvationAvoidanceEnabled = on
	})
}

// fill puts n messages of one priority across every slot and ages them past the
// starvation threshold.
func fillAged(t *testing.T, q *Queue, clk *FakeClock, n int, p Priority) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := q.EnqueueToSlot(uint16(i%SlotsPerQueue), EnqueueOptions{
			Payload: []byte("x"), Priority: p,
		}); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(5 * time.Minute)
}

func drainSeqs(q *Queue) []uint64 {
	var out []uint64
	for {
		m, r, ok := q.Dequeue()
		if !ok {
			return out
		}
		out = append(out, m.Seq)
		_ = q.Ack(r)
	}
}

func outOfOrder(seqs []uint64) int {
	n := 0
	for i := 1; i < len(seqs); i++ {
		if seqs[i] < seqs[i-1] {
			n++
		}
	}
	return n
}

// One priority means nothing can be starved, so the reserve must not fire even
// when every message is well past the threshold.
func TestStarvationDoesNotReorderOnePriority(t *testing.T) {
	for _, on := range []bool{false, true} {
		q, clk := agedQueue(t, on)
		fillAged(t, q, clk, 500, Medium)

		seqs := drainSeqs(q)
		if len(seqs) != 500 {
			t.Fatalf("enabled=%v: delivered %d of 500", on, len(seqs))
		}
		if bad := outOfOrder(seqs); bad != 0 {
			t.Fatalf("enabled=%v: %d messages of equal priority delivered out of FIFO order", on, bad)
		}
		if e := q.Stats().Escapes; e != 0 {
			t.Fatalf("enabled=%v: reserve fired %d times with nothing to rescue", on, e)
		}
	}
}

func TestStarvationOffKeepsStrictPriority(t *testing.T) {
	q, clk := agedQueue(t, false)

	low, err := q.EnqueueToSlot(0, EnqueueOptions{Payload: []byte("low"), Priority: Low})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Minute)
	for i := 0; i < 40; i++ {
		if _, err := q.EnqueueToSlot(uint16(i%SlotsPerQueue), EnqueueOptions{
			Payload: []byte("high"), Priority: High,
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 40; i++ {
		m, r, ok := q.Dequeue()
		if !ok {
			t.Fatalf("dequeue %d failed", i)
		}
		if m.ID == low.ID {
			t.Fatalf("low-priority message served at position %d with avoidance off", i)
		}
		_ = q.Ack(r)
	}
	if e := q.Stats().Escapes; e != 0 {
		t.Fatalf("reserve fired %d times with avoidance off", e)
	}
}

func TestStarvationOnRescuesLowerBand(t *testing.T) {
	q, clk := agedQueue(t, true)

	low, err := q.EnqueueToSlot(0, EnqueueOptions{Payload: []byte("low"), Priority: Low})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Minute)
	for i := 0; i < 40; i++ {
		if _, err := q.EnqueueToSlot(uint16(i%SlotsPerQueue), EnqueueOptions{
			Payload: []byte("high"), Priority: High,
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 40; i++ {
		m, r, ok := q.Dequeue()
		if !ok {
			t.Fatalf("dequeue %d failed", i)
		}
		_ = q.Ack(r)
		if m.ID == low.ID {
			if e := q.Stats().Escapes; e == 0 {
				t.Fatal("low message served but no escape recorded")
			}
			return
		}
	}
	t.Fatal("starved low-priority message never served with avoidance on")
}

// Reconfigure has to reach the delivery path, since the flag is toggled in the
// UI on a running queue.
func TestStarvationFlagAppliesOnReconfigure(t *testing.T) {
	q, clk := agedQueue(t, false)

	low, err := q.EnqueueToSlot(0, EnqueueOptions{Payload: []byte("low"), Priority: Low})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Minute)
	for i := 0; i < 60; i++ {
		if _, err := q.EnqueueToSlot(uint16(i%SlotsPerQueue), EnqueueOptions{
			Payload: []byte("high"), Priority: High,
		}); err != nil {
			t.Fatal(err)
		}
	}

	c := q.Config()
	c.StarvationAvoidanceEnabled = true
	if !q.Reconfigure(c) {
		t.Fatal("Reconfigure reported no change")
	}

	for i := 0; i < 40; i++ {
		m, r, ok := q.Dequeue()
		if !ok {
			t.Fatalf("dequeue %d failed", i)
		}
		_ = q.Ack(r)
		if m.ID == low.ID {
			return
		}
	}
	t.Fatal("flag turned on but the low-priority message was still not served")
}
