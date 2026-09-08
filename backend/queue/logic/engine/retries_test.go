package engine

import (
	"sync"
	"testing"
	"time"
)

// The API models MaxRetries as a pointer so an explicit 0 differs from absent,
// and 0 means dead-letter on the first failure. Defaulting it back to 3 makes
// the setting the queue reports a lie.
func TestMaxRetriesZeroDeadLettersOnTheFirstFailure(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) {
		c.MaxRetries, c.MaxRetriesSet, c.HasDeadLetter = 0, true, true
	})
	if got := q.Config().MaxRetries; got != 0 {
		t.Fatalf("an explicit MaxRetries=0 became %d", got)
	}

	enq(t, q, High, "")
	_, r := mustDequeue(t, q)
	dead, err := q.Nack(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dead == nil {
		t.Fatal("the first failure did not dead-letter")
	}
}

// A caller that never set one still gets the default, so the engine stays
// usable on its own.
func TestMaxRetriesDefaultsWhenNobodySetIt(t *testing.T) {
	q := New(Config{Key: QueueKey{Org: "org1", Name: "q"}}, NewFakeClock(), nil, 1)
	if got := q.Config().MaxRetries; got != 3 {
		t.Fatalf("MaxRetries = %d, want the default of 3", got)
	}
}

// A group is listed in the band of its head message. When that head expires the
// entry has to be reissued, or the next message is served at the dead one's
// priority and outranks messages that genuinely sit higher.
func TestExpiredGroupHeadDoesNotLeaveTheGroupInItsBand(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.StarvationReserve = 0 })

	if _, err := q.Enqueue(EnqueueOptions{
		Payload: []byte("high-expires"), Priority: High, GroupID: "g", TTL: time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(EnqueueOptions{
		Payload: []byte("low"), Priority: Low, GroupID: "g",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(EnqueueOptions{
		Payload: []byte("medium"), Priority: Medium, GroupID: "other",
	}); err != nil {
		t.Fatal(err)
	}

	clk.Advance(2 * time.Second)
	q.Sweep()

	m, _ := mustDequeue(t, q)
	if got := string(m.Payload); got != "medium" {
		t.Fatalf("served %q first, want the MEDIUM: the group is still listed in "+
			"the expired head's HIGH band", got)
	}
}

// MaxDepth is a cap, so concurrent producers must not be able to walk past it
// by all reading the same depth before any of them adds to it.
func TestMaxDepthHoldsUnderConcurrentProducers(t *testing.T) {
	const cap = 50
	q, _ := newTestQueue(t, func(c *Config) { c.MaxDepth = cap })

	var wg sync.WaitGroup
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				_, _ = q.Enqueue(EnqueueOptions{Payload: []byte("x"), Priority: Medium})
			}
		}()
	}
	wg.Wait()

	st := q.Stats()
	if got := st.ReadyTotal(); got > cap {
		t.Fatalf("%d messages accepted against a cap of %d", got, cap)
	}
}

// A rejected enqueue must give its reservation back, or the queue fills up with
// room it never used.
func TestARefusedEnqueueDoesNotConsumeDepth(t *testing.T) {
	q, _ := newTestQueue(t, func(c *Config) { c.MaxDepth = 2 })
	for i := 0; i < 5; i++ {
		_, _ = q.Enqueue(EnqueueOptions{Payload: []byte("x"), Priority: Medium})
	}
	// Drain both, then the queue has to accept two more.
	for i := 0; i < 2; i++ {
		m, r := mustDequeue(t, q)
		if err := q.Ack(r); err != nil {
			t.Fatalf("ack %s: %v", m.ID, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := q.Enqueue(EnqueueOptions{Payload: []byte("x"), Priority: Medium}); err != nil {
			t.Fatalf("enqueue %d after draining: %v", i, err)
		}
	}
}

// Expiry is the one way a message leaves without the caller that removed it
// adjusting the depth, so the sweep has to account for it. Otherwise the cap
// fills up with room nothing is using.
func TestExpiredMessagesGiveTheirDepthBack(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) { c.MaxDepth = 3 })

	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue(EnqueueOptions{
			Payload: []byte("x"), Priority: Medium, TTL: time.Second,
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if _, err := q.Enqueue(EnqueueOptions{Payload: []byte("x"), Priority: Medium}); err == nil {
		t.Fatal("a fourth message was accepted against a cap of 3")
	}

	clk.Advance(2 * time.Second)
	q.Sweep()

	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue(EnqueueOptions{Payload: []byte("x"), Priority: Medium}); err != nil {
			t.Fatalf("enqueue %d once the queue had expired empty: %v", i, err)
		}
	}
}

// Dead-lettering removes the message the same way, so the room it held has to
// come back too.
func TestDeadLetteringGivesDepthBack(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) {
		c.MaxDepth, c.MaxRetries, c.HasDeadLetter = 1, 1, true
	})

	enq(t, q, High, "")
	mustDequeue(t, q)
	clk.Advance(time.Hour) // let the lease lapse so the sweep retires it
	if res := q.Sweep(); len(res.DeadLettered) != 1 {
		t.Fatalf("dead-lettered %d, want 1", len(res.DeadLettered))
	}

	if _, err := q.Enqueue(EnqueueOptions{Payload: []byte("x"), Priority: High}); err != nil {
		t.Fatalf("the dead-lettered message did not free its room: %v", err)
	}
}
