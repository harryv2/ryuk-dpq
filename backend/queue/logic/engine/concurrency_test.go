package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tracker records what happened to every message so the invariants can be
// checked once the run finishes.
type tracker struct {
	mu        sync.Mutex
	acked     map[string]int
	delivered map[string]int
	inFlight  map[string]bool
	groupSeen map[string][]uint64
}

func newTracker() *tracker {
	return &tracker{
		acked:     map[string]int{},
		delivered: map[string]int{},
		inFlight:  map[string]bool{},
		groupSeen: map[string][]uint64{},
	}
}

func (tr *tracker) deliver(t *testing.T, m *Message) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.inFlight[m.ID] {
		t.Errorf("message %s delivered to two workers at once", m.ID)
	}
	tr.inFlight[m.ID] = true
	tr.delivered[m.ID]++
	if m.GroupID != "" {
		tr.groupSeen[m.GroupID] = append(tr.groupSeen[m.GroupID], m.Seq)
	}
}

func (tr *tracker) release(m *Message) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	delete(tr.inFlight, m.ID)
}

func (tr *tracker) ack(m *Message) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	delete(tr.inFlight, m.ID)
	tr.acked[m.ID]++
}

func TestConcurrentProducersAndConsumers(t *testing.T) {
	const (
		producers   = 8
		consumers   = 8
		perProducer = 500
		groups      = 16
		total       = producers * perProducer
	)

	q, _ := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = time.Hour // no redelivery in this run
		c.StarvationReserve = 0
	})
	tr := newTracker()

	var produced sync.WaitGroup
	produced.Add(producers)
	for p := 0; p < producers; p++ {
		go func(p int) {
			defer produced.Done()
			for i := 0; i < perProducer; i++ {
				_, err := q.Enqueue(EnqueueOptions{
					Payload:  []byte("x"),
					Priority: Priority((i*7 + p) % (MaxPriority + 1)),
					GroupID:  fmt.Sprintf("g%d", (p*perProducer+i)%groups),
				})
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}

	var ackedCount atomic.Int64
	var consumed sync.WaitGroup
	consumed.Add(consumers)
	done := make(chan struct{})

	for c := 0; c < consumers; c++ {
		go func() {
			defer consumed.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				m, r, ok := q.Dequeue()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				tr.deliver(t, m)
				if err := q.Ack(r); err != nil {
					t.Errorf("ack: %v", err)
					tr.release(m)
					continue
				}
				tr.ack(m)
				if ackedCount.Add(1) == total {
					close(done)
					return
				}
			}
		}()
	}

	produced.Wait()

	deadline := time.After(30 * time.Second)
	select {
	case <-done:
	case <-deadline:
		t.Fatalf("timed out: acked %d of %d", ackedCount.Load(), total)
	}
	consumed.Wait()

	if int(ackedCount.Load()) != total {
		t.Fatalf("acked %d, want %d", ackedCount.Load(), total)
	}
	for id, n := range tr.acked {
		if n != 1 {
			t.Fatalf("message %s acked %d times", id, n)
		}
	}
	if len(tr.inFlight) != 0 {
		t.Fatalf("%d messages still in flight", len(tr.inFlight))
	}
	for g, seqs := range tr.groupSeen {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] < seqs[i-1] {
				t.Fatalf("group %s delivered out of order at %d", g, i)
			}
		}
	}
	if s := q.Stats(); s.ReadyTotal() != 0 || s.InFlight != 0 {
		t.Fatalf("queue not drained: %+v", s)
	}
}

func TestConcurrentRedelivery(t *testing.T) {
	const total = 400

	q, clk := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = 50 * time.Millisecond
		c.MaxRetries = 100
		c.StarvationReserve = 0
	})
	for i := 0; i < total; i++ {
		enq(t, q, Priority(i%101), fmt.Sprintf("g%d", i%8))
	}

	tr := newTracker()
	var acked atomic.Int64
	var wg sync.WaitGroup
	done := make(chan struct{})

	// half the workers vanish without acknowledging
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				m, r, ok := q.Dequeue()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				tr.deliver(t, m)
				if c%2 == 0 {
					tr.release(m) // crash before acknowledging
					continue
				}
				if err := q.Ack(r); err != nil {
					tr.release(m)
					continue
				}
				tr.ack(m)
				if acked.Add(1) == total {
					close(done)
					return
				}
			}
		}(c)
	}

	// sweeper drives redelivery
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(2 * time.Millisecond):
				clk.Advance(60 * time.Millisecond)
				q.Sweep()
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out: acked %d of %d", acked.Load(), total)
	}
	wg.Wait()

	for id, n := range tr.acked {
		if n != 1 {
			t.Fatalf("message %s acked %d times", id, n)
		}
	}
	if int(acked.Load()) != total {
		t.Fatalf("acked %d, want %d", acked.Load(), total)
	}
}

// TestGroupHoldsOneMessageAtATime is the invariant that makes group ordering
// mean anything.
func TestGroupHoldsOneMessageAtATime(t *testing.T) {
	const (
		groups    = 12
		perGroup  = 60
		consumers = 10
		total     = groups * perGroup
	)

	q, _ := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = time.Hour
		c.StarvationReserve = 0
	})
	for g := 0; g < groups; g++ {
		for i := 0; i < perGroup; i++ {
			enq(t, q, Priority(i%101), fmt.Sprintf("g%d", g))
		}
	}

	var mu sync.Mutex
	open := map[string]string{} // group -> the message id currently held
	var violations atomic.Int64
	var acked atomic.Int64

	var wg sync.WaitGroup
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for acked.Load() < total {
				m, r, ok := q.Dequeue()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}

				mu.Lock()
				if held, busy := open[m.GroupID]; busy {
					violations.Add(1)
					t.Errorf("group %s handed out %s while %s was still held",
						m.GroupID, m.ID, held)
				}
				open[m.GroupID] = m.ID
				mu.Unlock()

				// Hold it long enough that a second consumer would collide.
				time.Sleep(200 * time.Microsecond)

				mu.Lock()
				delete(open, m.GroupID)
				mu.Unlock()

				if err := q.Ack(r); err != nil {
					t.Errorf("ack: %v", err)
					return
				}
				acked.Add(1)
			}
		}()
	}
	waitOrFail(t, &wg, 30*time.Second)

	if v := violations.Load(); v != 0 {
		t.Fatalf("%d group exclusivity violations", v)
	}
	if got := acked.Load(); got != total {
		t.Fatalf("acked %d, want %d", got, total)
	}
}

// TestGroupOrderSurvivesConcurrentProducers checks the ordering guarantee end to
// end: each producer owns its groups, so the payload index is the submission
// order and the delivered order must match it exactly.
func TestGroupOrderSurvivesConcurrentProducers(t *testing.T) {
	const (
		producers   = 6
		perProducer = 300
		consumers   = 6
		total       = producers * perProducer
	)

	q, _ := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = time.Hour
		c.StarvationReserve = 0
	})

	var produce sync.WaitGroup
	for p := 0; p < producers; p++ {
		produce.Add(1)
		go func(p int) {
			defer produce.Done()
			// One group per producer, so the order within it is the order this
			// goroutine submitted in and nothing else.
			group := fmt.Sprintf("p%d", p)
			for i := 0; i < perProducer; i++ {
				_, err := q.Enqueue(EnqueueOptions{
					Payload:  []byte(fmt.Sprintf("%d", i)),
					Priority: Priority((i * 13) % (MaxPriority + 1)),
					GroupID:  group,
				})
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}
	produce.Wait()

	var mu sync.Mutex
	lastSeen := map[string]int{}
	var acked atomic.Int64

	var wg sync.WaitGroup
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for acked.Load() < total {
				m, r, ok := q.Dequeue()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				var idx int
				fmt.Sscanf(string(m.Payload), "%d", &idx)

				mu.Lock()
				prev, seen := lastSeen[m.GroupID]
				if seen && idx != prev+1 {
					t.Errorf("group %s: got index %d after %d", m.GroupID, idx, prev)
				}
				if !seen && idx != 0 {
					t.Errorf("group %s: first delivery was index %d, want 0", m.GroupID, idx)
				}
				lastSeen[m.GroupID] = idx
				mu.Unlock()

				if err := q.Ack(r); err != nil {
					t.Errorf("ack: %v", err)
					return
				}
				acked.Add(1)
			}
		}()
	}
	waitOrFail(t, &wg, 30*time.Second)

	if got := acked.Load(); got != total {
		t.Fatalf("acked %d, want %d", got, total)
	}
}

// TestStaleAckIsRejected races an acknowledgment against the sweeper that
// takes the lease back.
func TestStaleAckIsRejected(t *testing.T) {
	const total = 300

	q, clk := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = time.Millisecond
		c.MaxRetries = 1000
		c.StarvationReserve = 0
	})
	for i := 0; i < total; i++ {
		enq(t, q, Priority(i%101), fmt.Sprintf("g%d", i%16))
	}

	var accepted, refused atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.Advance(2 * time.Millisecond)
				q.Sweep()
				time.Sleep(200 * time.Microsecond)
			}
		}
	}()

	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, r, ok := q.Dequeue()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				// Long enough that the sweeper usually gets there first.
				time.Sleep(time.Millisecond)
				switch err := q.Ack(r); err {
				case nil:
					accepted.Add(1)
				case ErrNotInFlight, ErrLeaseExpired:
					refused.Add(1)
				default:
					t.Errorf("unexpected ack error: %v", err)
					return
				}
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	if refused.Load() == 0 {
		t.Fatal("no acknowledgment was refused; the stale-ack path never ran")
	}
	// Nothing may be acknowledged more than once, so accepted acks can never
	// exceed the number of messages that existed.
	if accepted.Load() > total {
		t.Fatalf("accepted %d acks for %d messages", accepted.Load(), total)
	}
	s := q.Stats()
	if s.Acked > total {
		t.Fatalf("counter says %d acked, only %d messages existed", s.Acked, total)
	}
}

// TestDeadLetterExactlyOnce runs every message past its retry limit while
// several workers and the sweeper compete, then checks each message was
// dead-lettered exactly once.
func TestDeadLetterExactlyOnce(t *testing.T) {
	const (
		total   = 200
		retries = 2
	)

	q, clk := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = time.Millisecond
		c.MaxRetries = retries
		c.HasDeadLetter = true
		c.StarvationReserve = 0
	})
	for i := 0; i < total; i++ {
		enq(t, q, Priority(i%101), fmt.Sprintf("g%d", i%8))
	}

	var mu sync.Mutex
	dead := map[string]int{}
	record := func(ds []DeadLetter) {
		mu.Lock()
		defer mu.Unlock()
		for _, d := range ds {
			dead[d.Msg.ID]++
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Consumers that always give up, so every message burns its retries.
	for c := 0; c < 6; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, r, ok := q.Dequeue()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				if m, err := q.Nack(r, 0); err == nil && m != nil {
					record([]DeadLetter{{Slot: r.Slot, Msg: m}})
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.Advance(5 * time.Millisecond)
				record(q.Sweep().DeadLettered)
				time.Sleep(500 * time.Microsecond)
			}
		}
	}()

	deadline := time.After(20 * time.Second)
	for {
		mu.Lock()
		n := len(dead)
		mu.Unlock()
		if n == total {
			break
		}
		select {
		case <-deadline:
			close(stop)
			wg.Wait()
			t.Fatalf("only %d of %d messages dead-lettered", n, total)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	close(stop)
	wg.Wait()

	for id, n := range dead {
		if n != 1 {
			t.Fatalf("message %s dead-lettered %d times", id, n)
		}
	}
	if s := q.Stats(); s.ReadyTotal() != 0 || s.InFlight != 0 {
		t.Fatalf("queue should be empty after every message died: %+v", s)
	}
}

// TestFreezeDuringTrafficLosesNothing is the migration path under load.
func TestFreezeDuringTrafficLosesNothing(t *testing.T) {
	const (
		producers   = 4
		perProducer = 400
		total       = producers * perProducer
	)

	q, _ := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = time.Hour
		c.StarvationReserve = 0
	})

	var rejected atomic.Int64
	var produce sync.WaitGroup
	for p := 0; p < producers; p++ {
		produce.Add(1)
		go func(p int) {
			defer produce.Done()
			for i := 0; i < perProducer; i++ {
				_, err := q.Enqueue(EnqueueOptions{
					Payload:  []byte("x"),
					Priority: Priority((i * 3) % (MaxPriority + 1)),
					GroupID:  fmt.Sprintf("p%d-g%d", p, i%5),
				})
				if err == ErrFrozen {
					// A frozen queue refuses writes; the gateway retries against
					// the new owner, so the producer here just counts it.
					rejected.Add(1)
					i--
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}

	// Freeze and absorb while the producers are still running.
	time.Sleep(2 * time.Millisecond)
	held := q.Freeze()
	moved := 0
	for _, msgs := range held {
		moved += len(msgs)
	}
	if err := q.Absorb(held); err != nil {
		t.Fatalf("absorb: %v", err)
	}
	q.Thaw()

	produce.Wait()

	if rejected.Load() == 0 {
		t.Log("note: the freeze window closed before any producer hit it")
	}

	drained := 0
	for {
		_, r, ok := q.Dequeue()
		if !ok {
			break
		}
		if err := q.Ack(r); err != nil {
			t.Fatalf("ack: %v", err)
		}
		drained++
	}
	if drained != total {
		t.Fatalf("drained %d of %d after a freeze that moved %d", drained, total, moved)
	}
}

// TestStatsNeverGoNegative reads counters while the queue is being worked. A
// count below zero means two paths decremented the same message.
func TestStatsNeverGoNegative(t *testing.T) {
	const total = 1500

	q, clk := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = 5 * time.Millisecond
		c.MaxRetries = 50
	})
	for i := 0; i < total; i++ {
		enq(t, q, Priority(i%101), fmt.Sprintf("g%d", i%20))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var bad atomic.Int64

	// A reader racing every writer. It also proves Stats does not deadlock
	// against Dequeue, which take slot locks in different orders.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s := q.Stats()
			if s.Ready[0] < 0 || s.Ready[1] < 0 || s.Ready[2] < 0 ||
				s.InFlight < 0 || s.Delayed < 0 || s.Bytes < 0 {
				bad.Add(1)
				t.Errorf("negative counter: %+v", s)
				return
			}
		}
	}()

	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, r, ok := q.Dequeue()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				switch c % 3 {
				case 0:
					_ = q.Ack(r)
				case 1:
					_, _ = q.Nack(r, 0)
				default:
					_, _ = q.Nack(r, 2*time.Millisecond)
				}
			}
		}(c)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.Advance(3 * time.Millisecond)
				q.Sweep()
				time.Sleep(300 * time.Microsecond)
			}
		}
	}()

	time.Sleep(750 * time.Millisecond)
	close(stop)
	wg.Wait()

	if bad.Load() != 0 {
		t.Fatal("a counter went negative under concurrency")
	}
	final := q.Stats()
	if final.ReadyTotal()+final.InFlight+final.Delayed > total {
		t.Fatalf("more messages accounted for than were ever created: %+v", final)
	}
}

// TestStarvationReserveMovesLowPriorityUnderLoad checks the guard still works
// when producers keep the high band full.
func TestStarvationReserveMovesLowPriorityUnderLoad(t *testing.T) {
	q, clk := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = time.Hour
		c.StarvationThreshold = 10 * time.Millisecond
		c.StarvationReserve = 0.25
	})

	// A backlog of low-priority work that is already old enough to qualify.
	const lows = 40
	lowIDs := map[string]bool{}
	for i := 0; i < lows; i++ {
		lowIDs[enq(t, q, Low, fmt.Sprintf("low-%d", i)).ID] = true
	}
	clk.Advance(50 * time.Millisecond)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Keep the high band busy for the whole run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := q.Enqueue(EnqueueOptions{
				Payload: []byte("x"), Priority: High,
				GroupID: fmt.Sprintf("high-%d", i%8),
			}); err != nil {
				return
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()

	var lowSeen atomic.Int64
	for c := 0; c < 4; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				m, r, ok := q.Dequeue()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				if lowIDs[m.ID] {
					lowSeen.Add(1)
				}
				_ = q.Ack(r)
			}
		}()
	}

	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()

	if lowSeen.Load() == 0 {
		t.Fatal("no low-priority message was delivered while high priority was saturated")
	}
	if s := q.Stats(); s.Escapes == 0 {
		t.Fatal("the starvation reserve never fired")
	}
}

// TestConcurrentDelayedRelease has consumers waiting on messages that are not
// visible yet while the sweeper releases them.
func TestConcurrentDelayedRelease(t *testing.T) {
	const total = 300

	q, clk := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = time.Hour
		c.StarvationReserve = 0
	})
	for i := 0; i < total; i++ {
		if _, err := q.Enqueue(EnqueueOptions{
			Payload:      []byte("x"),
			Priority:     Priority(i % 101),
			GroupID:      fmt.Sprintf("g%d", i%10),
			DeliverAfter: time.Duration(i%20+1) * time.Millisecond,
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	if s := q.Stats(); s.ReadyTotal() != 0 {
		t.Fatalf("a delayed message should not be ready yet: %+v", s)
	}

	var acked atomic.Int64
	var wg sync.WaitGroup
	done := make(chan struct{})

	for c := 0; c < 6; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				_, r, ok := q.Dequeue()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				if err := q.Ack(r); err != nil {
					t.Errorf("ack: %v", err)
					return
				}
				if acked.Add(1) == total {
					close(done)
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
				clk.Advance(3 * time.Millisecond)
				q.Sweep()
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out: acked %d of %d", acked.Load(), total)
	}
	wg.Wait()

	if got := acked.Load(); got != total {
		t.Fatalf("acked %d, want %d", got, total)
	}
}

// TestConcurrentEnqueueToDistinctSlots exercises the lock striping directly:
// every slot is written at once, which is the case the per-slot lock exists for.
func TestConcurrentEnqueueToDistinctSlots(t *testing.T) {
	const perSlot = 200

	q, _ := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = time.Hour
		c.StarvationReserve = 0
	})

	var wg sync.WaitGroup
	for slot := 0; slot < SlotsPerQueue; slot++ {
		wg.Add(1)
		go func(slot uint16) {
			defer wg.Done()
			for i := 0; i < perSlot; i++ {
				if _, err := q.EnqueueToSlot(slot, EnqueueOptions{
					Payload:  []byte("x"),
					Priority: Priority(i % 101),
					GroupID:  fmt.Sprintf("s%d-g%d", slot, i%4),
				}); err != nil {
					t.Errorf("enqueue to slot %d: %v", slot, err)
					return
				}
			}
		}(uint16(slot))
	}
	waitOrFail(t, &wg, 30*time.Second)

	want := int64(SlotsPerQueue * perSlot)
	st := q.Stats()
	if got := st.ReadyTotal(); got != want {
		t.Fatalf("ready %d, want %d", got, want)
	}

	// And a slot the queue does not have is refused rather than silently
	// swallowing the message.
	if _, err := q.EnqueueToSlot(SlotsPerQueue, EnqueueOptions{Payload: []byte("x")}); err != ErrBadSlot {
		t.Fatalf("want ErrBadSlot for a slot past the end, got %v", err)
	}
}

func waitOrFail(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out waiting for goroutines")
	}
}
