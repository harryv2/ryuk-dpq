package engine

import (
	"math"
	"time"
)

const (
	maxDispatchRetries  = 4
	starvationScanLimit = 16
)

func (q *Queue) Dequeue() (*Message, Receipt, bool) {
	if q.frozen.Load() {
		return nil, Receipt{}, false
	}
	now := q.clock.Now()

	if q.reserveTick(q.dispatch.Add(1)) {
		if m, r, ok := q.takeStarved(now); ok {
			q.escapes.Add(1)
			q.afterTake(r, m)
			return m, r, true
		}
	}
	m, r, ok := q.takeUrgent(now)
	if ok {
		q.afterTake(r, m)
	}
	return m, r, ok
}

func (q *Queue) afterTake(r Receipt, m *Message) {
	q.journal.AppendAttempt(r.Slot, m.ID, m.Attempts, r.Epoch)
}

// reserveTick decides whether this delivery belongs to the share set aside for
// work that has waited too long.
func (q *Queue) reserveTick(n uint64) bool {
	if q.cfg.StarvationReserve <= 0 {
		return false
	}
	every := uint64(1.0 / q.cfg.StarvationReserve)
	if every == 0 {
		return true
	}
	return n%every == 0
}

// takeUrgent reads two atomics per slot, then locks only the winner. Comparing
// headSeq at equal priority is what makes FIFO exact -- and why a distributed
// queue, whose owner sees only a subset of slots, cannot promise it.
func (q *Queue) takeUrgent(now time.Time) (*Message, Receipt, bool) {
	for attempt := 0; attempt < maxDispatchRetries; attempt++ {
		var best *slot
		var bestBand Priority
		bestSeq := uint64(math.MaxUint64)
		found := false

		for _, s := range q.localSlots() {
			h := s.hint.Load()
			if h == 0 {
				continue
			}
			band := Priority(h - 1)
			seq := s.headSeq.Load()
			if !found || band > bestBand || (band == bestBand && seq < bestSeq) {
				best, bestBand, bestSeq, found = s, band, seq, true
			}
		}
		if !found {
			return nil, Receipt{}, false
		}

		best.mu.Lock()
		m, r, ok := best.take(bestBand, now, q.cfg.VisibilityTimeout)
		best.mu.Unlock()
		if ok {
			r.Incarnation = q.incarnation
			return m, r, true
		}
		// the slot moved under us, or that band held only stale entries
	}
	return nil, Receipt{}, false
}

func (q *Queue) takeStarved(now time.Time) (*Message, Receipt, bool) {
	slots := q.localSlots()
	if len(slots) == 0 {
		return nil, Receipt{}, false
	}
	cutoff := now.Add(-q.cfg.StarvationThreshold)
	start := int(q.cursor.Add(1)) % len(slots)

	for i := 0; i < len(slots) && i < starvationScanLimit; i++ {
		s := slots[(start+i)%len(slots)]
		s.mu.Lock()
		p, ok := s.oldestBandBefore(cutoff)
		if !ok {
			s.mu.Unlock()
			continue
		}
		m, r, taken := s.take(p, now, q.cfg.VisibilityTimeout)
		s.mu.Unlock()
		if taken {
			r.Incarnation = q.incarnation
			return m, r, true
		}
	}
	return nil, Receipt{}, false
}
