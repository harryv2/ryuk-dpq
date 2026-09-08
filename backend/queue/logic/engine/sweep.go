package engine

import (
	"container/heap"
	"sort"
	"time"
)

// sweepLeases expires leases whose deadline has passed. Returns messages that
// have exhausted their attempts; the caller routes them to a dead-letter queue.
func (s *slot) sweepLeases(now time.Time, maxRetries uint32, deadLetter bool) (dead []*Message, requeued int) {
	for s.timers.Len() > 0 && !s.timers[0].at.After(now) {
		e := heap.Pop(&s.timers).(timerEntry)
		l, ok := s.inflight[e.id]
		if !ok || l.epoch != e.epoch {
			continue // acknowledged already, or leased again since
		}
		delete(s.inflight, e.id)
		m, back := s.retire(l, now, maxRetries, 0, deadLetter)
		if m != nil {
			dead = append(dead, m)
		}
		if back {
			requeued++
		}
	}
	s.refreshHint()
	return dead, requeued
}

// retire decides what happens to a message whose lease ended. It goes back to
// the front of its group (order is strict) but its group goes to the back of
// its band, so a message that keeps failing does not block the band every cycle.
func (s *slot) retire(
	l *lease, now time.Time, maxRetries uint32, delay time.Duration, deadLetter bool,
) (*Message, bool) {
	g, m := l.g, l.msg
	defer s.unlock(g)

	switch {
	case m.expired(now):
		s.move(m, stInFlight, stAbsent)
		s.st.expired++
		return nil, false
	// Out of retries, and there is a dead-letter queue to move it to.
	case m.Attempts >= maxRetries && deadLetter:
		s.move(m, stInFlight, stAbsent)
		s.st.deadLettered++
		return m, false
	}

	if delay > 0 {
		m.DeliverAfter = now.Add(delay)
		s.hold(m, stInFlight)
		s.st.requeued++
		return nil, true
	}

	g.msgs.pushFront(m)
	s.move(m, stInFlight, stReady)
	s.st.requeued++
	return nil, true
}

func (s *slot) nack(
	r Receipt, delay time.Duration, now time.Time, maxRetries uint32, deadLetter bool,
) (*Message, error) {
	l, ok := s.inflight[r.MessageID]
	if !ok {
		return nil, ErrNotInFlight
	}
	if l.epoch != r.Epoch {
		return nil, ErrLeaseExpired
	}
	delete(s.inflight, r.MessageID)
	dead, _ := s.retire(l, now, maxRetries, delay, deadLetter)
	s.refreshHint()
	return dead, nil
}

// sweepTTL drops expired messages still sitting in ready.
func (s *slot) sweepTTL(now time.Time) int {
	n := 0
	for _, g := range s.groups {
		dropped := 0
		for {
			m, ok := g.msgs.front()
			if !ok || !m.expired(now) {
				break
			}
			g.msgs.popFront()
			s.dropExpired(m)
			dropped++
			n++
		}
		// A group is listed in the band of its head. That head is gone, so the
		// entry names a priority the group no longer has and the next message
		// would be served at the expired one's band.
		if dropped > 0 && g.inBand {
			g.version++
			g.inBand = false
		}
		if !g.inBand && !g.locked && !g.msgs.empty() {
			s.pushGroup(g)
		}
		s.dropGroupIfIdle(g)
	}
	s.refreshHint()
	return n
}

func (s *slot) releaseDelayed(now time.Time) int {
	n := 0
	for s.delayed.Len() > 0 && !s.delayed[0].at.After(now) {
		e := heap.Pop(&s.delayed).(delayEntry)
		e.msg.DeliverAfter = time.Time{}
		s.move(e.msg, stDelayed, stReady)
		s.admit(e.msg)
		n++
	}
	if n > 0 {
		s.refreshHint()
	}
	return n
}

// copyAll returns everything the slot holds without removing it, so a handoff
// can ship the messages while this node stays able to serve them if it fails.
func (s *slot) copyAll() []*Message {
	out := make([]*Message, 0, len(s.inflight))
	for _, l := range s.inflight {
		out = append(out, l.msg)
	}
	for _, g := range s.groups {
		out = append(out, g.msgs.all()...)
	}
	for _, e := range s.delayed {
		out = append(out, e.msg)
	}
	return out
}

// drain removes every message from the slot and returns them in Seq order.
func (s *slot) drain() []*Message {
	out := make([]*Message, 0, len(s.inflight))
	for _, l := range s.inflight {
		out = append(out, l.msg)
	}
	for _, g := range s.groups {
		for {
			m, ok := g.msgs.popFront()
			if !ok {
				break
			}
			out = append(out, m)
		}
	}
	for s.delayed.Len() > 0 {
		out = append(out, heap.Pop(&s.delayed).(delayEntry).msg)
	}

	s.inflight = make(map[string]*lease)
	s.groups = make(map[string]*group)
	s.bands = make(map[Priority]*deque[bandEntry])
	s.bandMask = [2]uint64{}
	s.timers = nil
	s.st.resetGauges()
	s.refreshHint()

	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}
