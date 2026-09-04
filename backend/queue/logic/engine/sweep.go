package engine

import (
	"container/heap"
	"sort"
	"time"
)

// sweepLeases expires leases whose deadline has passed. Returns messages that
// have exhausted their attempts; the caller routes them to a dead-letter queue.
func (s *slot) sweepLeases(now time.Time, maxRetries uint32) (dead []*Message, requeued int) {
	for s.timers.Len() > 0 && !s.timers[0].at.After(now) {
		e := heap.Pop(&s.timers).(timerEntry)
		l, ok := s.inflight[e.id]
		if !ok || l.epoch != e.epoch {
			continue // acknowledged already, or leased again since
		}
		delete(s.inflight, e.id)
		s.st.inflight--

		m, back := s.retire(l, now, maxRetries, 0)
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

// retire decides what happens to a message whose lease ended. It returns the
// message if it must be dead-lettered, and whether it went back to ready.
//
// The asymmetry matters: the message returns to the FRONT of its group, because
// it was the group's head and group order is strict; the group goes to the TAIL
// of its band, so a message that keeps failing does not block the band head
// every cycle.
func (s *slot) retire(l *lease, now time.Time, maxRetries uint32, delay time.Duration) (*Message, bool) {
	g, m := l.g, l.msg
	g.locked = false

	switch {
	case m.expired(now):
		s.st.expired++
		s.st.bytes -= int64(len(m.Payload))
		s.dropGroupIfIdle(g)
		return nil, false
	case m.Attempts >= maxRetries:
		s.st.deadLettered++
		s.st.bytes -= int64(len(m.Payload))
		s.dropGroupIfIdle(g)
		return m, false
	}

	if delay > 0 {
		m.DeliverAfter = now.Add(delay)
		heap.Push(&s.delayed, delayEntry{msg: m, at: m.DeliverAfter})
		s.st.delayed++
		s.st.requeued++
		s.dropGroupIfIdle(g)
		return nil, true
	}

	g.msgs.pushFront(m)
	s.st.ready[bucketOf(m.Priority)]++
	s.st.requeued++
	if !g.inBand {
		s.pushGroup(g)
	}
	return nil, true
}

func (s *slot) nack(r Receipt, delay time.Duration, now time.Time, maxRetries uint32) (*Message, error) {
	l, ok := s.inflight[r.MessageID]
	if !ok {
		return nil, ErrNotInFlight
	}
	if l.epoch != r.Epoch {
		return nil, ErrLeaseExpired
	}
	delete(s.inflight, r.MessageID)
	s.st.inflight--
	dead, _ := s.retire(l, now, maxRetries, delay)
	s.refreshHint()
	return dead, nil
}

// sweepTTL drops expired messages still sitting in ready. Only the front of
// each group is checked: anything behind is dropped when it reaches the front.
// Lazy expiry at take would be enough for correctness, but the oldest-age
// metric is defined over non-expired messages.
func (s *slot) sweepTTL(now time.Time) int {
	n := 0
	for _, g := range s.groups {
		for {
			m, ok := g.msgs.front()
			if !ok || !m.expired(now) {
				break
			}
			g.msgs.popFront()
			s.dropExpired(m)
			n++
		}
		if !g.inBand && !g.locked && !g.msgs.empty() {
			s.pushGroup(g)
		}
		s.dropGroupIfIdle(g)
	}
	if n > 0 {
		s.refreshHint()
	}
	return n
}

func (s *slot) releaseDelayed(now time.Time) int {
	n := 0
	for s.delayed.Len() > 0 && !s.delayed[0].at.After(now) {
		e := heap.Pop(&s.delayed).(delayEntry)
		e.msg.DeliverAfter = time.Time{}
		s.st.delayed--
		s.st.enqueued--                         // enqueue counts it again
		s.st.bytes -= int64(len(e.msg.Payload)) // and adds the bytes again
		s.enqueue(e.msg, now)
		n++
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
	s.st = slotStats{}
	s.refreshHint()

	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}
