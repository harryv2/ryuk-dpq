package engine

import (
	"container/heap"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

type bandEntry struct {
	g   *group
	ver uint64
}

type lease struct {
	msg      *Message
	g        *group
	epoch    uint64
	deadline time.Time
}

// slot holds messages for one queue. One lock covers everything in it: handing
// out a message mutates five structures and they have to move together.
type slot struct {
	id uint16

	mu       sync.Mutex
	bandMask [2]uint64
	bands    map[Priority]*deque[bandEntry]
	groups   map[string]*group
	inflight map[string]*lease
	timers   timerHeap
	delayed  delayHeap
	epochSeq uint64
	st       slotStats

	// read without the lock by the dispatcher
	hint    atomic.Uint32 // highest non-empty band + 1, 0 means empty
	headSeq atomic.Uint64

	// A slot being handed to another node stops changing: no delivery, no
	// enqueue, no timers.
	frozen atomic.Bool
}

func newSlot(id uint16) *slot {
	s := &slot{
		id:       id,
		bands:    make(map[Priority]*deque[bandEntry]),
		groups:   make(map[string]*group),
		inflight: make(map[string]*lease),
	}
	s.headSeq.Store(math.MaxUint64)
	return s
}

func (s *slot) setBand(p Priority)   { s.bandMask[p>>6] |= 1 << (p & 63) }
func (s *slot) clearBand(p Priority) { s.bandMask[p>>6] &^= 1 << (p & 63) }

// highestBand is a superset: a set bit means the band deque is non-empty, not
// that it will yield a message. Never a false negative.
func (s *slot) highestBand() (Priority, bool) {
	if w := s.bandMask[1]; w != 0 {
		return Priority(127 - bits.LeadingZeros64(w)), true
	}
	if w := s.bandMask[0]; w != 0 {
		return Priority(63 - bits.LeadingZeros64(w)), true
	}
	return 0, false
}

func (s *slot) pushGroup(g *group) {
	m, ok := g.msgs.front()
	if !ok || g.locked {
		return
	}
	g.version++
	g.band = m.Priority
	g.inBand = true

	d := s.bands[m.Priority]
	if d == nil {
		d = &deque[bandEntry]{}
		s.bands[m.Priority] = d
	}
	d.pushBack(bandEntry{g: g, ver: g.version})
	s.setBand(m.Priority)
}

func (s *slot) takeGroup(p Priority) *group {
	d := s.bands[p]
	if d == nil {
		s.clearBand(p)
		return nil
	}
	for {
		e, ok := d.popFront()
		if !ok {
			break
		}
		// A stale entry means the group already has a newer live one. Clearing
		// inBand here would let pushGroup add a second and dispatch it twice.
		if e.ver != e.g.version {
			continue
		}
		e.g.inBand = false
		if e.g.locked || e.g.msgs.empty() {
			continue
		}
		if d.empty() {
			s.dropBand(p)
		}
		return e.g
	}
	s.dropBand(p)
	return nil
}

func (s *slot) dropBand(p Priority) {
	delete(s.bands, p)
	s.clearBand(p)
}

func (s *slot) enqueue(m *Message, now time.Time) {
	s.st.enqueued++
	if !m.DeliverAfter.IsZero() && m.DeliverAfter.After(now) {
		s.hold(m, stAbsent)
		return
	}
	s.move(m, stAbsent, stReady)
	s.admit(m)
	s.refreshHint()
}

func (s *slot) hold(m *Message, from msgState) {
	heap.Push(&s.delayed, delayEntry{msg: m, at: m.DeliverAfter})
	s.move(m, from, stDelayed)
}

// admit expects the caller to have recorded the move already.
func (s *slot) admit(m *Message) {
	gid := m.group()
	g := s.groups[gid]
	if g == nil {
		g = &group{id: gid}
		s.groups[gid] = g
	}
	g.msgs.pushBack(m)
	if !g.locked && !g.inBand {
		s.pushGroup(g)
	}
}

func (s *slot) take(p Priority, now time.Time, vis time.Duration) (*Message, Receipt, bool) {
	g := s.takeGroup(p)
	if g == nil {
		s.refreshHint()
		return nil, Receipt{}, false
	}

	var m *Message
	for {
		v, ok := g.msgs.popFront()
		if !ok {
			break
		}
		if v.expired(now) {
			s.dropExpired(v)
			continue
		}
		m = v
		break
	}
	if m == nil {
		s.dropGroupIfIdle(g)
		s.refreshHint()
		return nil, Receipt{}, false
	}

	m.Attempts++
	s.epochSeq++
	g.locked = true

	l := &lease{msg: m, g: g, epoch: s.epochSeq, deadline: now.Add(vis)}
	s.inflight[m.ID] = l
	heap.Push(&s.timers, timerEntry{id: m.ID, epoch: l.epoch, at: l.deadline})

	s.move(m, stReady, stInFlight)
	s.refreshHint()

	// A copy, not the live message: the slot keeps mutating the original after
	// this lock is gone, and sharing the pointer is a data race.
	out := *m
	return &out, Receipt{Slot: s.id, MessageID: m.ID, Epoch: l.epoch}, true
}

func (s *slot) ack(r Receipt) error {
	l, ok := s.inflight[r.MessageID]
	if !ok {
		return ErrNotInFlight
	}
	// The lease moved on. Accepting this would delete a message another worker
	// is processing.
	if l.epoch != r.Epoch {
		return ErrLeaseExpired
	}
	delete(s.inflight, r.MessageID)
	s.move(l.msg, stInFlight, stAbsent)
	s.st.acked++
	s.unlock(l.g)
	s.refreshHint()
	return nil
}

func (s *slot) unlock(g *group) {
	g.locked = false
	if g.msgs.empty() {
		s.dropGroupIfIdle(g)
		return
	}
	if !g.inBand {
		s.pushGroup(g)
	}
}

func (s *slot) dropGroupIfIdle(g *group) {
	if !g.locked && g.msgs.empty() && !g.inBand {
		delete(s.groups, g.id)
	}
}

func (s *slot) dropExpired(m *Message) {
	s.move(m, stReady, stAbsent)
	s.st.expired++
}

// headMessage returns the first deliverable message of a band without removing
// anything. Used by the hint and the starvation scan.
func (s *slot) headMessage(p Priority) (*Message, bool) {
	d := s.bands[p]
	if d == nil {
		return nil, false
	}
	limit := d.len()
	if limit > 8 {
		limit = 8 // it is a hint, not an answer
	}
	for i := 0; i < limit; i++ {
		e := d.at(i)
		if e.ver != e.g.version || e.g.locked {
			continue
		}
		if m, ok := e.g.msgs.front(); ok {
			return m, true
		}
	}
	return nil, false
}

func (s *slot) refreshHint() {
	p, ok := s.highestBand()
	if !ok {
		s.hint.Store(0)
		s.headSeq.Store(math.MaxUint64)
		return
	}
	s.hint.Store(uint32(p) + 1)
	if m, ok := s.headMessage(p); ok {
		s.headSeq.Store(m.Seq)
	} else {
		s.headSeq.Store(math.MaxUint64)
	}
}

// oldestBandBefore walks bands from the lowest priority up, which is where
// starvation happens, and returns the first whose head has waited too long.
func (s *slot) oldestBandBefore(cutoff time.Time) (Priority, bool) {
	for w := 0; w < 2; w++ {
		word := s.bandMask[w]
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			word &^= 1 << uint(bit)
			p := Priority(w*64 + bit)
			if m, ok := s.headMessage(p); ok && m.EnqueuedAt.Before(cutoff) {
				return p, true
			}
		}
	}
	return 0, false
}

func (s *slot) stats(now time.Time) Stats {
	st := Stats{
		Ready:        s.st.ready,
		InFlight:     s.st.inflight,
		Delayed:      s.st.delayed,
		Bytes:        s.st.bytes,
		Enqueued:     s.st.enqueued,
		Acked:        s.st.acked,
		Expired:      s.st.expired,
		Requeued:     s.st.requeued,
		DeadLettered: s.st.deadLettered,
		TopReady:     -1,
	}
	// Every group, not every band head: a redelivered group goes to the back of
	// its band, so a head scan misses exactly the messages that have waited
	// longest. Within a group the front is the oldest, so one probe each covers
	// the whole queue.
	for _, g := range s.groups {
		m, ok := g.msgs.front()
		if !ok || m.expired(now) {
			continue
		}
		if age := now.Sub(m.EnqueuedAt); age > st.OldestAge {
			st.OldestAge = age
		}
		// Not the band mask: a band keeps its bit until something pops the stale
		// entry, so the mask names priorities this slot can no longer serve and
		// the gateway ranks the node on work that is not there. A locked group
		// is not servable either, however old its front is.
		if !g.locked && int16(m.Priority) > st.TopReady {
			st.TopReady = int16(m.Priority)
		}
	}
	if st.OldestAge < 0 {
		st.OldestAge = 0
	}
	return st
}
