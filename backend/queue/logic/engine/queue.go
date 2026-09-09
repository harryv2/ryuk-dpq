package engine

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Key               QueueKey
	VisibilityTimeout time.Duration
	MaxRetries        uint32
	// Tells an explicit 0, meaning dead-letter on the first failure, from a
	// caller that set nothing.
	MaxRetriesSet       bool
	DefaultTTL          time.Duration
	StarvationThreshold time.Duration
	StarvationReserve   float64
	// StarvationAvoidanceEnabled turns the reserve on. Off, delivery is strictly
	// by priority and then by Seq; on, a share of deliveries goes to whatever has
	// waited past the threshold instead.
	StarvationAvoidanceEnabled bool
	MaxDepth                   int64
	Distributed                bool
	// HasDeadLetter says whether failures have anywhere to go.
	HasDeadLetter bool
}

func (c *Config) applyDefaults() {
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 30 * time.Second
	}
	if !c.MaxRetriesSet && c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.StarvationReserve <= 0 || c.StarvationReserve > 1 {
		c.StarvationReserve = 0.2
	}
	if c.StarvationThreshold <= 0 {
		if c.DefaultTTL > 0 {
			c.StarvationThreshold = c.DefaultTTL / 4
		} else {
			c.StarvationThreshold = time.Minute
		}
	}
}

type EnqueueOptions struct {
	Payload      []byte
	Priority     Priority
	GroupID      string
	TTL          time.Duration
	DeliverAfter time.Duration
}

type Queue struct {
	// Settings can change while the queue is running and are read on the
	// delivery path, so they are swapped whole rather than field by field.
	conf    atomic.Pointer[Config]
	clock   Clock
	journal Journal

	slotsMu   sync.RWMutex
	slots     map[uint16]*slot
	localView atomic.Pointer[[]*slot]

	incarnation uint64

	seq      atomic.Uint64
	dispatch atomic.Uint64
	rr       atomic.Uint64
	cursor   atomic.Uint32
	depth    atomic.Int64
	// Enqueues that have claimed room but not landed. The cap has to see these
	// as well as depth, or producers racing between the two both get in.
	reserved atomic.Int64
	escapes  atomic.Uint64
	frozen   atomic.Bool
}

func New(cfg Config, clk Clock, j Journal, generation uint64) *Queue {
	cfg.applyDefaults()
	if clk == nil {
		clk = SystemClock{}
	}
	if j == nil {
		j = NoopJournal{}
	}
	q := &Queue{
		clock:       clk,
		journal:     j,
		slots:       make(map[uint16]*slot),
		incarnation: j.Incarnation(),
	}
	q.seq.Store(generation << generationShift)
	q.conf.Store(&cfg)
	empty := []*slot{}
	q.localView.Store(&empty)
	return q
}

func (q *Queue) cfg() *Config        { return q.conf.Load() }
func (q *Queue) Config() Config      { return *q.conf.Load() }
func (q *Queue) Generation() uint64  { return q.seq.Load() >> generationShift }
func (q *Queue) Incarnation() uint64 { return q.incarnation }

// Reconfigure swaps the settings of a running queue.
func (q *Queue) Reconfigure(c Config) bool {
	cur := q.conf.Load()
	c.Key = cur.Key
	// The shape decides the slot count, so it is fixed for a queue's life and
	// the API refuses to change it. It can still be learned: a queue rebuilt
	// from its log has no shape until the first request names one.
	c.Distributed = cur.Distributed || c.Distributed
	c.applyDefaults()
	if c == *cur {
		return false
	}
	q.conf.Store(&c)
	return true
}

// slotCount is the queue's shape. Fixed for its life, because a group key has
// to keep resolving to the same slot.
func (q *Queue) slotCount() int { return SlotCountFor(q.cfg().Distributed) }

// generationShift splits a sequence number: ownership generation above,
// message counter below.
const generationShift = 40

// nextSeq prefixes the counter with the ownership generation, so two owners
// never hand out overlapping values and an older owner's messages sort first.
func (q *Queue) nextSeq() uint64 { return q.seq.Add(1) }

// adoptSeq keeps the next sequence number above seq. Messages arriving from a
// log or another owner keep the numbering they were given, so new arrivals have
// to continue past it rather than start again underneath and sort ahead.
func (q *Queue) adoptSeq(seq uint64) {
	for {
		cur := q.seq.Load()
		if seq <= cur {
			return
		}
		if q.seq.CompareAndSwap(cur, seq) {
			return
		}
	}
}

func (q *Queue) slot(id uint16) *slot {
	q.slotsMu.RLock()
	s := q.slots[id]
	q.slotsMu.RUnlock()
	if s != nil {
		return s
	}

	q.slotsMu.Lock()
	defer q.slotsMu.Unlock()
	if s = q.slots[id]; s == nil {
		s = newSlot(id)
		q.slots[id] = s
		q.rebuildLocalView()
	}
	return s
}

// caller holds slotsMu for write
func (q *Queue) rebuildLocalView() {
	view := make([]*slot, 0, len(q.slots))
	for _, s := range q.slots {
		view = append(view, s)
	}
	// Sorted so the starvation scan's round-robin covers slots evenly; map
	// order would reshuffle it on every rebuild.
	sort.Slice(view, func(i, j int) bool { return view[i].id < view[j].id })
	q.localView.Store(&view)
}

func (q *Queue) localSlots() []*slot { return *q.localView.Load() }

// fanout grows the slots an ungrouped message can land in with queue depth, so
// a small queue stays in one slot instead of scattering over all sixteen.
func (q *Queue) fanout(n int) int {
	d := q.depth.Load()
	want := 1
	for want < n && int64(want)*msgsPerSlotTarget < d {
		want *= 2
	}
	return want
}

const msgsPerSlotTarget = 1000

func (q *Queue) slotFor(groupID string) uint16 {
	if groupID != "" {
		return SlotOf(q.cfg().Key, groupID, q.slotCount())
	}
	n := q.slotCount()
	return uint16(q.rr.Add(1) % uint64(q.fanout(n)))
}

func (q *Queue) Enqueue(o EnqueueOptions) (*Message, error) {
	return q.EnqueueToSlot(q.slotFor(o.GroupID), o)
}

func (q *Queue) EnqueueToSlot(slotID uint16, o EnqueueOptions) (*Message, error) {
	if q.frozen.Load() {
		return nil, ErrFrozen
	}
	// A slot past the end would be created and never scanned, so the message
	// would be written and never delivered.
	if int(slotID) >= q.slotCount() {
		return nil, ErrBadSlot
	}
	if !o.Priority.Valid() {
		return nil, ErrBadPriority
	}
	// Claimed before the message is built, not counted after it lands: the
	// claim numbers this caller among the enqueues in flight, so exactly the
	// ones that fit get through. Every path out from here gives it back.
	pending := q.reserved.Add(1)
	if max := q.cfg().MaxDepth; max > 0 && q.depth.Load()+pending > max {
		q.reserved.Add(-1)
		return nil, ErrQueueFull
	}

	now := q.clock.Now()
	ttl := o.TTL
	if ttl == 0 {
		ttl = q.cfg().DefaultTTL
	}

	m := &Message{
		ID:         newID(),
		Payload:    o.Payload,
		Priority:   o.Priority,
		GroupID:    o.GroupID,
		EnqueuedAt: now,
	}
	if ttl > 0 {
		m.ExpiresAt = now.Add(ttl)
	}
	if o.DeliverAfter > 0 {
		m.DeliverAfter = now.Add(o.DeliverAfter)
	}

	s := q.slot(slotID)
	if s.frozen.Load() {
		// Being handed to another node. The caller re-reads placement and
		// retries, so the message lands wherever the slot ends up.
		q.reserved.Add(-1)
		return nil, ErrFrozen
	}
	s.mu.Lock()
	// Assigned under the lock: it is what orders two concurrent producers, so
	// the number and the list position have to be decided together.
	m.Seq = q.nextSeq()
	// Written before it is visible. The log append is a buffered write; the
	// flush happens off the lock.
	if err := q.journal.AppendEnqueue(slotID, m); err != nil {
		s.mu.Unlock()
		q.reserved.Add(-1)
		return nil, err
	}
	s.enqueue(m, now)
	s.mu.Unlock()

	// In this order, so the message is never missing from both at once.
	q.depth.Add(1)
	q.reserved.Add(-1)
	return m, nil
}

func (q *Queue) Ack(r Receipt) error {
	if r.Incarnation != q.incarnation {
		return ErrLeaseExpired
	}
	s := q.slot(r.Slot)
	s.mu.Lock()
	err := s.ack(r)
	s.mu.Unlock()
	if err == nil {
		q.depth.Add(-1)
		q.journal.AppendTerminal(r.Slot, r.MessageID, TerminalAck)
	}
	return err
}

func (q *Queue) Nack(r Receipt, delay time.Duration) (*Message, error) {
	if r.Incarnation != q.incarnation {
		return nil, ErrLeaseExpired
	}
	s := q.slot(r.Slot)
	s.mu.Lock()
	c := q.cfg()
	dead, err := s.nack(r, delay, q.clock.Now(), c.MaxRetries, c.HasDeadLetter)
	s.mu.Unlock()
	if err == nil && dead != nil {
		q.depth.Add(-1)
		q.journal.AppendTerminal(r.Slot, dead.ID, TerminalDeadLettered)
	}
	return dead, err
}

// DeadLetter carries the slot the message was in.
type DeadLetter struct {
	Slot uint16
	Msg  *Message
}

type SweepResult struct {
	Redelivered  int
	Expired      int
	Released     int
	DeadLettered []DeadLetter
}

// Sweep runs the three timers. Callable directly so tests drive the real code
// path without waiting for real time.
func (q *Queue) Sweep() SweepResult {
	now := q.clock.Now()
	var res SweepResult
	var removed int64

	q.slotsMu.RLock()
	slots := make([]*slot, 0, len(q.slots))
	for _, s := range q.slots {
		slots = append(slots, s)
	}
	q.slotsMu.RUnlock()

	for _, s := range slots {
		if s.frozen.Load() {
			continue // its messages are being moved; timers would mutate them
		}
		s.mu.Lock()
		// Expiry and dead-lettering are the only ways a message leaves without
		// its remover adjusting depth, so the sweep counts its own removals.
		gone := s.st.expired + s.st.deadLettered
		res.Released += s.releaseDelayed(now)
		dead, requeued := s.sweepLeases(now, q.cfg().MaxRetries, q.cfg().HasDeadLetter)
		res.Redelivered += requeued
		res.Expired += s.sweepTTL(now)
		removed += int64(s.st.expired + s.st.deadLettered - gone)
		s.mu.Unlock()

		for _, m := range dead {
			res.DeadLettered = append(res.DeadLettered, DeadLetter{Slot: s.id, Msg: m})
		}
	}

	for _, d := range res.DeadLettered {
		q.journal.AppendTerminal(d.Slot, d.Msg.ID, TerminalDeadLettered)
	}
	// A delta, not a recount: a total read while producers run is stale by the
	// time it is written back, and either erases their enqueues or doubles them.
	q.depth.Add(-removed)
	return res
}

func (q *Queue) Stats() Stats {
	now := q.clock.Now()
	total := Stats{TopReady: -1}

	q.slotsMu.RLock()
	slots := make([]*slot, 0, len(q.slots))
	for _, s := range q.slots {
		slots = append(slots, s)
	}
	q.slotsMu.RUnlock()

	for _, s := range slots {
		s.mu.Lock()
		st := s.stats(now)
		s.mu.Unlock()

		for i := range st.Ready {
			total.Ready[i] += st.Ready[i]
		}
		total.InFlight += st.InFlight
		total.Delayed += st.Delayed
		total.Bytes += st.Bytes
		total.Enqueued += st.Enqueued
		total.Acked += st.Acked
		total.Expired += st.Expired
		total.Requeued += st.Requeued
		total.DeadLettered += st.DeadLettered
		if st.OldestAge > total.OldestAge {
			total.OldestAge = st.OldestAge // max across slots, never a sum
		}
		if st.TopReady > total.TopReady {
			total.TopReady = st.TopReady
		}
	}
	total.Escapes = q.escapes.Load()
	return total
}

// Freeze stops the queue serving and empties it, returning what it held keyed
// by slot so placement survives the move.
func (q *Queue) Freeze() map[uint16][]*Message {
	q.frozen.Store(true)

	q.slotsMu.RLock()
	slots := make([]*slot, 0, len(q.slots))
	for _, s := range q.slots {
		slots = append(slots, s)
	}
	q.slotsMu.RUnlock()

	out := make(map[uint16][]*Message)
	for _, s := range slots {
		s.mu.Lock()
		msgs := s.drain()
		s.mu.Unlock()
		if len(msgs) > 0 {
			out[s.id] = msgs
		}
	}
	q.depth.Store(0)
	return out
}

func (q *Queue) Thaw() { q.frozen.Store(false) }

// FreezeSlots stops the named slots changing and returns copies of what they
// hold.
func (q *Queue) FreezeSlots(ids []uint16) map[uint16][]*Message {
	out := make(map[uint16][]*Message, len(ids))
	for _, id := range ids {
		if int(id) >= q.slotCount() {
			continue
		}
		s := q.slot(id)
		s.frozen.Store(true)

		s.mu.Lock()
		msgs := s.copyAll()
		s.mu.Unlock()
		if len(msgs) > 0 {
			out[id] = msgs
		}
	}
	return out
}

// ThawSlots puts frozen slots back into service.
func (q *Queue) ThawSlots(ids []uint16) {
	for _, id := range ids {
		if int(id) < q.slotCount() {
			q.slot(id).frozen.Store(false)
		}
	}
}

// DropSlots removes slots this node no longer owns. Called only once the new
// owner is serving them.
func (q *Queue) DropSlots(ids []uint16) int {
	dropped := 0
	for _, id := range ids {
		if int(id) >= q.slotCount() {
			continue
		}
		s := q.slot(id)
		s.mu.Lock()
		dropped += len(s.drain())
		s.mu.Unlock()
		s.frozen.Store(false) // empty now; it may be handed back later
	}
	q.depth.Add(-int64(dropped))
	return dropped
}

// FrozenSlots lists slots currently held out of service, so the reconciler can
// spot a migration that stalled.
func (q *Queue) FrozenSlots() []uint16 {
	var out []uint16
	q.slotsMu.RLock()
	defer q.slotsMu.RUnlock()
	for id, s := range q.slots {
		if s.frozen.Load() {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// HeldSlots lists every slot with anything in it, which is what the reconciler
// compares against placement.
func (q *Queue) HeldSlots() []uint16 {
	var out []uint16
	q.slotsMu.RLock()
	defer q.slotsMu.RUnlock()
	for id, s := range q.slots {
		s.mu.Lock()
		empty := s.st.ready[0]+s.st.ready[1]+s.st.ready[2] == 0 &&
			s.st.inflight == 0 && s.st.delayed == 0
		s.mu.Unlock()
		if !empty {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Absorb merges messages in, inserting by Seq rather than appending so an
// older generation lands ahead of anything already here.
func (q *Queue) Absorb(bySlot map[uint16][]*Message) error {
	now := q.clock.Now()
	var added int64
	var maxSeq uint64

	for slotID, msgs := range bySlot {
		if len(msgs) == 0 {
			continue
		}
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq < msgs[j].Seq })
		if last := msgs[len(msgs)-1].Seq; last > maxSeq {
			maxSeq = last
		}

		byGroup := make(map[string][]*Message)
		var delayed []*Message
		for _, m := range msgs {
			if !m.DeliverAfter.IsZero() && m.DeliverAfter.After(now) {
				delayed = append(delayed, m)
				continue
			}
			byGroup[m.group()] = append(byGroup[m.group()], m)
		}

		s := q.slot(slotID)
		s.mu.Lock()
		for gid, list := range byGroup {
			g := s.groups[gid]
			if g == nil {
				g = &group{id: gid}
				s.groups[gid] = g
			}
			g.msgs.prepend(list)
			// Not a new submission: the same message moving between owners, or
			// coming back from the log after a restart.
			for _, m := range list {
				s.move(m, stAbsent, stReady)
			}
			// The head may have changed, so the band entry must be reissued.
			g.version++
			g.inBand = false
			if !g.locked {
				s.pushGroup(g)
			}
		}
		for _, m := range delayed {
			s.hold(m, stAbsent)
		}
		s.refreshHint()
		s.mu.Unlock()

		added += int64(len(msgs))
	}

	q.adoptSeq(maxSeq)
	q.depth.Add(added)
	return nil
}

// Snapshot copies what each slot holds without removing anything. Used to
// rewrite a log so it only contains what is still live.
func (q *Queue) Snapshot() (map[uint16][]*Message, error) {
	q.slotsMu.RLock()
	slots := make([]*slot, 0, len(q.slots))
	for _, s := range q.slots {
		slots = append(slots, s)
	}
	q.slotsMu.RUnlock()

	out := make(map[uint16][]*Message, len(slots))
	for _, s := range slots {
		s.mu.Lock()
		msgs := make([]*Message, 0, len(s.inflight))
		for _, l := range s.inflight {
			msgs = append(msgs, l.msg)
		}
		for _, g := range s.groups {
			for i := 0; i < g.msgs.len(); i++ {
				msgs = append(msgs, g.msgs.at(i))
			}
		}
		for _, d := range s.delayed {
			msgs = append(msgs, d.msg)
		}
		s.mu.Unlock()

		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq < msgs[j].Seq })
		out[s.id] = msgs
	}
	return out, nil
}
