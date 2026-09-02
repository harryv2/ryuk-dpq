package engine

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Key                 QueueKey
	VisibilityTimeout   time.Duration
	MaxRetries          uint32
	DefaultTTL          time.Duration
	StarvationThreshold time.Duration
	StarvationReserve   float64
	MaxDepth            int64
	Distributed         bool
}

func (c *Config) applyDefaults() {
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 30 * time.Second
	}
	if c.MaxRetries == 0 {
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
	cfg     Config
	clock   Clock
	cluster Cluster
	journal Journal

	slotsMu   sync.RWMutex
	slots     map[uint16]*slot
	localView atomic.Pointer[[]*slot]

	generation  uint64
	incarnation uint64

	counter  atomic.Uint64
	dispatch atomic.Uint64
	rr       atomic.Uint64
	cursor   atomic.Uint32
	depth    atomic.Int64
	escapes  atomic.Uint64
	frozen   atomic.Bool
}

func New(cfg Config, clk Clock, cl Cluster, j Journal, generation uint64) *Queue {
	cfg.applyDefaults()
	if clk == nil {
		clk = SystemClock{}
	}
	if cl == nil {
		cl = NewLocalCluster()
	}
	if j == nil {
		j = NoopJournal{}
	}
	q := &Queue{
		cfg:         cfg,
		clock:       clk,
		cluster:     cl,
		journal:     j,
		slots:       make(map[uint16]*slot),
		generation:  generation,
		incarnation: j.Incarnation(),
	}
	empty := []*slot{}
	q.localView.Store(&empty)
	return q
}

func (q *Queue) Config() Config      { return q.cfg }
func (q *Queue) Generation() uint64  { return q.generation }
func (q *Queue) Incarnation() uint64 { return q.incarnation }

// slotCount is the queue's shape. Fixed for its life, because a group key has
// to keep resolving to the same slot.
func (q *Queue) slotCount() int { return SlotCountFor(q.cfg.Distributed) }

// nextSeq prefixes the counter with the ownership generation so two owners can
// never hand out overlapping values, and messages merged back from an older
// owner sort ahead of anything the new owner accepted.
func (q *Queue) nextSeq() uint64 {
	return (q.generation << 40) | (q.counter.Add(1) & (1<<40 - 1))
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
	ids := q.cluster.LocalSlots(q.cfg.Key, q.slotCount())
	view := make([]*slot, 0, len(ids))
	for _, id := range ids {
		if s := q.slots[id]; s != nil {
			view = append(view, s)
		}
	}
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

// SlotFor exposes slot selection so a caller that has to decide before the
// request reaches the owning node gets the same answer.
func (q *Queue) SlotFor(groupID string) uint16 { return q.slotFor(groupID) }

func (q *Queue) slotFor(groupID string) uint16 {
	if groupID != "" {
		return q.cluster.SlotFor(q.cfg.Key, groupID, q.slotCount())
	}
	local := q.cluster.LocalSlots(q.cfg.Key, q.slotCount())
	if len(local) == 0 {
		return 0
	}
	return local[q.rr.Add(1)%uint64(q.fanout(len(local)))]
}

func (q *Queue) Enqueue(o EnqueueOptions) (*Message, error) {
	return q.EnqueueToSlot(q.slotFor(o.GroupID), o)
}

func (q *Queue) EnqueueToSlot(slotID uint16, o EnqueueOptions) (*Message, error) {
	if q.frozen.Load() {
		return nil, ErrFrozen
	}
	// A slot outside the range would be created and then never scanned by the
	// dispatcher, so the message would be written and never delivered. Refuse
	// it loudly instead.
	if int(slotID) >= q.slotCount() {
		return nil, ErrBadSlot
	}
	if !o.Priority.Valid() {
		return nil, ErrBadPriority
	}
	if q.cfg.MaxDepth > 0 && q.depth.Load() >= q.cfg.MaxDepth {
		return nil, ErrQueueFull
	}

	now := q.clock.Now()
	ttl := o.TTL
	if ttl == 0 {
		ttl = q.cfg.DefaultTTL
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
	s.mu.Lock()
	// Seq is assigned here, not earlier: the lock is the point that orders two
	// concurrent producers, so sequence numbers and list position have to be
	// decided together or a group can end up out of order.
	m.Seq = q.nextSeq()
	// Written before it is visible. The log append is a buffered write; the
	// flush happens off the lock.
	if err := q.journal.AppendEnqueue(slotID, m); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.enqueue(m, now)
	s.mu.Unlock()

	q.depth.Add(1)
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
	dead, err := s.nack(r, delay, q.clock.Now(), q.cfg.MaxRetries)
	s.mu.Unlock()
	if err == nil && dead != nil {
		q.depth.Add(-1)
		q.journal.AppendTerminal(r.Slot, dead.ID, TerminalDeadLettered)
	}
	return dead, err
}

type SweepResult struct {
	Redelivered  int
	Expired      int
	Released     int
	DeadLettered []*Message
}

// Sweep runs the three timers. Callable directly so tests drive the real code
// path without waiting for real time.
func (q *Queue) Sweep() SweepResult {
	now := q.clock.Now()
	var res SweepResult
	var depth int64

	q.slotsMu.RLock()
	slots := make([]*slot, 0, len(q.slots))
	for _, s := range q.slots {
		slots = append(slots, s)
	}
	q.slotsMu.RUnlock()

	for _, s := range slots {
		s.mu.Lock()
		res.Released += s.releaseDelayed(now)
		dead, requeued := s.sweepLeases(now, q.cfg.MaxRetries)
		res.Redelivered += requeued
		res.Expired += s.sweepTTL(now)
		st := s.stats(now)
		s.mu.Unlock()

		res.DeadLettered = append(res.DeadLettered, dead...)
		depth += st.ReadyTotal() + st.InFlight + st.Delayed
	}

	for _, m := range res.DeadLettered {
		q.journal.AppendTerminal(SlotOf(q.cfg.Key, m.group(), q.slotCount()), m.ID, TerminalDeadLettered)
	}
	q.depth.Store(depth)
	return res
}

func (q *Queue) Stats() Stats {
	now := q.clock.Now()
	var total Stats

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

// Absorb merges messages in, inserting by Seq rather than appending so an
// older generation lands ahead of anything already here.
func (q *Queue) Absorb(bySlot map[uint16][]*Message) error {
	now := q.clock.Now()
	var added int64

	for slotID, msgs := range bySlot {
		if len(msgs) == 0 {
			continue
		}
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq < msgs[j].Seq })

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
			for _, m := range list {
				s.st.enqueued++
				s.st.ready[bucketOf(m.Priority)]++
				s.st.bytes += int64(len(m.Payload))
			}
			// The head may have changed, so the band entry must be reissued.
			g.version++
			g.inBand = false
			if !g.locked {
				s.pushGroup(g)
			}
		}
		for _, m := range delayed {
			s.enqueue(m, now)
		}
		s.refreshHint()
		s.mu.Unlock()

		added += int64(len(msgs))
	}

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
