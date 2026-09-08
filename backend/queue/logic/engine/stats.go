package engine

import "time"

type slotStats struct {
	ready    [3]int64
	inflight int64
	delayed  int64
	bytes    int64

	enqueued     uint64
	acked        uint64
	expired      uint64
	requeued     uint64
	deadLettered uint64
}

// Where a message is. Exactly one at a time, so the gauges are a partition.
type msgState uint8

const (
	stAbsent msgState = iota
	stReady
	stInFlight
	stDelayed
)

// resetGauges empties the slot without rewriting its history, so handing slots
// away does not take the lifetime totals backwards.
func (t *slotStats) resetGauges() {
	t.ready = [3]int64{}
	t.inflight = 0
	t.delayed = 0
	t.bytes = 0
}

// move is both sides of a transition, so a caller cannot adjust one gauge and
// forget the other. Callers hold s.mu.
func (s *slot) move(m *Message, from, to msgState) {
	switch from {
	case stReady:
		s.st.ready[bucketOf(m.Priority)]--
	case stInFlight:
		s.st.inflight--
	case stDelayed:
		s.st.delayed--
	case stAbsent:
		s.st.bytes += int64(len(m.Payload))
	}
	switch to {
	case stReady:
		s.st.ready[bucketOf(m.Priority)]++
	case stInFlight:
		s.st.inflight++
	case stDelayed:
		s.st.delayed++
	case stAbsent:
		s.st.bytes -= int64(len(m.Payload))
	}
}

type Stats struct {
	Ready     [3]int64
	InFlight  int64
	Delayed   int64
	Bytes     int64
	OldestAge time.Duration
	// -1 when nothing is ready. Ready is only bucketed, so this is what lets the
	// gateway rank nodes by the priority they actually hold.
	TopReady int16

	Enqueued     uint64
	Acked        uint64
	Expired      uint64
	Requeued     uint64
	DeadLettered uint64
	Escapes      uint64
}

func (s *Stats) ReadyTotal() int64 { return s.Ready[0] + s.Ready[1] + s.Ready[2] }
