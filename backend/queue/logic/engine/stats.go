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

type Stats struct {
	Ready     [3]int64
	InFlight  int64
	Delayed   int64
	Bytes     int64
	OldestAge time.Duration

	Enqueued     uint64
	Acked        uint64
	Expired      uint64
	Requeued     uint64
	DeadLettered uint64
	Escapes      uint64
}

func (s *Stats) ReadyTotal() int64 { return s.Ready[0] + s.Ready[1] + s.Ready[2] }
