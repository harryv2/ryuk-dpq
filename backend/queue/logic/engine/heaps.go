package engine

import "time"

type timerEntry struct {
	id    string
	epoch uint64
	at    time.Time
}

// timerHeap orders leases by deadline. Entries are never removed on ack; they
// are dropped when they surface with a stale epoch.
type timerHeap []timerEntry

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)        { *h = append(*h, x.(timerEntry)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

type delayEntry struct {
	msg *Message
	at  time.Time
}

type delayHeap []delayEntry

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *delayHeap) Push(x any)        { *h = append(*h, x.(delayEntry)) }
func (h *delayHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}
