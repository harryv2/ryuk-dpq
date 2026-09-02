package engine

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func benchQueue(slots int) *Queue {
	all := make([]uint16, slots)
	for i := range all {
		all[i] = uint16(i)
	}
	cfg := Config{
		Key:               QueueKey{Org: "o", Name: "q"},
		VisibilityTimeout: time.Hour,
		MaxRetries:        3,
		StarvationReserve: 0,
	}
	return New(cfg, SystemClock{}, &fixedCluster{all: all, n: slots}, NoopJournal{}, 1)
}

// fixedCluster pins the slot count so the benchmark can vary it.
type fixedCluster struct {
	all []uint16
	n   int
}

func (c *fixedCluster) SlotFor(q QueueKey, groupID string, _ int) uint16 {
	return uint16(SlotOf(q, groupID, c.n)) % uint16(c.n)
}
func (c *fixedCluster) LocalSlots(QueueKey, int) []uint16 { return c.all }

// BenchmarkRoundTrip measures enqueue, dequeue and ack under contention, which
// is what the slot lock actually guards.
func BenchmarkRoundTrip(b *testing.B) {
	for _, slots := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			q := benchQueue(slots)
			var n atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := n.Add(1)
					_, err := q.Enqueue(EnqueueOptions{
						Payload:  []byte("x"),
						Priority: Priority(i % 101),
						GroupID:  fmt.Sprintf("g%d", i%256),
					})
					if err != nil {
						b.Fatal(err)
					}
					if _, r, ok := q.Dequeue(); ok {
						_ = q.Ack(r)
					}
				}
			})
		})
	}
}
