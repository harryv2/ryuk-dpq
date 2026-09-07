package engine

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func benchQueue() *Queue {
	cfg := Config{
		Key:               QueueKey{Org: "o", Name: "q"},
		VisibilityTimeout: time.Hour,
		MaxRetries:        3,
		StarvationReserve: 0,
		Distributed:       true,
	}
	return New(cfg, SystemClock{}, NoopJournal{}, 1)
}

// BenchmarkRoundTrip measures enqueue, dequeue and ack under contention, which
// is what the slot lock actually guards. Spreading the load over more slots is
// what buys the parallelism, so that is the variable.
func BenchmarkRoundTrip(b *testing.B) {
	for _, slots := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			q := benchQueue()
			var n atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := n.Add(1)
					// The gateway picks the slot in production; here the
					// benchmark does, so it can vary how wide the load spreads.
					_, err := q.EnqueueToSlot(uint16(i%uint64(slots)), EnqueueOptions{
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
