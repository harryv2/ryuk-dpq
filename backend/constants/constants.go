// Package constants holds values shared across both services.
package constants

import "time"

const (
	// Slot counts are fixed for the life of a queue: a group key has to resolve
	// to the same slot forever, so resplitting would break ordering.
	//
	// A normal queue's slots are only lock stripes, and a benchmark of the full
	// enqueue, dequeue and acknowledge cycle shows the gain is flat past four
	// (1707ns at one slot, 1187 at four, 1171 at sixteen). Sixteen sits past
	// that knee with margin for a machine with many cores.
	//
	// A distributed queue's count is also the ceiling on how many machines it
	// can use, so it is larger. Fan-out costs are bounded by machine count
	// rather than slot count, so the larger number is close to free.
	// engine.SlotsPerQueue and engine.MaxSlotsPerQueue must match these. The
	// engine imports nothing, so a test in queue/logic asserts they agree
	// rather than sharing a declaration.
	SlotsPerQueue            = 16
	SlotsPerDistributedQueue = 64

	MaxPayloadBytes = 256 << 10
	MaxWaitTime     = 20 * time.Second
	MaxDequeueBatch = 10

	DefaultVisibilityTimeout = 30 * time.Second
	DefaultMaxRetries        = 3
	DefaultStarvationReserve = 0.2
)
