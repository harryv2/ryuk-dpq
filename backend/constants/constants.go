// Package constants holds values shared across both services.
package constants

import "time"

const (
	// Fixed for the life of a queue: a group key has to keep resolving to the
	// same slot. A normal queue's slots are only lock stripes and the benchmark
	// is flat past four; a distributed queue's count is also the ceiling on how
	// many machines it can use. engine.SlotsPerQueue and engine.MaxSlotsPerQueue
	// must match, and a test in queue/logic asserts it.
	SlotsPerQueue            = 16
	SlotsPerDistributedQueue = 64

	MaxPayloadBytes = 256 << 10
	MaxWaitTime     = 20 * time.Second
	MaxDequeueBatch = 10

	DefaultVisibilityTimeout = 30 * time.Second
	DefaultMaxRetries        = 3
	DefaultStarvationReserve = 0.2
)
