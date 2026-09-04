// Package constants holds values shared across both services.
package constants

import (
	"time"

	"github.com/harryv2/ryuk-dpq/backend/slotting"
)

const (
	// A normal queue's slots are only lock stripes; a distributed queue's count
	// is also the ceiling on how many machines it can use.
	SlotsPerQueue            = slotting.PerQueue
	SlotsPerDistributedQueue = slotting.PerDistributedQueue

	// DefaultPlacementWidth bounds how many machines one distributed queue uses.
	// Six is past the point where partial availability means anything, and well
	// short of making every queue a neighbour of every other.
	DefaultPlacementWidth = 6
	MinPlacementWidth     = 2
	MaxPlacementWidth     = 64

	MaxPayloadBytes = 256 << 10
	MaxWaitTime     = 20 * time.Second
	MaxDequeueBatch = 10

	DefaultVisibilityTimeout = 30 * time.Second
	DefaultMaxRetries        = 3
	DefaultStarvationReserve = 0.2
)
