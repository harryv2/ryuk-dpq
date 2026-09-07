package engine

import (
	"strconv"

	"github.com/harryv2/ryuk-dpq/backend/slotting"
)

const (
	SlotsPerQueue    = slotting.PerQueue
	MaxSlotsPerQueue = slotting.PerDistributedQueue
)

func SlotCountFor(distributed bool) int { return slotting.CountFor(distributed) }

func SlotOf(q QueueKey, groupID string, slots int) uint16 {
	return slotting.SlotFor(q.Org, q.Name, groupID, slots)
}

// OwnerKeyFor is the key rendezvous hashing places. A normal queue places as a
// whole; a distributed queue places each slot independently.
func OwnerKeyFor(q QueueKey, slot uint16, distributed bool) string {
	if !distributed {
		return q.String()
	}
	return q.String() + "/slot-" + strconv.Itoa(int(slot))
}
