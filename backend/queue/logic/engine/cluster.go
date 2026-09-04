package engine

import (
	"strconv"
	"sync"

	"github.com/harryv2/ryuk-dpq/backend/slotting"
)

const (
	SlotsPerQueue    = slotting.PerQueue
	MaxSlotsPerQueue = slotting.PerDistributedQueue
)

func SlotCountFor(distributed bool) int { return slotting.CountFor(distributed) }

// Cluster tells the engine how a queue is laid out. LocalSlots is where the
// distributed flag lands: the dispatcher can only compare what it is given.
type Cluster interface {
	SlotFor(q QueueKey, groupID string, slots int) uint16
	LocalSlots(q QueueKey, slots int) []uint16
}

type LocalCluster struct {
	mu  sync.Mutex
	all map[int][]uint16
}

func NewLocalCluster() *LocalCluster {
	return &LocalCluster{all: map[int][]uint16{}}
}

func (c *LocalCluster) SlotFor(q QueueKey, groupID string, slots int) uint16 {
	return SlotOf(q, groupID, slots)
}

func (c *LocalCluster) LocalSlots(_ QueueKey, slots int) []uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.all[slots]; ok {
		return v
	}
	v := make([]uint16, slots)
	for i := range v {
		v[i] = uint16(i)
	}
	c.all[slots] = v
	return v
}

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
