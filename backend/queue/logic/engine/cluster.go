package engine

import (
	"hash/fnv"
	"strconv"
	"sync"
)

// Must match constants.SlotsPerQueue and SlotsPerDistributedQueue. The engine
// imports nothing outside the standard library, so a test in queue/logic asserts
// it instead. If they drift, a message lands in a slot nothing scans.
const (
	SlotsPerQueue    = 16
	MaxSlotsPerQueue = 64
)

// SlotCountFor never changes for a given queue: a group key has to keep
// resolving to the same slot.
func SlotCountFor(distributed bool) int {
	if distributed {
		return MaxSlotsPerQueue
	}
	return SlotsPerQueue
}

// Cluster tells the engine how a queue is laid out. LocalSlots is where the
// distributed flag lands: the dispatcher can only compare what it is given.
type Cluster interface {
	SlotFor(q QueueKey, groupID string, slots int) uint16
	LocalSlots(q QueueKey, slots int) []uint16
}

// LocalCluster owns every slot, which is what a single node does.
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

// SlotOf hashes org, queue and group together. The separators stop org "a"
// queue "bc" colliding with org "ab" queue "c".
func SlotOf(q QueueKey, groupID string, slots int) uint16 {
	h := fnv.New64a()
	h.Write([]byte(q.Org))
	h.Write([]byte{0})
	h.Write([]byte(q.Name))
	h.Write([]byte{0})
	h.Write([]byte(groupID))
	return uint16(h.Sum64() % uint64(slots))
}

// OwnerKeyFor is the key rendezvous hashing places. A normal queue places as a
// whole; a distributed queue places each slot independently.
func OwnerKeyFor(q QueueKey, slot uint16, distributed bool) string {
	if !distributed {
		return q.String()
	}
	return q.String() + "/slot-" + strconv.Itoa(int(slot))
}
