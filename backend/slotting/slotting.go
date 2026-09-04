package slotting

import "hash/fnv"

const (
	PerQueue            = 16
	PerDistributedQueue = 64
)

func CountFor(distributed bool) int {
	if distributed {
		return PerDistributedQueue
	}
	return PerQueue
}

// SlotFor hashes org, queue and group together. The separators stop org "a"
// queue "bc" colliding with org "ab" queue "c".
//
// An empty group is not handled here: what an ungrouped message should do is a
// policy the caller owns, and the two callers want different things.
func SlotFor(org, name, group string, slots int) uint16 {
	h := fnv.New64a()
	h.Write([]byte(org))
	h.Write([]byte{0})
	h.Write([]byte(name))
	h.Write([]byte{0})
	h.Write([]byte(group))
	return uint16(h.Sum64() % uint64(slots))
}
