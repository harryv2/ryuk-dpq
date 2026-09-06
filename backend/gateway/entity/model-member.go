package entity

import (
	"hash/fnv"
	"sort"
)

// Member is a live node.
type Member struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// CandidatesFor is the set of machines a queue may use: the top `width` of the
// same ranking OwnerFor picks its winner from.
func CandidatesFor(key string, members []Member, width int) []Member {
	if width <= 0 || width >= len(members) {
		return members
	}
	ranked := make([]Member, len(members))
	copy(ranked, members)

	kh := hash64(key)
	sort.Slice(ranked, func(i, j int) bool {
		si, sj := mix(kh, hash64(ranked[i].ID)), mix(kh, hash64(ranked[j].ID))
		if si != sj {
			return si > sj
		}
		return ranked[i].ID < ranked[j].ID // stable when scores tie
	})
	return ranked[:width]
}

// OwnerFor is rendezvous hashing: the member scoring highest wins, so every
// gateway agrees without electing anything, and adding a machine moves about
// 1/(n+1) of the keys where a modulo would move nearly all of them.
func OwnerFor(key string, members []Member) (Member, bool) {
	var best Member
	var bestScore uint64
	found := false

	kh := hash64(key)
	for _, m := range members {
		if s := mix(kh, hash64(m.ID)); !found || s > bestScore {
			best, bestScore, found = m, s, true
		}
	}
	return best, found
}

func hash64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// mix runs the combined hashes through splitmix64.
func mix(a, b uint64) uint64 {
	x := a ^ (b * 0x9E3779B97F4A7C15)
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return x
}

// OwnerKey is what gets hashed. A normal queue places as a whole; a distributed
// queue places each slot on its own, which is what spreads it across machines.
func OwnerKey(org, name string, slot int, distributed bool) string {
	k := org + "/" + name
	if !distributed {
		return k
	}
	return k + "/slot-" + itoa(slot)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
