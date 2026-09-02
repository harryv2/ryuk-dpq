package entity

import (
	"fmt"
	"testing"
)

func members(n int) []Member {
	out := make([]Member, n)
	for i := range out {
		out[i] = Member{ID: fmt.Sprintf("node-%d", i), Addr: fmt.Sprintf("n%d:9090", i)}
	}
	return out
}

func TestOwnerIsStableForTheSameMemberList(t *testing.T) {
	ms := members(5)
	first, ok := OwnerFor("org1/orders", ms)
	if !ok {
		t.Fatal("expected an owner")
	}
	for i := 0; i < 20; i++ {
		got, _ := OwnerFor("org1/orders", ms)
		if got.ID != first.ID {
			t.Fatal("placement must be a pure function of the member list")
		}
	}
}

func TestOwnerSpreadsAcrossMembers(t *testing.T) {
	ms := members(5)
	seen := map[string]int{}
	for i := 0; i < 200; i++ {
		m, _ := OwnerFor(fmt.Sprintf("org1/queue-%d", i), ms)
		seen[m.ID]++
	}
	if len(seen) != 5 {
		t.Fatalf("only %d of 5 members were used", len(seen))
	}
	for id, n := range seen {
		if n < 15 || n > 70 { // 200/5 = 40 expected, allow a wide band
			t.Fatalf("member %s got %d of 200, which is not a spread", id, n)
		}
	}
}

// Adding a machine should move about 1/(n+1) of the keys, not most of them.
// That is the property that makes rebalancing cheap.
func TestAddingAMemberMovesFewKeys(t *testing.T) {
	before := members(5)
	after := members(6)

	moved, total := 0, 500
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("org1/queue-%d", i)
		a, _ := OwnerFor(key, before)
		b, _ := OwnerFor(key, after)
		if a.ID != b.ID {
			moved++
		}
	}
	share := float64(moved) / float64(total)
	if share > 0.30 { // 1/6 is about 0.17
		t.Fatalf("adding one of six members moved %.0f%% of keys", share*100)
	}
	if moved == 0 {
		t.Fatal("a new member should take some keys")
	}
}

// A key must never move to a machine that was not the one it moved to for
// somebody else -- removing a member only redistributes what that member held.
func TestRemovingAMemberOnlyMovesItsKeys(t *testing.T) {
	before := members(5)
	after := before[:4] // node-4 leaves

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("org1/queue-%d", i)
		a, _ := OwnerFor(key, before)
		b, _ := OwnerFor(key, after)
		if a.ID != "node-4" && a.ID != b.ID {
			t.Fatalf("key %q moved from %s to %s but its owner was still alive", key, a.ID, b.ID)
		}
	}
}

func TestOwnerKeyDistinguishesSlots(t *testing.T) {
	whole := OwnerKey("org1", "events", 0, false)
	if whole != "org1/events" {
		t.Fatalf("a normal queue places as a whole, got %q", whole)
	}
	seen := map[string]bool{}
	for slot := 0; slot < 16; slot++ {
		k := OwnerKey("org1", "events", slot, true)
		if seen[k] {
			t.Fatalf("slot key %q is not unique", k)
		}
		seen[k] = true
	}
}

func TestNoMembersMeansNoOwner(t *testing.T) {
	if _, ok := OwnerFor("org1/orders", nil); ok {
		t.Fatal("an empty cluster has no owner")
	}
}
