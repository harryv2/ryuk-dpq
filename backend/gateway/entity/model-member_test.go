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

func TestCandidatesBarelyMoveWhenAMachineJoins(t *testing.T) {
	before := CandidatesFor("org1/orders", members(6), 4)
	after := CandidatesFor("org1/orders", members(7), 4)

	kept := 0
	for _, a := range after {
		for _, b := range before {
			if a.ID == b.ID {
				kept++
			}
		}
	}
	// Scores do not change when a machine joins, so it can only displace the
	// lowest-ranked candidate. Everything else stays put and nothing moves.
	if kept < 3 {
		t.Fatalf("one new machine displaced %d of 4 candidates", 4-kept)
	}
}

func TestCandidatesBoundHowManyMachinesAQueueUses(t *testing.T) {
	ms := members(20)
	for _, width := range []int{2, 4, 8} {
		seen := map[string]bool{}
		for slot := 0; slot < 64; slot++ {
			c := CandidatesFor("org1/orders", ms, width)
			m, _ := OwnerFor(OwnerKey("org1", "orders", slot, true), c)
			seen[m.ID] = true
		}
		if len(seen) > width {
			t.Fatalf("width %d: queue landed on %d machines", width, len(seen))
		}
	}
}

func TestCandidatesFallBackToTheWholeCluster(t *testing.T) {
	ms := members(3)
	for _, width := range []int{0, 3, 9} {
		if got := len(CandidatesFor("org1/orders", ms, width)); got != 3 {
			t.Fatalf("width %d: got %d candidates, want the whole cluster", width, got)
		}
	}
}

func TestCandidatesRankTheSameWayAsTheOwner(t *testing.T) {
	ms := members(12)
	winner, _ := OwnerFor("org1/orders", ms)
	if CandidatesFor("org1/orders", ms, 4)[0].ID != winner.ID {
		t.Fatal("the first candidate must be the machine OwnerFor picks")
	}
}

func placeHRW(ms []Member, slots int) map[int]string {
	out := map[int]string{}
	for s := 0; s < slots; s++ {
		m, _ := OwnerFor(OwnerKey("org1", "orders", s, true), ms)
		out[s] = m.ID
	}
	return out
}

// The property that makes a rebalance cheap: when a machine joins, a slot
// either stays where it is or moves to the new machine.
func TestJoinOnlyMovesSlotsToTheNewMachine(t *testing.T) {
	const slots = 64
	for _, n := range []int{3, 5, 8, 12, 20} {
		before := placeHRW(members(n), slots)
		after := placeHRW(members(n+1), slots)
		newest := members(n + 1)[n].ID

		for s := 0; s < slots; s++ {
			if before[s] != after[s] && after[s] != newest {
				t.Fatalf("%d→%d nodes: slot %d moved %s→%s, neither of which is the new machine",
					n, n+1, s, before[s], after[s])
			}
		}
	}
}
