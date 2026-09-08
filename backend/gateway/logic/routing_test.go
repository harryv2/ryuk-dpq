package logic

import (
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

func ownersFor(t *testing.T, deep, urgent entity.NodeStats) []string {
	t.Helper()
	l, d := setupWithMembers(t, nil)
	d.members.EXPECT().Lookup("node-deep").
		Return(entity.Member{ID: "node-deep", Addr: "deep:9090"}, true).AnyTimes()
	d.members.EXPECT().Lookup("node-urgent").
		Return(entity.Member{ID: "node-urgent", Addr: "urgent:9090"}, true).AnyTimes()

	cfg := distributedCfg()
	cfg.SlotOwners = map[uint16]string{0: "node-deep", 1: "node-urgent"}

	writeCache(l, nodeStatsCacheKey("org1", "orders", "node-deep"), time.Minute, deep)
	writeCache(l, nodeStatsCacheKey("org1", "orders", "node-urgent"), time.Minute, urgent)
	return l.rankedOwners(cfg)
}

// Ready is bucketed, so 67 and 100 are the same number to the gateway. Without
// TopReady the deeper node wins and its 67 is served ahead of the other's 100.
func TestRankedOwnersPrefersTheHigherPriorityOverTheDeeperQueue(t *testing.T) {
	got := ownersFor(t,
		entity.NodeStats{Ready: [3]int64{0, 0, 40}, TopReady: 67},
		entity.NodeStats{Ready: [3]int64{0, 0, 1}, TopReady: 100})

	if len(got) != 2 || got[0] != "urgent:9090" {
		t.Fatalf("got %v, want the node holding priority 100 first", got)
	}
}

func TestRankedOwnersFallsBackToDepthAtEqualPriority(t *testing.T) {
	got := ownersFor(t,
		entity.NodeStats{Ready: [3]int64{0, 0, 40}, TopReady: 75},
		entity.NodeStats{Ready: [3]int64{0, 0, 1}, TopReady: 75})

	if len(got) != 2 || got[0] != "deep:9090" {
		t.Fatalf("got %v, want the deeper node first", got)
	}
}

func TestRankedOwnersRanksAnEmptyNodeLast(t *testing.T) {
	got := ownersFor(t,
		entity.NodeStats{TopReady: -1},
		entity.NodeStats{Ready: [3]int64{1, 0, 0}, TopReady: 0})

	if len(got) != 2 || got[0] != "urgent:9090" {
		t.Fatalf("got %v, want the node holding priority 0 ahead of the empty one", got)
	}
}
