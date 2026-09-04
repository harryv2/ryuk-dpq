package logic

import (
	"testing"

	"github.com/harryv2/ryuk-dpq/backend/constants"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
	"github.com/harryv2/ryuk-dpq/backend/slotting"
)

// These used to be three separate copies that a test compared. They now come
// from one place, so this only guards against someone reintroducing a literal.
func TestSlotCountsComeFromOnePlace(t *testing.T) {
	if engine.SlotsPerQueue != slotting.PerQueue || constants.SlotsPerQueue != slotting.PerQueue {
		t.Fatal("a slot count has been redefined instead of taken from slotting")
	}
	if engine.MaxSlotsPerQueue != slotting.PerDistributedQueue ||
		constants.SlotsPerDistributedQueue != slotting.PerDistributedQueue {
		t.Fatal("a distributed slot count has been redefined instead of taken from slotting")
	}
}

// The gateway picks a slot and the node stores by it, so a grouped message has
// to reach the same answer on both sides.
func TestGatewayAndEngineAgreeOnAGroupsSlot(t *testing.T) {
	const org, name, group = "org1", "orders", "customer-7"
	want := engine.SlotOf(engine.QueueKey{Org: org, Name: name}, group, slotting.PerQueue)
	got := slotting.SlotFor(org, name, group, slotting.PerQueue)
	if got != want {
		t.Fatalf("gateway would route to slot %d, the node stores in %d", got, want)
	}
}
