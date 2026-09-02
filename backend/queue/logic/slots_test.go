package logic

import (
	"testing"

	"github.com/harryv2/ryuk-dpq/backend/constants"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// The engine imports nothing outside the standard library, so it cannot share
// this constant with the rest of the module. If the two drift, the gateway
// routes a message to a slot the dispatcher never scans and it is written to
// the log and then never delivered -- silent, and only for some group keys.
func TestSlotCountMatchesSharedConstant(t *testing.T) {
	if engine.SlotsPerQueue != constants.SlotsPerQueue {
		t.Fatalf("engine.SlotsPerQueue = %d but constants.SlotsPerQueue = %d; "+
			"a message routed to a slot outside the engine's range is never delivered",
			engine.SlotsPerQueue, constants.SlotsPerQueue)
	}
	if engine.MaxSlotsPerQueue != constants.SlotsPerDistributedQueue {
		t.Fatalf("engine.MaxSlotsPerQueue = %d but constants.SlotsPerDistributedQueue = %d",
			engine.MaxSlotsPerQueue, constants.SlotsPerDistributedQueue)
	}
}
