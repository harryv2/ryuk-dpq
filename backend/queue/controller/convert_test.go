package controller

import (
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
)

// A setting the gateway sends has to survive the proto hop and reach the engine,
// or changing it in the UI does nothing.
func TestSpecRoundTripsThroughProto(t *testing.T) {
	in := entity.QueueSpec{
		Org:                        "org1",
		Name:                       "orders",
		VisibilityTimeout:          45 * time.Second,
		MaxRetries:                 5,
		DefaultTTL:                 6 * time.Hour,
		StarvationThreshold:        90 * time.Second,
		StarvationReserve:          0.35,
		StarvationAvoidanceEnabled: true,
		MaxDepth:                   1000,
		Distributed:                true,
		HasDeadLetter:              true,
		Generation:                 7,
	}

	if out := specFrom(specTo(in)); out != in {
		t.Fatalf("round trip changed the spec:\n got %+v\nwant %+v", out, in)
	}

	cfg := in.EngineConfig()
	if !cfg.StarvationAvoidanceEnabled {
		t.Fatal("EngineConfig dropped StarvationAvoidanceEnabled")
	}
	if cfg.StarvationReserve != in.StarvationReserve ||
		cfg.StarvationThreshold != in.StarvationThreshold {
		t.Fatalf("EngineConfig lost the starvation settings: %+v", cfg)
	}
}

func TestSpecRoundTripsWithAvoidanceOff(t *testing.T) {
	in := entity.QueueSpec{Org: "org1", Name: "orders", StarvationReserve: 0.2}
	if out := specFrom(specTo(in)); out.StarvationAvoidanceEnabled {
		t.Fatal("avoidance came back on after a round trip")
	}
}
