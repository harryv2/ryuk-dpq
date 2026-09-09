package controller

import (
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
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

// A handoff ships sequence numbers over gRPC. They are generation-prefixed and
// therefore large, so the wire must carry them without loss.
func TestSequenceNumbersSurviveTheWire(t *testing.T) {
	for _, seq := range []uint64{1, 1<<40 | 1, 7<<40 | 999999, 1<<63 | 1, ^uint64(0) - 1} {
		m := &engine.Message{
			ID: "abc", Payload: []byte("x"), Priority: engine.Medium,
			GroupID: "g", Seq: seq, EnqueuedAt: time.Now().UTC(), Attempts: 2,
		}
		back := entity.FromWire(wireFrom(wireTo(entity.ToWire(m))))
		if back.Seq != seq {
			t.Errorf("seq %d came back as %d", seq, back.Seq)
		}
		if back.ID != m.ID || back.Attempts != m.Attempts || back.Priority != m.Priority {
			t.Errorf("round trip lost fields: %+v", back)
		}
	}
}

// The spec carries the generation the receiver should number under.
func TestSpecGenerationSurvivesTheWire(t *testing.T) {
	for _, gen := range []uint64{0, 1, 4096, ^uint64(0)} {
		in := entity.QueueSpec{Org: "o", Name: "q", Generation: gen, MaxRetries: 3}
		if got := specFrom(specTo(in)).Generation; got != gen {
			t.Errorf("generation %d came back as %d", gen, got)
		}
	}
}
