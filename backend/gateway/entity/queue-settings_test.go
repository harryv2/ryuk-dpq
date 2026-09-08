package entity

import (
	"encoding/json"
	"testing"
)

// Settings are stored as JSONB, so a row written before the field existed has to
// read back as off rather than failing.
func TestQueueSettingsJSONRoundTrip(t *testing.T) {
	in := QueueSettings{
		MaxRetries:                 3,
		StarvationReserve:          0.2,
		StarvationAvoidanceEnabled: true,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out QueueSettings
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round trip changed the settings:\n got %+v\nwant %+v", out, in)
	}
}

func TestQueueSettingsFromRowWithoutTheField(t *testing.T) {
	var s QueueSettings
	if err := json.Unmarshal([]byte(`{"maxRetries":3,"starvationReserve":0.2}`), &s); err != nil {
		t.Fatal(err)
	}
	if s.StarvationAvoidanceEnabled {
		t.Fatal("a row written before the field existed must read back as off")
	}
}
