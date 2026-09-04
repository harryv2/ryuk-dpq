package controller

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/constants"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

func createReq(body string) (entity.CreateQueueRequest, error) {
	r := httptest.NewRequest("POST", "/v1/queues", strings.NewReader(body))
	return parseAndValidateCreateQueueRequest(r, "org1")
}

func TestCreateQueueValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
		ok   bool
	}{
		{"defaults", `{"name":"orders"}`, true},
		{"no name", `{}`, false},
		{"bad characters", `{"name":"orders/2"}`, false},
		{"dlq is itself", `{"name":"orders","deadLetterQueue":"orders"}`, false},
		{"dlq named", `{"name":"orders","deadLetterQueue":"orders-dlq"}`, true},
		{"bad duration", `{"name":"orders","defaultTtl":"soon"}`, false},
		{"threshold above ttl", `{"name":"orders","defaultTtl":"1m","starvationThreshold":"2m"}`, false},
		{"reserve out of range", `{"name":"orders","starvationReserve":1.5}`, false},
		{"negative depth", `{"name":"orders","maxDepth":-1}`, false},
		{"width on a distributed queue", `{"name":"orders","distributed":true,"placementWidth":8}`, true},
		{"width on a single-node queue", `{"name":"orders","placementWidth":8}`, false},
		{"width of one", `{"name":"orders","distributed":true,"placementWidth":1}`, false},
		{"width past the slot count", `{"name":"orders","distributed":true,"placementWidth":65}`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := createReq(c.body)
			if c.ok && err != nil {
				t.Fatalf("want accepted, got %v", err)
			}
			if !c.ok && enterr.CodeOf(err) != enterr.CodeInvalid {
				t.Fatalf("want an invalid-request error, got %v", err)
			}
		})
	}
}

func TestCreateQueueDefaults(t *testing.T) {
	req, err := createReq(`{"name":"orders","defaultTtl":"1h"}`)
	if err != nil {
		t.Fatal(err)
	}
	s := req.Settings
	if s.VisibilityTimeout != 30*time.Second || s.MaxRetries != 3 || s.StarvationReserve != 0.2 {
		t.Fatalf("defaults wrong: %+v", s)
	}
	if s.StarvationThreshold != 15*time.Minute {
		t.Fatalf("threshold should default to a quarter of the ttl, got %v", s.StarvationThreshold)
	}
	if s.PlacementWidth != 0 {
		t.Fatalf("a single-node queue has no placement width, got %d", s.PlacementWidth)
	}

	req, err = createReq(`{"name":"orders","distributed":true}`)
	if err != nil {
		t.Fatal(err)
	}
	if req.Settings.PlacementWidth != constants.DefaultPlacementWidth {
		t.Fatalf("width defaulted to %d, want %d",
			req.Settings.PlacementWidth, constants.DefaultPlacementWidth)
	}
}

func TestPriorityParsing(t *testing.T) {
	cases := []struct {
		in   any
		want uint8
		ok   bool
	}{
		{"HIGH", 75, true}, {"MEDIUM", 50, true}, {"LOW", 25, true},
		{float64(0), 0, true}, {float64(100), 100, true}, {"42", 42, true},
		{nil, 50, true},
		{float64(101), 0, false}, {"urgent", 0, false},
	}
	for _, c := range cases {
		got, err := parsePriority(c.in)
		if c.ok && (err != nil || got != c.want) {
			t.Fatalf("parsePriority(%v) = %d, %v", c.in, got, err)
		}
		if !c.ok && err == nil {
			t.Fatalf("parsePriority(%v) should have failed", c.in)
		}
	}
}

func TestEnqueueValidation(t *testing.T) {
	parse := func(body string) error {
		r := httptest.NewRequest("POST", "/v1/queues/q/messages", strings.NewReader(body))
		_, err := parseAndValidateEnqueueRequest(r, "org1")
		return err
	}
	if err := parse(`{"priority":"HIGH"}`); enterr.CodeOf(err) != enterr.CodeInvalid {
		t.Fatalf("empty payload should be rejected, got %v", err)
	}
	if err := parse(`{"payload":"hi","ttl":"forever"}`); enterr.CodeOf(err) != enterr.CodeInvalid {
		t.Fatalf("bad ttl should be rejected, got %v", err)
	}
	if err := parse(`{"payload":"hi","priority":42}`); err != nil {
		t.Fatal(err)
	}
}

func TestPayloadEncodingIsNotGuessed(t *testing.T) {
	parse := func(body string) (string, error) {
		r := httptest.NewRequest("POST", "/v1/queues/q/messages", strings.NewReader(body))
		req, err := parseAndValidateEnqueueRequest(r, "org1")
		return string(req.Body), err
	}

	// "m000" is a plausible message and also valid base64. Sniffing turned it
	// into three bytes of noise, so the encoding is now declared, not guessed.
	if got, err := parse(`{"payload":"m000"}`); err != nil || got != "m000" {
		t.Fatalf("text payload came back as %q (%v)", got, err)
	}
	if got, err := parse(`{"payload":"aGVsbG8=","payloadEncoding":"base64"}`); err != nil || got != "hello" {
		t.Fatalf("base64 payload came back as %q (%v)", got, err)
	}
	if _, err := parse(`{"payload":"not!base64","payloadEncoding":"base64"}`); enterr.CodeOf(err) != enterr.CodeInvalid {
		t.Fatalf("bad base64 should be rejected, got %v", err)
	}
	if _, err := parse(`{"payload":"x","payloadEncoding":"rot13"}`); enterr.CodeOf(err) != enterr.CodeInvalid {
		t.Fatalf("unknown encoding should be rejected, got %v", err)
	}
}

func TestDequeueClampsWaitTime(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/queues/q/messages/receive", strings.NewReader(`{"waitTime":"5m"}`))
	req, err := parseAndValidateDequeueRequest(r, "org1")
	if err != nil {
		t.Fatal(err)
	}
	if req.WaitTime != constants.MaxWaitTime {
		t.Fatalf("wait time should be capped at %v, got %v", constants.MaxWaitTime, req.WaitTime)
	}
}
