package controller

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

// Everything here decides whether a request is well formed, which needs nothing
// but the request itself. Checks that need stored state -- does this queue
// exist, is the dead-letter queue real -- belong in the logic layer.

func optDuration(field, s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, enterr.Invalid("%s: bad duration %q", field, s)
	}
	if d < 0 {
		return 0, enterr.Invalid("%s must not be negative", field)
	}
	return d, nil
}

func parsePriority(v any) (uint8, error) {
	switch x := v.(type) {
	case nil:
		return 50, nil
	case float64:
		if x < 0 || x > 100 {
			return 0, enterr.Invalid("priority must be 0-100")
		}
		return uint8(x), nil
	case string:
		switch strings.ToUpper(x) {
		case "HIGH":
			return 75, nil
		case "MEDIUM":
			return 50, nil
		case "LOW":
			return 25, nil
		}
		n, err := strconv.Atoi(x)
		if err != nil || n < 0 || n > 100 {
			return 0, enterr.Invalid("priority must be 0-100, or HIGH, MEDIUM, LOW")
		}
		return uint8(n), nil
	}
	return 0, enterr.Invalid("priority must be a number or HIGH, MEDIUM, LOW")
}

// decodePayload reads the body according to what the caller said it is. It does
// not sniff: a short alphanumeric message like "m000" is valid base64, so
// guessing silently turns a message into three bytes of noise.
func decodePayload(s, encoding string) ([]byte, error) {
	if s == "" {
		return nil, enterr.Invalid("payload is required")
	}
	switch encoding {
	case "", "text", "utf8", "utf-8":
		return []byte(s), nil
	case "base64":
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, enterr.Invalid("payload is not valid base64")
		}
		return b, nil
	}
	return nil, enterr.Invalid("payloadEncoding must be text or base64")
}

// queueName keeps names to what fits in a URL path and a directory name, since
// a queue becomes a directory of write-ahead logs on the owning node.
func queueName(field, name string) error {
	if name == "" {
		return enterr.Invalid("%s is required", field)
	}
	if len(name) > 128 {
		return enterr.Invalid("%s must be 128 characters or fewer", field)
	}
	for _, r := range name {
		ok := r == '-' || r == '_' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !ok {
			return enterr.Invalid("%s may only contain letters, digits, - and _", field)
		}
	}
	return nil
}
