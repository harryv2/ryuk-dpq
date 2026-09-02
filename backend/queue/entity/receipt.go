package entity

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// EncodeReceipt makes the dequeue token. Routing and both generation counters
// live here rather than in the message ID, because a receipt expires with its
// delivery attempt and so can never outlive the layout it describes.
func EncodeReceipt(r engine.Receipt) string {
	raw := fmt.Sprintf("%d:%s:%d:%d", r.Slot, r.MessageID, r.Epoch, r.Incarnation)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func DecodeReceipt(s string) (engine.Receipt, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return engine.Receipt{}, fmt.Errorf("receipt: %w", err)
	}
	parts := strings.Split(string(b), ":")
	if len(parts) != 4 {
		return engine.Receipt{}, fmt.Errorf("receipt: want 4 fields, got %d", len(parts))
	}
	slot, err := strconv.ParseUint(parts[0], 10, 16)
	if err != nil {
		return engine.Receipt{}, fmt.Errorf("receipt slot: %w", err)
	}
	epoch, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return engine.Receipt{}, fmt.Errorf("receipt epoch: %w", err)
	}
	inc, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return engine.Receipt{}, fmt.Errorf("receipt incarnation: %w", err)
	}
	return engine.Receipt{
		Slot: uint16(slot), MessageID: parts[1], Epoch: epoch, Incarnation: inc,
	}, nil
}
