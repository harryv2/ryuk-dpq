package engine

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

type QueueKey struct {
	Org  string
	Name string
}

func (k QueueKey) String() string { return k.Org + "/" + k.Name }

type Message struct {
	ID           string
	Payload      []byte
	Priority     Priority
	GroupID      string
	Seq          uint64
	EnqueuedAt   time.Time
	ExpiresAt    time.Time
	DeliverAfter time.Time
	Attempts     uint32
}

func (m *Message) expired(now time.Time) bool {
	return !m.ExpiresAt.IsZero() && !now.Before(m.ExpiresAt)
}

// group returns the ordering key. An ungrouped message is its own group so it
// can never be blocked behind anything.
func (m *Message) group() string {
	if m.GroupID != "" {
		return m.GroupID
	}
	return m.ID
}

// Receipt routes an acknowledgement and identifies one delivery attempt.
type Receipt struct {
	Slot        uint16
	MessageID   string
	Epoch       uint64
	Incarnation uint64
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("engine: entropy source failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
