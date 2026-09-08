package entity

import (
	"time"

	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// QueueSpec is everything a node needs to run a queue. The gateway sends it
// with each request so a node can materialise a queue it has not seen.
type QueueSpec struct {
	Org                        string        `json:"org"`
	Name                       string        `json:"name"`
	VisibilityTimeout          time.Duration `json:"visibilityTimeout"`
	MaxRetries                 uint32        `json:"maxRetries"`
	DefaultTTL                 time.Duration `json:"defaultTtl"`
	StarvationThreshold        time.Duration `json:"starvationThreshold"`
	StarvationReserve          float64       `json:"starvationReserve"`
	StarvationAvoidanceEnabled bool          `json:"starvationAvoidanceEnabled"`
	MaxDepth                   int64         `json:"maxDepth"`
	Distributed                bool          `json:"distributed"`
	HasDeadLetter              bool          `json:"hasDeadLetter"`
	Generation                 uint64        `json:"generation"`
}

func (s QueueSpec) Key() engine.QueueKey {
	return engine.QueueKey{Org: s.Org, Name: s.Name}
}

func (s QueueSpec) EngineConfig() engine.Config {
	return engine.Config{
		Key:                        s.Key(),
		VisibilityTimeout:          s.VisibilityTimeout,
		MaxRetries:                 s.MaxRetries,
		MaxRetriesSet:              true, // the gateway resolves it before sending
		DefaultTTL:                 s.DefaultTTL,
		StarvationThreshold:        s.StarvationThreshold,
		StarvationReserve:          s.StarvationReserve,
		StarvationAvoidanceEnabled: s.StarvationAvoidanceEnabled,
		MaxDepth:                   s.MaxDepth,
		Distributed:                s.Distributed,
		HasDeadLetter:              s.HasDeadLetter,
	}
}

type EnqueueRequest struct {
	Spec         QueueSpec     `json:"spec"`
	Slot         *uint16       `json:"slot,omitempty"` // set when the caller already chose
	Payload      []byte        `json:"payload"`
	Priority     uint8         `json:"priority"`
	GroupID      string        `json:"groupId,omitempty"`
	TTL          time.Duration `json:"ttl,omitempty"`
	DeliverAfter time.Duration `json:"deliverAfter,omitempty"`
}

type EnqueueResponse struct {
	MessageID string `json:"messageId"`
	Slot      uint16 `json:"slot"`
	Seq       uint64 `json:"seq"`
}

type DequeueRequest struct {
	Spec        QueueSpec     `json:"spec"`
	MaxMessages int           `json:"maxMessages"`
	WaitTime    time.Duration `json:"waitTime"`
}

type DeliveredMessage struct {
	MessageID  string    `json:"messageId"`
	Payload    []byte    `json:"payload"`
	Priority   uint8     `json:"priority"`
	GroupID    string    `json:"groupId,omitempty"`
	Attempts   uint32    `json:"attempts"`
	EnqueuedAt time.Time `json:"enqueuedAt"`
	Receipt    string    `json:"receipt"`
}

type DequeueResponse struct {
	Messages []DeliveredMessage `json:"messages"`
}

type AckRequest struct {
	Spec    QueueSpec `json:"spec"`
	Receipt string    `json:"receipt"`
}

type NackRequest struct {
	Spec    QueueSpec     `json:"spec"`
	Receipt string        `json:"receipt"`
	Delay   time.Duration `json:"delay"`
}

type QueueStats struct {
	Org          string        `json:"org"`
	Name         string        `json:"name"`
	Ready        [3]int64      `json:"ready"`
	InFlight     int64         `json:"inFlight"`
	Delayed      int64         `json:"delayed"`
	OldestAge    time.Duration `json:"oldestAge"`
	TopReady     int16         `json:"topReady"`
	Enqueued     uint64        `json:"enqueued"`
	Acked        uint64        `json:"acked"`
	Expired      uint64        `json:"expired"`
	Requeued     uint64        `json:"requeued"`
	DeadLettered uint64        `json:"deadLettered"`
	Escapes      uint64        `json:"escapes"`
}

type StatsAllResponse struct {
	NodeID string       `json:"nodeId"`
	Queues []QueueStats `json:"queues"`
}

// TransferRequest carries a queue's contents to its new owner. MoveID names the
// handoff so a retry can be recognised and ignored rather than duplicating.
type TransferRequest struct {
	MoveID string                   `json:"moveId,omitempty"`
	Spec   QueueSpec                `json:"spec"`
	BySlot map[uint16][]WireMessage `json:"bySlot"`
}

type WireMessage struct {
	ID           string     `json:"id"`
	Payload      []byte     `json:"payload"`
	Priority     uint8      `json:"priority"`
	GroupID      string     `json:"groupId,omitempty"`
	Seq          uint64     `json:"seq"`
	EnqueuedAt   time.Time  `json:"enqueuedAt"`
	ExpiresAt    *time.Time `json:"expiresAt,omitempty"`
	DeliverAfter *time.Time `json:"deliverAfter,omitempty"`
	Attempts     uint32     `json:"attempts"`
}

func ToWire(m *engine.Message) WireMessage {
	w := WireMessage{
		ID: m.ID, Payload: m.Payload, Priority: uint8(m.Priority),
		GroupID: m.GroupID, Seq: m.Seq, EnqueuedAt: m.EnqueuedAt, Attempts: m.Attempts,
	}
	if !m.ExpiresAt.IsZero() {
		e := m.ExpiresAt
		w.ExpiresAt = &e
	}
	if !m.DeliverAfter.IsZero() {
		d := m.DeliverAfter
		w.DeliverAfter = &d
	}
	return w
}

func FromWire(w WireMessage) *engine.Message {
	m := &engine.Message{
		ID: w.ID, Payload: w.Payload, Priority: engine.Priority(w.Priority),
		GroupID: w.GroupID, Seq: w.Seq, EnqueuedAt: w.EnqueuedAt, Attempts: w.Attempts,
	}
	if w.ExpiresAt != nil {
		m.ExpiresAt = *w.ExpiresAt
	}
	if w.DeliverAfter != nil {
		m.DeliverAfter = *w.DeliverAfter
	}
	return m
}

// HeldSlots is what one node has for one queue, which the gateway compares
// against placement to find slots a failed handoff stranded.
type HeldSlots struct {
	Org    string   `json:"org"`
	Name   string   `json:"name"`
	Slots  []uint16 `json:"slots"`
	Frozen []uint16 `json:"frozen"`
}

type HeldResponse struct {
	NodeID string      `json:"nodeId"`
	Queues []HeldSlots `json:"queues"`
}

// PendingDeadLetter is a message the queue gave up on, waiting to be moved.
type PendingDeadLetter struct {
	Slot uint16
	Msg  *engine.Message
}

// DeadLetterItem names the queue a dead letter came from, because the gateway
// is the one that knows where it should go.
type DeadLetterItem struct {
	Org  string
	Name string
	Slot uint16
	Msg  *engine.Message
}

type DeadLetterResponse struct {
	DeadLetters []DeadLetterItem
}
