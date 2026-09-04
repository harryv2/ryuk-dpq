package entity

import (
	"context"
	"time"
)

//go:generate mockgen -source=repo-node.go -destination=../mocks/mock_node_repo.go -package=mocks

type NodeEnqueue struct {
	Spec         QueueSpec
	Slot         *uint16
	Payload      []byte
	Priority     uint8
	GroupID      string
	TTL          time.Duration
	DeliverAfter time.Duration
}

type NodeEnqueueResult struct {
	MessageID string `json:"messageId"`
	Slot      uint16 `json:"slot"`
	Seq       uint64 `json:"seq"`
}

type NodeMessage struct {
	MessageID  string    `json:"messageId"`
	Payload    []byte    `json:"payload"`
	Priority   uint8     `json:"priority"`
	GroupID    string    `json:"groupId,omitempty"`
	Attempts   uint32    `json:"attempts"`
	EnqueuedAt time.Time `json:"enqueuedAt"`
	Receipt    string    `json:"receipt"`
}

type NodeStats struct {
	Org          string        `json:"org"`
	Name         string        `json:"name"`
	Ready        [3]int64      `json:"ready"`
	InFlight     int64         `json:"inFlight"`
	Delayed      int64         `json:"delayed"`
	OldestAge    time.Duration `json:"oldestAge"`
	Enqueued     uint64        `json:"enqueued"`
	Acked        uint64        `json:"acked"`
	Expired      uint64        `json:"expired"`
	Requeued     uint64        `json:"requeued"`
	DeadLettered uint64        `json:"deadLettered"`
	Escapes      uint64        `json:"escapes"`
}

// QueueSpec travels with every request so a node can materialise a queue it has
// not seen without a separate create call.
type QueueSpec struct {
	Org                 string        `json:"org"`
	Name                string        `json:"name"`
	VisibilityTimeout   time.Duration `json:"visibilityTimeout"`
	MaxRetries          uint32        `json:"maxRetries"`
	DefaultTTL          time.Duration `json:"defaultTtl"`
	StarvationThreshold time.Duration `json:"starvationThreshold"`
	StarvationReserve   float64       `json:"starvationReserve"`
	MaxDepth            int64         `json:"maxDepth"`
	Distributed         bool          `json:"distributed"`
	Generation          uint64        `json:"generation"`
}

func (c QueueConfig) Spec() QueueSpec {
	return QueueSpec{
		Org:                 c.Org,
		Name:                c.Name,
		VisibilityTimeout:   c.Settings.VisibilityTimeout,
		MaxRetries:          c.Settings.MaxRetries,
		DefaultTTL:          c.Settings.DefaultTTL,
		StarvationThreshold: c.Settings.StarvationThreshold,
		StarvationReserve:   c.Settings.StarvationReserve,
		MaxDepth:            c.Settings.MaxDepth,
		Distributed:         c.Distributed,
		Generation:          c.Generation,
	}
}

// NodeGRPCRepo is how the gateway talks to a node. StatsAll answers for every queue
// on that node in one call, so collecting metrics costs one request per node
// rather than one per queue.
type NodeGRPCRepo interface {
	Enqueue(ctx context.Context, addr string, req NodeEnqueue) (NodeEnqueueResult, error)
	Dequeue(ctx context.Context, addr string, spec QueueSpec, max int) ([]NodeMessage, error)
	Ack(ctx context.Context, addr string, spec QueueSpec, receipt string) error
	Nack(ctx context.Context, addr string, spec QueueSpec, receipt string, delay time.Duration) error
	Stats(ctx context.Context, addr string, spec QueueSpec) (NodeStats, error)
	StatsAll(ctx context.Context, addr string) (string, []NodeStats, error)
	Drop(ctx context.Context, addr string, spec QueueSpec) error

	// PrepareMove snapshots slots without removing them, so a handoff that
	// fails leaves the old owner able to serve. DiscardMove is the point of no
	// return; AbortMove puts them back.
	PrepareMove(ctx context.Context, addr string, spec QueueSpec, slots []uint16, moveID string) (Transfer, error)
	DiscardMove(ctx context.Context, addr string, spec QueueSpec, slots []uint16, moveID string) error
	AbortMove(ctx context.Context, addr string, spec QueueSpec, slots []uint16, moveID string) error

	Held(ctx context.Context, addr string) (HeldResponse, error)

	Freeze(ctx context.Context, addr string, spec QueueSpec, slots []uint16) (Transfer, error)
	Absorb(ctx context.Context, addr string, t Transfer) error

	// Subscribe opens the notification stream so a parked consumer can be woken
	// without polling.
	Subscribe(ctx context.Context, addr, gatewayID string) (<-chan WorkAvailable, error)
}

type WorkAvailable struct {
	Org          string
	Name         string
	BestPriority uint8
}

type Transfer struct {
	// MoveID names one handoff, so absorbing a retry is a no-op.
	MoveID string
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

type HeldSlots struct {
	Org    string
	Name   string
	Slots  []uint16
	Frozen []uint16
}

type HeldResponse struct {
	NodeID string
	Queues []HeldSlots
}
