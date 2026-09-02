package entity

import (
	"context"
	"time"
)

//go:generate mockgen -source=repo-table-queue.go -destination=../mocks/mock_queue_table_repo.go -package=mocks

type QueueState string

const (
	StateActive    QueueState = "active"
	StateMigrating QueueState = "migrating"
	StateDeleting  QueueState = "deleting"
)

// QueueSettings is what a caller chooses. No node names, no slot counts.
type QueueSettings struct {
	VisibilityTimeout   time.Duration `json:"visibilityTimeout"`
	MaxRetries          uint32        `json:"maxRetries"`
	DefaultTTL          time.Duration `json:"defaultTtl"`
	StarvationThreshold time.Duration `json:"starvationThreshold"`
	StarvationReserve   float64       `json:"starvationReserve"`
	MaxDepth            int64         `json:"maxDepth"`
	DeadLetterQueue     string        `json:"deadLetterQueue,omitempty"`
}

// QueueConfig is the stored record. OwnerNode says where the queue IS, which is
// not where the hash says it should go: without replication the data exists in
// one place, so ownership has to follow it.
//
// SlotOwners is filled in from the slot_placement table for distributed queues.
type QueueConfig struct {
	Org         string            `json:"org"`
	Name        string            `json:"name"`
	Settings    QueueSettings     `json:"settings"`
	Distributed bool              `json:"distributed"`
	State       QueueState        `json:"state"`
	OwnerNode   string            `json:"ownerNode,omitempty"`
	Generation  uint64            `json:"generation"`
	SlotOwners  map[uint16]string `json:"slotOwners,omitempty"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
}

// QueueTableRepo owns the queues table.
type QueueTableRepo interface {
	Create(ctx context.Context, cfg QueueConfig) (QueueConfig, bool, error)
	Get(ctx context.Context, org, name string) (QueueConfig, error)
	ListByOrg(ctx context.Context, org string) ([]QueueConfig, error)
	ListAll(ctx context.Context) ([]QueueConfig, error)
	SetOwner(ctx context.Context, org, name, owner string, generation uint64) error
	// SetState only moves into migrating from active, so two gateways cannot
	// both start the same migration.
	SetState(ctx context.Context, org, name string, state QueueState) error
	Delete(ctx context.Context, org, name string) error
}
