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
	// Fixed at creation: changing it re-derives the candidate set and would
	// move most of the queue.
	PlacementWidth int `json:"placementWidth,omitempty"`
}

// QueueConfig is the stored record.
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

type QueueTableRepo interface {
	Create(ctx context.Context, cfg QueueConfig) (QueueConfig, bool, error)
	Get(ctx context.Context, org, name string) (QueueConfig, error)
	ListByOrg(ctx context.Context, org string) ([]QueueConfig, error)
	ListAll(ctx context.Context) ([]QueueConfig, error)
	SetOwner(ctx context.Context, org, name, owner string, generation uint64) error
	// UpdateSettings replaces the tunable settings. Shape is not tunable, so it
	// cannot change what the caller chose at creation.
	UpdateSettings(ctx context.Context, org, name string, s QueueSettings) error
	// SetState only moves into migrating from active, so two gateways cannot
	// both start the same migration.
	SetState(ctx context.Context, org, name string, state QueueState) error
	Delete(ctx context.Context, org, name string) error
}
