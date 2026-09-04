package entity

import "github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"

//go:generate mockgen -source=repo-wal.go -destination=../mocks/mock_wal_repo.go -package=mocks

type WALRepo interface {
	engine.Journal
	Replay() (map[uint16][]*engine.Message, error)
	Compact(slot uint16, msgs []*engine.Message) error
	Drop(slot uint16) error
	Close() error
}

// WALFactory owns the on-disk layout: which directory belongs to which queue.
type WALFactory interface {
	Open(key engine.QueueKey) (WALRepo, error)
	Remove(key engine.QueueKey) error
	List() ([]engine.QueueKey, error)
}
