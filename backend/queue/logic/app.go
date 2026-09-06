// Package logic owns the node's behaviour: which queues live here, how they are
// recovered, the timers that drive redelivery, and handing a queue to another
// machine.
package logic

import (
	"log/slog"
	"sync"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

type Config struct {
	NodeID      string
	Incarnation uint64
	SweepEvery  time.Duration
}

type QueueLogicInterface interface {
	Enqueue(entity.EnqueueRequest) (entity.EnqueueResponse, error)
	Dequeue(entity.DequeueRequest) (entity.DequeueResponse, error)
	Ack(entity.AckRequest) error
	Nack(entity.NackRequest) error

	Stats(entity.QueueSpec) (entity.QueueStats, error)
	StatsAll() entity.StatsAllResponse

	Drop(engine.QueueKey) error
	Held() []engine.QueueKey

	DeadLetters() entity.DeadLetterResponse
	AckDeadLetters([]string) error

	Freeze(entity.QueueSpec, []uint16) (map[uint16][]entity.WireMessage, error)
	PrepareMove(entity.QueueSpec, []uint16) (map[uint16][]entity.WireMessage, error)
	DiscardMove(entity.QueueSpec, []uint16) error
	AbortMove(entity.QueueSpec, []uint16) error
	HeldSlots() entity.HeldResponse
	Absorb(entity.TransferRequest) error

	Recover() error
	Sweep()
	NodeID() string

	Subscribe() (uint64, <-chan Notification)
	Unsubscribe(uint64)
}

type QueueLogic struct {
	moveMu       sync.Mutex
	appliedMoves map[string]bool // handoffs already absorbed, so a retry is a no-op

	cfg     Config
	log     *slog.Logger
	clock   engine.Clock
	cluster engine.Cluster
	wals    entity.WALFactory

	mu     sync.RWMutex
	queues map[engine.QueueKey]*liveQueue

	subs *subscribers
}

// liveQueue pairs an engine with the log it writes to. Both are per queue,
// because a queue's log is what moves when the queue does.
type liveQueue struct {
	q   *engine.Queue
	wal entity.WALRepo

	// Messages this queue has given up on, waiting for the gateway to move them.
	dlMu sync.Mutex
	dl   map[string]entity.PendingDeadLetter
}

func New(cfg Config, log *slog.Logger, clock engine.Clock, wals entity.WALFactory) *QueueLogic {
	if cfg.SweepEvery <= 0 {
		cfg.SweepEvery = 200 * time.Millisecond
	}
	return &QueueLogic{
		cfg:     cfg,
		log:     log,
		clock:   clock,
		cluster: engine.NewLocalCluster(),
		wals:    wals,
		queues:  make(map[engine.QueueKey]*liveQueue),
		subs:    newSubscribers(),
	}
}

func (l *QueueLogic) NodeID() string { return l.cfg.NodeID }
