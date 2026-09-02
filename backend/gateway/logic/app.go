// Package logic is the gateway's behaviour: validate, work out which node owns
// a queue, forward, and collect metrics.
package logic

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/third_party/inmemorycache"
)

type Config struct {
	CacheTTL     time.Duration
	CollectEvery time.Duration
}

type GatewayLogicInterface interface {
	CreateQueue(context.Context, entity.CreateQueueRequest) (entity.CreateQueueResponse, error)
	DeleteQueue(context.Context, string, string) error
	ListQueues(context.Context, string) ([]entity.QueueSummary, error)

	Enqueue(context.Context, entity.EnqueueRequest) (entity.EnqueueResponse, error)
	Dequeue(context.Context, entity.DequeueRequest) (entity.DequeueResponse, error)
	Ack(context.Context, entity.AckRequest) error
	Nack(context.Context, entity.NackRequest) error

	Stats(context.Context, string, string) (entity.QueueStatsResponse, error)
	Metrics(context.Context, string) ([]entity.QueueStatsResponse, error)
	Cluster(context.Context) (entity.ClusterResponse, error)

	Collect(context.Context)
}

type GatewayLogic struct {
	cfg     Config
	log     *slog.Logger
	queues  entity.QueueTableRepo
	slots   entity.SlotPlacementTableRepo
	nodes   entity.NodeRepo
	members entity.MembershipRepo

	wait    *waiters
	subMu   sync.Mutex
	subOpen map[string]bool

	cache *inmemorycache.TTL[string, entity.QueueConfig]
	stats *inmemorycache.TTL[string, entity.NodeStats]
	owner *inmemorycache.TTL[string, string] // queue key -> node id, from the last collection
}

func New(
	cfg Config,
	log *slog.Logger,
	queues entity.QueueTableRepo,
	slots entity.SlotPlacementTableRepo,
	nodes entity.NodeRepo,
	members entity.MembershipRepo,
) *GatewayLogic {
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 30 * time.Second
	}
	if cfg.CollectEvery <= 0 {
		cfg.CollectEvery = 5 * time.Second
	}
	return &GatewayLogic{
		cfg:     cfg,
		log:     log,
		queues:  queues,
		slots:   slots,
		nodes:   nodes,
		members: members,
		wait:    newWaiters(),
		subOpen: map[string]bool{},
		cache:   inmemorycache.New[string, entity.QueueConfig](cfg.CacheTTL),
		stats:   inmemorycache.New[string, entity.NodeStats](3 * cfg.CollectEvery),
		owner:   inmemorycache.New[string, string](3 * cfg.CollectEvery),
	}
}

func key(org, name string) string { return org + "/" + name }

// WatchMembers starts following the cluster. Kept on the god struct so main
// does not need to know which repo holds the membership.
func (l *GatewayLogic) WatchMembers(ctx context.Context) error {
	return l.members.Watch(ctx)
}
