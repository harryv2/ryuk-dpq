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
	UpdateQueue(context.Context, entity.UpdateQueueRequest) (entity.QueueSummary, error)

	Enqueue(context.Context, entity.EnqueueRequest) (entity.EnqueueResponse, error)
	Dequeue(context.Context, entity.DequeueRequest) (entity.DequeueResponse, error)
	Ack(context.Context, entity.AckRequest) error
	Nack(context.Context, entity.NackRequest) error

	Stats(context.Context, string, string) (entity.QueueStatsResponse, error)
	GetTimeseriesMetrics(context.Context, string, string, string) (entity.TimeseriesResponse, error)
	GetMetrics(context.Context, string) ([]entity.QueueStatsResponse, error)
	GetClusterDetails(context.Context) (entity.ClusterResponse, error)
	GetNodeDetails(context.Context, string, string) (entity.NodeDetailResponse, error)
	GetRegistry(context.Context) (entity.RegistryResponse, error)

	Collect(context.Context)
	Reconcile(context.Context)
	RouteDeadLetters(context.Context)
}

type GatewayLogic struct {
	cfg                     Config
	log                     *slog.Logger
	queueTableRepo          entity.QueueTableRepo
	slotsPlacementTableRepo entity.SlotPlacementTableRepo
	nodesGRPCRepo           entity.NodeGRPCRepo
	membershipRepo          entity.MembershipRepo
	timeSeriesRepo          entity.TimeseriesRepo

	wait    *waiters
	subMu   sync.Mutex
	subOpen map[string]bool

	// One cache for every kind of value, each under its own key prefix. Reads
	// and writes go through the helpers in cache-logic.go, so no caller here
	// decides a TTL or handles a miss itself.
	cache *inmemorycache.InMemoryCache
}

func New(
	cfg Config,
	log *slog.Logger,
	queues entity.QueueTableRepo,
	slots entity.SlotPlacementTableRepo,
	nodes entity.NodeGRPCRepo,
	members entity.MembershipRepo,
	series entity.TimeseriesRepo,
) *GatewayLogic {
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 30 * time.Second
	}
	if cfg.CollectEvery <= 0 {
		cfg.CollectEvery = 5 * time.Second
	}
	return &GatewayLogic{
		cfg:                     cfg,
		log:                     log,
		queueTableRepo:          queues,
		slotsPlacementTableRepo: slots,
		nodesGRPCRepo:           nodes,
		membershipRepo:          members,
		timeSeriesRepo:          series,
		wait:                    newWaiters(),
		subOpen:                 map[string]bool{},
		cache:                   inmemorycache.NewInMemoryCache(log),
	}
}

func key(org, name string) string { return org + "/" + name }

// WatchMembers starts following the cluster. Kept on the god struct so main
// does not need to know which repo holds the membership.
func (l *GatewayLogic) WatchMembers(ctx context.Context) error {
	return l.membershipRepo.Watch(ctx)
}
