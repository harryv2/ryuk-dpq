package logic

import (
	"context"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

// config reads through the cache. A miss means go and look, never "no such
// queue": a queue created a moment ago has to be usable immediately.
func (l *GatewayLogic) config(ctx context.Context, org, name string) (entity.QueueConfig, error) {
	return fetchViaCache(l, queueCacheKey(org, name), l.cfg.CacheTTL,
		func() (entity.QueueConfig, error) { return l.loadConfig(ctx, org, name) })
}

// loadConfig joins the two tables.
func (l *GatewayLogic) loadConfig(ctx context.Context, org, name string) (entity.QueueConfig, error) {
	cfg, err := l.queueTableRepo.Get(ctx, org, name)
	if err != nil || !cfg.Distributed {
		return cfg, err
	}
	owners, err := l.slotsPlacementTableRepo.ListByQueue(ctx, org, name)
	if err != nil {
		return cfg, enterr.Internal("read slot placement", err)
	}
	cfg.SlotOwners = owners
	return cfg, nil
}

// withPlacement fills in slot owners for the distributed queues in a list.
// Callers that only need names and settings skip this.
func (l *GatewayLogic) withPlacement(ctx context.Context, cfgs []entity.QueueConfig) []entity.QueueConfig {
	for i := range cfgs {
		if !cfgs[i].Distributed {
			continue
		}
		owners, err := l.slotsPlacementTableRepo.ListByQueue(ctx, cfgs[i].Org, cfgs[i].Name)
		if err != nil {
			l.log.Warn("read slot placement", "queue", cfgs[i].Name, "err", err)
			continue
		}
		cfgs[i].SlotOwners = owners
	}
	return cfgs
}
