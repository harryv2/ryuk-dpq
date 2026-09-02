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

// loadConfig joins the two tables. A normal queue is placed as a whole so its
// owner is one column; a distributed queue is placed per slot, so its owners
// come from the placement table.
func (l *GatewayLogic) loadConfig(ctx context.Context, org, name string) (entity.QueueConfig, error) {
	cfg, err := l.queues.Get(ctx, org, name)
	if err != nil || !cfg.Distributed {
		return cfg, err
	}
	owners, err := l.slots.ListByQueue(ctx, org, name)
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
		owners, err := l.slots.ListByQueue(ctx, cfgs[i].Org, cfgs[i].Name)
		if err != nil {
			l.log.Warn("read slot placement", "queue", cfgs[i].Name, "err", err)
			continue
		}
		cfgs[i].SlotOwners = owners
	}
	return cfgs
}

// assignOwner picks where a queue should go. Rendezvous hashing is a pure
// function of the member list, so every gateway gets the same answer and
// nothing has to be elected.
