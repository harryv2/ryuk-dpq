package logic

import (
	"context"

	"github.com/harryv2/ryuk-dpq/backend/constants"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

func (l *GatewayLogic) CreateQueue(ctx context.Context, req entity.CreateQueueRequest) (entity.CreateQueueResponse, error) {
	settings := req.Settings

	// The dead-letter queue has to exist now. Checking it only when a message
	// runs out of retries would lose the message and report nothing.
	if settings.DeadLetterQueue != "" {
		dlq, err := l.queues.Get(ctx, req.Org, settings.DeadLetterQueue)
		if err != nil {
			if enterr.CodeOf(err) == enterr.CodeNotFound {
				return entity.CreateQueueResponse{}, enterr.Invalid(
					"dead-letter queue %q does not exist; create it first", settings.DeadLetterQueue)
			}
			return entity.CreateQueueResponse{}, err
		}
		if dlq.State == entity.StateDeleting {
			return entity.CreateQueueResponse{}, enterr.Invalid(
				"dead-letter queue %q is being deleted", settings.DeadLetterQueue)
		}
	}

	owner := ""
	if !req.Distributed {
		if m, err := l.assignOwner(req.Org, req.Name, 0, false); err == nil {
			owner = m.ID
		}
	}

	cfg, created, err := l.queues.Create(ctx, entity.QueueConfig{
		Org: req.Org, Name: req.Name, Settings: settings,
		Distributed: req.Distributed, OwnerNode: owner,
	})
	if err != nil {
		return entity.CreateQueueResponse{}, enterr.Internal("create queue", err)
	}

	// A distributed queue places each slot on its own, so they spread over the
	// machines instead of landing together.
	if created && req.Distributed {
		for slot := 0; slot < constants.SlotsPerDistributedQueue; slot++ {
			m, err := l.assignOwner(req.Org, req.Name, slot, true)
			if err != nil {
				break
			}
			if err := l.slots.SetOwner(ctx, req.Org, req.Name, uint16(slot), m.ID, 1); err != nil {
				return entity.CreateQueueResponse{}, enterr.Internal("place slot", err)
			}
		}
		if cfg, err = l.loadConfig(ctx, req.Org, req.Name); err != nil {
			return entity.CreateQueueResponse{}, enterr.Internal("reload queue", err)
		}
	}
	if !created && cfg.Settings != settings {
		return entity.CreateQueueResponse{}, enterr.New(enterr.CodeConflict,
			"queue exists with different settings")
	}
	writeCache(l, queueCacheKey(cfg.Org, cfg.Name), l.cfg.CacheTTL, cfg)

	return entity.CreateQueueResponse{Name: cfg.Name, Created: created, OwnerNode: cfg.OwnerNode}, nil
}

func (l *GatewayLogic) DeleteQueue(ctx context.Context, org, name string) error {
	cfg, err := l.config(ctx, org, name)
	if err != nil {
		return err
	}
	// Mark first so gateways stop accepting, then drop the data, then remove
	// the row: the deletion has to reach the owner before the name is free.
	if err := l.queues.SetState(ctx, org, name, entity.StateDeleting); err != nil {
		return enterr.Internal("mark deleting", err)
	}
	l.evictCache(queueCacheKey(org, name))

	if _, addr, err := l.ownerAddr(ctx, cfg, 0); err == nil && addr != "" {
		_ = l.nodes.Drop(ctx, addr, cfg.Spec())
	}
	if err := l.slots.DeleteByQueue(ctx, org, name); err != nil {
		return enterr.Internal("delete slot placement", err)
	}
	if err := l.queues.Delete(ctx, org, name); err != nil {
		return enterr.Internal("delete queue", err)
	}
	l.evictCache(queueCacheKey(org, name))
	return nil
}

func (l *GatewayLogic) ListQueues(ctx context.Context, org string) ([]entity.QueueSummary, error) {
	cfgs, err := l.queues.ListByOrg(ctx, org)
	if err != nil {
		return nil, enterr.Internal("list queues", err)
	}
	out := make([]entity.QueueSummary, 0, len(cfgs))
	for _, c := range cfgs {
		s := entity.QueueSummary{
			Name: c.Name, Distributed: c.Distributed, State: string(c.State),
			OwnerNode: c.OwnerNode, Settings: c.Settings,
		}
		if st, ok := readCache[entity.NodeStats](l, statsCacheKey(c.Org, c.Name)); ok {
			s.Messages = st.Ready[0] + st.Ready[1] + st.Ready[2]
			s.InFlight = st.InFlight
			s.OldestAge = st.OldestAge.Seconds()
		}
		out = append(out, s)
	}
	return out, nil
}
