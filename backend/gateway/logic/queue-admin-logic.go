package logic

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/harryv2/ryuk-dpq/backend/constants"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

func (l *GatewayLogic) CreateQueue(ctx context.Context, req entity.CreateQueueRequest) (entity.CreateQueueResponse, error) {
	settings := req.ParseQueueSettings

	// The dead-letter queue has to exist now. Checking it only when a message
	// runs out of retries would lose the message and report nothing.
	if settings.DeadLetterQueue != "" {
		dlq, err := l.config(ctx, req.Org, settings.DeadLetterQueue)
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
		if m, err := l.assignOwner(req.Org, req.Name, 0, false, 0); err == nil {
			owner = m.ID
		}
	}

	cfg, created, err := l.queueTableRepo.Create(ctx, entity.QueueConfig{
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
			m, err := l.assignOwner(req.Org, req.Name, slot, true, settings.PlacementWidth)
			if err != nil {
				break
			}
			if err := l.slotsPlacementTableRepo.SetOwner(ctx, req.Org, req.Name, uint16(slot), m.ID, 1); err != nil {
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
	// Deleting a queue that failures are routed to would leave those queues with
	// nowhere to put them, which surfaces much later as messages that never go
	// away.
	if users, err := l.deadLetterUsers(ctx, org, name); err != nil {
		return err
	} else if len(users) > 0 {
		// A conflict with what exists, not a malformed request -- same shape as
		// creating a queue that already exists with other settings.
		return enterr.New(enterr.CodeConflict, fmt.Sprintf(
			"%q is the dead-letter queue for %s; point them elsewhere first",
			name, strings.Join(users, ", ")))
	}

	// Mark first so gateways stop accepting, then drop the data, then remove
	// the row: the deletion has to reach the owner before the name is free.
	if err := l.queueTableRepo.SetState(ctx, org, name, entity.StateDeleting); err != nil {
		return enterr.Internal("mark deleting", err)
	}
	l.evictCache(queueCacheKey(org, name))

	if _, addr, err := l.ownerAddr(ctx, cfg, 0); err == nil && addr != "" {
		_ = l.nodesGRPCRepo.Drop(ctx, addr, cfg.Spec())
	}
	if err := l.slotsPlacementTableRepo.DeleteByQueue(ctx, org, name); err != nil {
		return enterr.Internal("delete slot placement", err)
	}
	if err := l.queueTableRepo.Delete(ctx, org, name); err != nil {
		return enterr.Internal("delete queue", err)
	}
	l.evictCache(queueCacheKey(org, name))
	return nil
}

// deadLetterUsers lists the queues that send their failures to this one.
func (l *GatewayLogic) deadLetterUsers(ctx context.Context, org, name string) ([]string, error) {
	cfgs, err := l.queueTableRepo.ListByOrg(ctx, org)
	if err != nil {
		return nil, enterr.Internal("list queues", err)
	}
	var out []string
	for _, c := range cfgs {
		if c.Name != name && c.Settings.DeadLetterQueue == name {
			out = append(out, c.Name)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (l *GatewayLogic) ListQueues(ctx context.Context, org string) ([]entity.QueueSummary, error) {
	cfgs, err := l.queueTableRepo.ListByOrg(ctx, org)
	if err != nil {
		return nil, enterr.Internal("list queues", err)
	}
	out := make([]entity.QueueSummary, 0, len(cfgs))
	for _, c := range cfgs {
		s := entity.QueueSummary{
			Name: c.Name, Distributed: c.Distributed, State: string(c.State),
			PlacementWidth: c.Settings.PlacementWidth,
			OwnerNode:      c.OwnerNode, Settings: c.Settings,
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
