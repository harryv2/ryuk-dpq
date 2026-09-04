package logic

import (
	"context"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

// UpdateQueue changes a queue's settings while it is running. Only the tunable
// ones: placement and slot count decide where a group's messages live, so
// changing them would send later messages of a group somewhere else.
//
// Nothing already queued is rewritten. Nodes pick the change up on their next
// request, because the settings travel with every one.
func (l *GatewayLogic) UpdateQueue(ctx context.Context, req entity.UpdateQueueRequest) (entity.QueueSummary, error) {
	cfg, err := l.config(ctx, req.Org, req.Name)
	if err != nil {
		return entity.QueueSummary{}, err
	}
	if cfg.State == entity.StateDeleting {
		return entity.QueueSummary{}, enterr.Invalid("queue is being deleted")
	}

	settings := req.Settings
	// Not tunable, and silently dropping a caller's value would be worse than
	// refusing it.
	settings.PlacementWidth = cfg.Settings.PlacementWidth

	if settings.DeadLetterQueue != "" && settings.DeadLetterQueue != cfg.Settings.DeadLetterQueue {
		if _, err := l.config(ctx, req.Org, settings.DeadLetterQueue); err != nil {
			if enterr.CodeOf(err) == enterr.CodeNotFound {
				return entity.QueueSummary{}, enterr.Invalid(
					"dead-letter queue %q does not exist; create it first", settings.DeadLetterQueue)
			}
			return entity.QueueSummary{}, err
		}
	}

	if err := l.queueTableRepo.UpdateSettings(ctx, req.Org, req.Name, settings); err != nil {
		return entity.QueueSummary{}, enterr.Internal("update queue", err)
	}
	l.evictCache(queueCacheKey(req.Org, req.Name))

	cfg.Settings = settings
	return entity.QueueSummary{
		Name: cfg.Name, Distributed: cfg.Distributed, State: string(cfg.State),
		PlacementWidth: cfg.Settings.PlacementWidth,
		OwnerNode:      cfg.OwnerNode, Settings: cfg.Settings,
	}, nil
}
