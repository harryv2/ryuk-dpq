package logic

import (
	"context"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

func (l *GatewayLogic) Enqueue(ctx context.Context, req entity.EnqueueRequest) (entity.EnqueueResponse, error) {
	cfg, err := l.config(ctx, req.Org, req.Queue)
	if err != nil {
		return entity.EnqueueResponse{}, err
	}
	if cfg.State == entity.StateDeleting {
		return entity.EnqueueResponse{}, enterr.NotFound("queue")
	}

	// Every message in a group goes to the same slot, which is what makes
	// ordering possible at all.
	slot := slotOf(req.Org, req.Queue, req.GroupID, slotCountFor(cfg.Distributed))

	var res entity.NodeEnqueueResult
	err = l.withOwner(ctx, cfg, int(slot), func(addr string, spec entity.QueueSpec) error {
		s := slot
		var err error
		res, err = l.nodes.Enqueue(ctx, addr, entity.NodeEnqueue{
			Spec: spec, Slot: &s, Payload: req.Body, Priority: req.PriorityValue,
			GroupID: req.GroupID, TTL: req.TTLFor, DeliverAfter: req.DeliverIn,
		})
		return err
	})
	if err != nil {
		return entity.EnqueueResponse{}, err
	}
	return entity.EnqueueResponse{MessageID: res.MessageID}, nil
}
