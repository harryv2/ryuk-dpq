package logic

import (
	"errors"

	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

func (l *QueueLogic) Enqueue(req entity.EnqueueRequest) (entity.EnqueueResponse, error) {
	lq, err := l.queueFor(req.Spec)
	if err != nil {
		return entity.EnqueueResponse{}, enterr.Internal("open queue", err)
	}

	opts := engine.EnqueueOptions{
		Payload:      req.Payload,
		Priority:     engine.Priority(req.Priority),
		GroupID:      req.GroupID,
		TTL:          req.TTL,
		DeliverAfter: req.DeliverAfter,
	}

	// The gateway picks the slot, because for a distributed queue the slot is
	// what chose this node in the first place. A node cannot second-guess it.
	if req.Slot == nil {
		return entity.EnqueueResponse{}, enterr.Invalid("slot is required")
	}

	m, err := lq.q.EnqueueToSlot(*req.Slot, opts)
	switch {
	case errors.Is(err, engine.ErrBadPriority):
		return entity.EnqueueResponse{}, enterr.Invalid("priority must be 0-100")
	case errors.Is(err, engine.ErrQueueFull):
		return entity.EnqueueResponse{}, enterr.New(enterr.CodeExhausted, "queue is at max depth")
	case errors.Is(err, engine.ErrFrozen):
		return entity.EnqueueResponse{}, enterr.Moved("")
	case err != nil:
		return entity.EnqueueResponse{}, enterr.Internal("enqueue", err)
	}

	l.notifyWork(req.Spec.Key(), req.Priority, 1)
	return entity.EnqueueResponse{MessageID: m.ID, Slot: *req.Slot, Seq: m.Seq}, nil
}
