package logic

import (
	"errors"

	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

func (l *QueueLogic) Ack(req entity.AckRequest) error {
	lq, ok := l.lookup(req.Spec.Key())
	if !ok {
		return enterr.NotFound("queue")
	}
	r, err := entity.DecodeReceipt(req.Receipt)
	if err != nil {
		return enterr.Invalid("%s", err.Error())
	}

	switch err := lq.q.Ack(r); {
	case err == nil:
		return nil
	case errors.Is(err, engine.ErrNotInFlight):
		// already acknowledged, dead-lettered, or never existed
		return nil
	case errors.Is(err, engine.ErrLeaseExpired):
		return enterr.New(enterr.CodeConflict, "lease expired, message was redelivered")
	default:
		return enterr.Internal("ack", err)
	}
}

func (l *QueueLogic) Nack(req entity.NackRequest) error {
	lq, ok := l.lookup(req.Spec.Key())
	if !ok {
		return enterr.NotFound("queue")
	}
	r, err := entity.DecodeReceipt(req.Receipt)
	if err != nil {
		return enterr.Invalid("%s", err.Error())
	}

	dead, err := lq.q.Nack(r, req.Delay)
	switch {
	case errors.Is(err, engine.ErrNotInFlight):
		return nil
	case errors.Is(err, engine.ErrLeaseExpired):
		return enterr.New(enterr.CodeConflict, "lease expired, message was redelivered")
	case err != nil:
		return enterr.Internal("nack", err)
	}
	if dead != nil {
		l.offerDeadLetter(req.Spec.Key(), dead)
	}
	return nil
}
