package logic

import (
	"context"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

func (l *GatewayLogic) Ack(ctx context.Context, req entity.AckRequest) error {
	cfg, err := l.config(ctx, req.Org, req.Queue)
	if err != nil {
		return err
	}
	slot, err := slotFromReceipt(req.Receipt)
	if err != nil {
		return err
	}
	return l.withOwner(ctx, cfg, slot, func(addr string, spec entity.QueueSpec) error {
		return l.nodesGRPCRepo.Ack(ctx, addr, spec, req.Receipt)
	})
}

func (l *GatewayLogic) Nack(ctx context.Context, req entity.NackRequest) error {
	cfg, err := l.config(ctx, req.Org, req.Queue)
	if err != nil {
		return err
	}
	slot, err := slotFromReceipt(req.Receipt)
	if err != nil {
		return err
	}
	return l.withOwner(ctx, cfg, slot, func(addr string, spec entity.QueueSpec) error {
		return l.nodesGRPCRepo.Nack(ctx, addr, spec, req.Receipt, req.DelayFor)
	})
}

// withOwner resolves the owner and runs the call. If the node says the queue
// moved, the cached config is dropped and the call is retried once against the
// new owner, so a migration is invisible to callers.
