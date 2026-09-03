package logic

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

func (l *GatewayLogic) Dequeue(ctx context.Context, req entity.DequeueRequest) (entity.DequeueResponse, error) {
	cfg, err := l.config(ctx, req.Org, req.Queue)
	if err != nil {
		return entity.DequeueResponse{}, err
	}
	max := req.MaxMessages

	// Try once. A busy queue never gets past this, so the parking below only
	// ever costs anything on an idle one.
	take := func() ([]entity.NodeMessage, error) {
		if !cfg.Distributed {
			var msgs []entity.NodeMessage
			err := l.withOwner(ctx, cfg, 0, func(addr string, spec entity.QueueSpec) error {
				var err error
				msgs, err = l.nodesGRPCRepo.Dequeue(ctx, addr, spec, max)
				return err
			})
			return msgs, err
		}

		// Slots are on different machines, so ask the one claiming the most
		// urgent work and fall through if it comes back empty.
		var lastErr error
		for _, addr := range l.rankedOwners(cfg) {
			msgs, err := l.nodesGRPCRepo.Dequeue(ctx, addr, cfg.Spec(), max)
			if err != nil {
				lastErr = err
				continue
			}
			if len(msgs) > 0 {
				return msgs, nil
			}
		}
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, nil
	}

	msgs, err := take()
	if err != nil || len(msgs) > 0 || req.WaitTime <= 0 {
		if err != nil {
			return entity.DequeueResponse{}, err
		}
		return entity.DequeueResponse{Messages: toMessages(msgs)}, nil
	}

	// Park until a node says this queue gained work, then issue exactly one
	// dequeue. Fanning a waiting dequeue out would lease messages nobody is
	// processing.
	k := key(req.Org, req.Queue)
	ch := l.wait.park(k)
	defer l.wait.unpark(k, ch)

	timer := time.NewTimer(req.WaitTime)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return entity.DequeueResponse{}, ctx.Err()
		case <-timer.C:
			return entity.DequeueResponse{}, nil
		case <-ch:
			msgs, err := take()
			if err != nil {
				return entity.DequeueResponse{}, err
			}
			if len(msgs) > 0 {
				return entity.DequeueResponse{Messages: toMessages(msgs)}, nil
			}
			// another consumer won the race; keep waiting
		}
	}
}

func toMessages(in []entity.NodeMessage) []entity.Message {
	out := make([]entity.Message, 0, len(in))
	for _, m := range in {
		out = append(out, entity.Message{
			MessageID:  m.MessageID,
			Payload:    base64.StdEncoding.EncodeToString(m.Payload),
			Priority:   m.Priority,
			GroupID:    m.GroupID,
			Attempts:   m.Attempts,
			EnqueuedAt: m.EnqueuedAt,
			Receipt:    m.Receipt,
		})
	}
	return out
}
