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
		answered := 0
		for _, addr := range l.rankedOwners(cfg) {
			msgs, err := l.nodesGRPCRepo.Dequeue(ctx, addr, cfg.Spec(), max)
			if err != nil {
				lastErr = err
				continue
			}
			answered++
			if len(msgs) > 0 {
				return msgs, nil
			}
		}
		// One owner being down does not make an empty queue a failure.
		if answered == 0 && lastErr != nil {
			return nil, lastErr
		}
		return nil, nil
	}

	if req.WaitTime <= 0 {
		msgs, err := take()
		if err != nil {
			return entity.DequeueResponse{}, err
		}
		return entity.DequeueResponse{Messages: toMessages(msgs)}, nil
	}

	// A message arriving while the first take is in flight would find nobody
	// waiting, so register before it rather than after.
	k := key(req.Org, req.Queue)
	ch := l.wait.park(k)
	defer l.wait.unpark(k, ch)

	timer := time.NewTimer(req.WaitTime)
	defer timer.Stop()

	// Notifications are dropped rather than delivered late at both hops. This is
	// what bounds the wait when one goes missing.
	poll := time.NewTicker(l.cfg.BackstopPoll)
	defer poll.Stop()

	first := true
	for {
		msgs, err := take()
		switch {
		case err != nil && first:
			return entity.DequeueResponse{}, err
		case err != nil:
			// Coming back empty is a normal answer to a poll; a 5xx is not.
		case len(msgs) > 0:
			return entity.DequeueResponse{Messages: toMessages(msgs)}, nil
		}
		first = false

		select {
		case <-ctx.Done():
			return entity.DequeueResponse{}, ctx.Err()
		case <-timer.C:
			return entity.DequeueResponse{}, nil
		case <-poll.C:
		case <-ch:
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
