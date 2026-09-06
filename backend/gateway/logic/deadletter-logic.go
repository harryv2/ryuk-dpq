package logic

import (
	"context"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

const drainEvery = 5 * time.Second

// RunDeadLetterRouter moves what nodes have given up on into the queues those
// messages were meant to fail into.
//
// The node cannot do this itself: it does not know which node owns the
// dead-letter queue, and placement lives in Postgres, which nodes deliberately
// never read. So the gateway, which knows both, pulls them and re-enqueues them
// through the ordinary path.
func (l *GatewayLogic) RunDeadLetterRouter(ctx context.Context) {
	t := time.NewTicker(drainEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.RouteDeadLetters(ctx)
		}
	}
}

func (l *GatewayLogic) RouteDeadLetters(ctx context.Context) {
	for _, m := range l.membershipRepo.Members() {
		items, err := l.nodesGRPCRepo.DeadLetters(ctx, m.Addr)
		if err != nil || len(items) == 0 {
			continue
		}

		var done []string
		for _, d := range items {
			switch l.deliverDeadLetter(ctx, d) {
			case deadLetterMoved, deadLetterDrop:
				done = append(done, d.Message.MessageID)
			case deadLetterRetry:
				// Left on the node, tried again next round.
			}
		}
		if len(done) == 0 {
			continue
		}
		// Only now: the node holds them until this call, so a gateway that dies
		// above costs a duplicate in the dead-letter queue, not a lost message.
		if err := l.nodesGRPCRepo.AckDeadLetters(ctx, m.Addr, done); err != nil {
			l.log.Warn("dead-letter: ack drain", "node", m.ID, "err", err)
			continue
		}
		l.log.Info("dead letters moved", "node", m.ID, "messages", len(done))
	}
}

type deadLetterOutcome int

const (
	deadLetterMoved deadLetterOutcome = iota
	deadLetterRetry
	deadLetterDrop
)

func (l *GatewayLogic) deliverDeadLetter(ctx context.Context, d entity.DeadLetterItem) deadLetterOutcome {
	cfg, err := l.config(ctx, d.Org, d.Name)
	if err != nil {
		// The source queue is gone, so there is nothing left to preserve it for.
		if enterr.CodeOf(err) == enterr.CodeNotFound {
			return deadLetterDrop
		}
		return deadLetterRetry
	}
	target := cfg.Settings.DeadLetterQueue
	if target == "" {
		// A queue with no dead-letter queue never gives up on a message, so
		// this can only be one that was configured and then cleared.
		return deadLetterDrop
	}

	_, err = l.Enqueue(ctx, entity.EnqueueRequest{
		Org:           d.Org,
		Queue:         target,
		Body:          d.Message.Payload,
		PriorityValue: d.Message.Priority,
		GroupID:       d.Message.GroupID,
	})
	if err != nil {
		l.log.Warn("dead-letter: enqueue", "queue", d.Name, "target", target, "err", err)
		return deadLetterRetry
	}
	return deadLetterMoved
}
