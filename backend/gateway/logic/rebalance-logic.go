package logic

import (
	"context"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

const (
	stabilityWindow = 15 * time.Second
	maxConcurrent   = 2
)

// RunRebalancer moves queues onto machines that join. Placement is a pure
// function of the member list, so working out what should move needs no
// coordination; the only thing that needs care is not doing it twice.
func (l *GatewayLogic) RunRebalancer(ctx context.Context) {
	var settleAt time.Time
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-l.members.Changed():
			// A container in a crash loop would otherwise move data continuously,
			// which hurts far more than the imbalance it is correcting.
			settleAt = time.Now().Add(stabilityWindow)
		case <-tick.C:
			if settleAt.IsZero() || time.Now().Before(settleAt) {
				continue
			}
			settleAt = time.Time{}
			l.Rebalance(ctx)
		}
	}
}

func (l *GatewayLogic) Rebalance(ctx context.Context) {
	members := l.members.Members()
	if len(members) == 0 {
		return
	}
	cfgs, err := l.queues.ListAll(ctx)
	if err != nil {
		l.log.Warn("rebalance: list queues", "err", err)
		return
	}
	cfgs = l.withPlacement(ctx, cfgs)

	moved := 0
	for _, cfg := range cfgs {
		if moved >= maxConcurrent {
			break
		}
		if cfg.State != entity.StateActive {
			continue
		}
		if cfg.Distributed {
			if l.rebalanceSlots(ctx, cfg, members) {
				moved++
			}
			continue
		}
		want, ok := entity.OwnerFor(
			entity.OwnerKey(cfg.Org, cfg.Name, 0, false), members)
		if !ok || want.ID == cfg.OwnerNode {
			continue
		}
		from, live := l.members.Lookup(cfg.OwnerNode)
		if !live {
			// Its data is only there, so the assignment stays with it until it
			// comes back. Reassigning would serve an empty queue.
			continue
		}
		if l.migrate(ctx, cfg, from, want) == nil {
			moved++
		}
	}
	if moved > 0 {
		l.log.Info("rebalanced", "queues", moved, "members", len(members))
	}
}

// rebalanceSlots moves the slots of a distributed queue that no longer hash to
// their current owner. Slots are grouped by where they are moving from and to,
// so one handoff carries everything going the same way.
func (l *GatewayLogic) rebalanceSlots(ctx context.Context, cfg entity.QueueConfig, members []entity.Member) bool {
	type route struct{ from, to string }
	moves := map[route][]uint16{}

	for slot := 0; slot < slotCountFor(cfg.Distributed); slot++ {
		current := cfg.SlotOwners[uint16(slot)]
		if current == "" {
			continue
		}
		want, ok := entity.OwnerFor(
			entity.OwnerKey(cfg.Org, cfg.Name, slot, true), members)
		if !ok || want.ID == current {
			continue
		}
		if _, live := l.members.Lookup(current); !live {
			continue // its data is only there; nothing is reassigned
		}
		r := route{from: current, to: want.ID}
		moves[r] = append(moves[r], uint16(slot))
	}
	if len(moves) == 0 {
		return false
	}

	if err := l.queues.SetState(ctx, cfg.Org, cfg.Name, entity.StateMigrating); err != nil {
		return false
	}
	defer func() {
		_ = l.queues.SetState(ctx, cfg.Org, cfg.Name, entity.StateActive)
		l.evictCache(queueCacheKey(cfg.Org, cfg.Name))
	}()

	nextGen := cfg.Generation + 1
	done := 0
	for r, slots := range moves {
		from, okFrom := l.members.Lookup(r.from)
		to, okTo := l.members.Lookup(r.to)
		if !okFrom || !okTo {
			continue
		}
		spec := cfg.Spec()
		transfer, err := l.nodes.Freeze(ctx, from.Addr, spec, slots)
		if err != nil {
			l.log.Warn("rebalance slots: freeze", "queue", cfg.Name, "err", err)
			continue
		}
		transfer.Spec = spec
		transfer.Spec.Generation = nextGen
		if err := l.nodes.Absorb(ctx, to.Addr, transfer); err != nil {
			l.log.Warn("rebalance slots: absorb", "queue", cfg.Name, "err", err)
			continue
		}
		for _, slot := range slots {
			if err := l.slots.SetOwner(ctx, cfg.Org, cfg.Name, slot, r.to, nextGen); err != nil {
				l.log.Warn("rebalance slots: record", "queue", cfg.Name, "slot", slot, "err", err)
			}
		}
		l.log.Info("moved slots", "org", cfg.Org, "queue", cfg.Name,
			"slots", len(slots), "from", r.from, "to", r.to)
		done++
	}
	return done > 0
}

// migrate hands a queue and its messages to a new owner: freeze, ship, record,
// release. The generation is bumped so the new owner's sequence numbers sort
// after the old owner's.
func (l *GatewayLogic) migrate(ctx context.Context, cfg entity.QueueConfig, from, to entity.Member) error {
	// One gateway at a time. Whoever flips the state to migrating owns the move.
	if err := l.queues.SetState(ctx, cfg.Org, cfg.Name, entity.StateMigrating); err != nil {
		return err
	}
	defer func() {
		_ = l.queues.SetState(ctx, cfg.Org, cfg.Name, entity.StateActive)
		l.evictCache(queueCacheKey(cfg.Org, cfg.Name))
	}()

	l.log.Info("migrating queue", "org", cfg.Org, "queue", cfg.Name,
		"from", from.ID, "to", to.ID)

	transfer, err := l.nodes.Freeze(ctx, from.Addr, cfg.Spec(), nil)
	if err != nil {
		l.log.Warn("migrate: freeze", "queue", cfg.Name, "err", err)
		return err
	}

	nextGen := cfg.Generation + 1
	transfer.Spec = cfg.Spec()
	transfer.Spec.Generation = nextGen

	if err := l.nodes.Absorb(ctx, to.Addr, transfer); err != nil {
		l.log.Warn("migrate: absorb", "queue", cfg.Name, "err", err)
		return err
	}
	if err := l.queues.SetOwner(ctx, cfg.Org, cfg.Name, to.ID, nextGen); err != nil {
		return err
	}
	_ = l.nodes.Drop(ctx, from.Addr, cfg.Spec())

	count := 0
	for _, msgs := range transfer.BySlot {
		count += len(msgs)
	}
	l.log.Info("migrated queue", "org", cfg.Org, "queue", cfg.Name,
		"to", to.ID, "messages", count, "generation", nextGen)
	return nil
}
