package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

const (
	stabilityWindow = 15 * time.Second
	maxConcurrent   = 2
	sweepEvery      = 60 * time.Second
)

// RunRebalancer moves queues onto machines that join. Placement is a pure
// function of the member list, so working out what should move needs no
// coordination; the only thing that needs care is not doing it twice.
func (l *GatewayLogic) RunRebalancer(ctx context.Context) {
	var settleAt time.Time
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	// A membership change is the fast path, but it is only an edge: a queue
	// that missed its window -- locked, or the gateway restarted -- would stay
	// unbalanced until something else moved. This sweep is the level.
	sweep := time.NewTicker(sweepEvery)
	defer sweep.Stop()

	// Taken once: every call registers a listener, so calling it inside the
	// loop would add one per iteration.
	changed := l.membershipRepo.Changed()

	for {
		select {
		case <-ctx.Done():
			return
		case <-changed:
			// A container in a crash loop would otherwise move data continuously,
			// which hurts far more than the imbalance it is correcting.
			settleAt = time.Now().Add(stabilityWindow)
		case <-sweep.C:
			l.Rebalance(ctx)
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
	members := l.membershipRepo.Members()
	if len(members) == 0 {
		return
	}
	cfgs, err := l.queueTableRepo.ListAll(ctx)
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
		from, live := l.membershipRepo.Lookup(cfg.OwnerNode)
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

func (l *GatewayLogic) abort(ctx context.Context, addr string, spec entity.QueueSpec, slots []uint16, moveID string) {
	if err := l.nodesGRPCRepo.AbortMove(ctx, addr, spec, slots, moveID); err != nil {
		l.log.Warn("move: abort failed, the slots stay frozen until the reconciler thaws them",
			"queue", spec.Name, "err", err)
	}
}

// newMoveID names one handoff so the receiving node can tell a retry from a
// second move of the same slots.
func newMoveID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "move"
	}
	return hex.EncodeToString(b[:])
}

// rebalanceSlots moves the slots of a distributed queue that no longer hash to
// their current owner. Slots are grouped by where they are moving from and to,
// so one handoff carries everything going the same way.
func (l *GatewayLogic) rebalanceSlots(ctx context.Context, cfg entity.QueueConfig, members []entity.Member) bool {
	type route struct{ from, to string }
	moves := map[route][]uint16{}

	// The same candidate set creation used, or a rebalance would scatter the
	// queue across machines its width was meant to exclude.
	candidates := entity.CandidatesFor(key(cfg.Org, cfg.Name), members, cfg.Settings.PlacementWidth)

	for slot := 0; slot < slotCountFor(cfg.Distributed); slot++ {
		current := cfg.SlotOwners[uint16(slot)]
		if current == "" {
			continue
		}
		want, ok := entity.OwnerFor(
			entity.OwnerKey(cfg.Org, cfg.Name, slot, true), candidates)
		if !ok || want.ID == current {
			continue
		}
		if _, live := l.membershipRepo.Lookup(current); !live {
			continue // its data is only there; nothing is reassigned
		}
		r := route{from: current, to: want.ID}
		moves[r] = append(moves[r], uint16(slot))
	}
	if len(moves) == 0 {
		return false
	}

	if err := l.queueTableRepo.SetState(ctx, cfg.Org, cfg.Name, entity.StateMigrating); err != nil {
		return false
	}
	defer func() {
		_ = l.queueTableRepo.SetState(ctx, cfg.Org, cfg.Name, entity.StateActive)
		l.evictCache(queueCacheKey(cfg.Org, cfg.Name))
	}()

	nextGen := cfg.Generation + 1
	done := 0
	for r, slots := range moves {
		from, okFrom := l.membershipRepo.Lookup(r.from)
		to, okTo := l.membershipRepo.Lookup(r.to)
		if !okFrom || !okTo {
			continue
		}
		spec := cfg.Spec()
		moveID := newMoveID()

		// 1. Hold the slots and take copies. Nothing is removed yet, so a
		//    failure anywhere below leaves the old owner able to serve.
		transfer, err := l.nodesGRPCRepo.PrepareMove(ctx, from.Addr, spec, slots, moveID)
		if err != nil {
			l.log.Warn("move: prepare", "queue", cfg.Name, "err", err)
			continue
		}
		transfer.Spec = spec
		transfer.Spec.Generation = nextGen
		transfer.MoveID = moveID

		// 2. Put them on the new owner, which writes them to its log before
		//    they become visible. Placement still names the old owner, so
		//    nothing is asking the new one for them yet.
		if err := l.nodesGRPCRepo.Absorb(ctx, to.Addr, transfer); err != nil {
			l.log.Warn("move: absorb, putting the slots back", "queue", cfg.Name, "err", err)
			l.abort(ctx, from.Addr, spec, slots, moveID)
			continue
		}

		// 3. The commit. From here the new owner serves them.
		recorded := true
		for _, slot := range slots {
			if err := l.slotsPlacementTableRepo.SetOwner(ctx, cfg.Org, cfg.Name, slot, r.to, nextGen); err != nil {
				l.log.Warn("move: record", "queue", cfg.Name, "slot", slot, "err", err)
				recorded = false
			}
		}
		if !recorded {
			// Placement is half written. Leave both copies alone; the
			// reconciler compares what nodes hold against placement and
			// finishes or undoes it.
			l.log.Warn("move: placement incomplete, leaving it to the reconciler",
				"queue", cfg.Name, "move", moveID)
			continue
		}

		// 4. Only now is it safe for the old owner to let go.
		if err := l.nodesGRPCRepo.DiscardMove(ctx, from.Addr, spec, slots, moveID); err != nil {
			l.log.Warn("move: discard, the reconciler will clean up", "queue", cfg.Name, "err", err)
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
	if err := l.queueTableRepo.SetState(ctx, cfg.Org, cfg.Name, entity.StateMigrating); err != nil {
		return err
	}
	defer func() {
		_ = l.queueTableRepo.SetState(ctx, cfg.Org, cfg.Name, entity.StateActive)
		l.evictCache(queueCacheKey(cfg.Org, cfg.Name))
	}()

	l.log.Info("migrating queue", "org", cfg.Org, "queue", cfg.Name,
		"from", from.ID, "to", to.ID)

	transfer, err := l.nodesGRPCRepo.Freeze(ctx, from.Addr, cfg.Spec(), nil)
	if err != nil {
		l.log.Warn("migrate: freeze", "queue", cfg.Name, "err", err)
		return err
	}

	nextGen := cfg.Generation + 1
	transfer.Spec = cfg.Spec()
	transfer.Spec.Generation = nextGen

	if err := l.nodesGRPCRepo.Absorb(ctx, to.Addr, transfer); err != nil {
		l.log.Warn("migrate: absorb", "queue", cfg.Name, "err", err)
		return err
	}
	if err := l.queueTableRepo.SetOwner(ctx, cfg.Org, cfg.Name, to.ID, nextGen); err != nil {
		return err
	}
	_ = l.nodesGRPCRepo.Drop(ctx, from.Addr, cfg.Spec())

	count := 0
	for _, msgs := range transfer.BySlot {
		count += len(msgs)
	}
	l.log.Info("migrated queue", "org", cfg.Org, "queue", cfg.Name,
		"to", to.ID, "messages", count, "generation", nextGen)
	return nil
}
