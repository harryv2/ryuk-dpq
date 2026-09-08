package logic

import (
	"context"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

// Reconcile compares what nodes hold against what placement says they should.
func (l *GatewayLogic) Reconcile(ctx context.Context) {
	members := l.membershipRepo.Members()
	if len(members) == 0 {
		return
	}

	// Placement is read after the reports, never before: a move committing
	// during these calls would otherwise make the new owner look like a
	// leftover, and the repair would discard what the move just delivered.
	type report struct {
		member entity.Member
		queues []entity.HeldSlots
	}
	reports := make([]report, 0, len(members))
	for _, m := range members {
		held, err := l.nodesGRPCRepo.Held(ctx, m.Addr)
		if err != nil {
			continue // it will be seen on a later pass
		}
		reports = append(reports, report{member: m, queues: held.Queues})
	}

	cfgs, err := l.queueTableRepo.ListAll(ctx)
	if err != nil {
		l.log.Warn("reconcile: list queues", "err", err)
		return
	}
	cfgs = l.withPlacement(ctx, cfgs)

	owners := map[string]map[uint16]string{} // queue key -> slot -> owner
	specs := map[string]entity.QueueSpec{}
	for _, c := range cfgs {
		// Mid-handoff: the slots are meant to be frozen and placement is still
		// moving, so repairing here would fight the move.
		if c.State != entity.StateActive {
			continue
		}
		k := key(c.Org, c.Name)
		specs[k] = c.Spec()
		if c.Distributed {
			owners[k] = c.SlotOwners
			continue
		}
		// A single-node queue has no placement rows: it is placed whole, so every
		// slot belongs to the one owner named on its row.
		if c.OwnerNode == "" {
			continue
		}
		whole := make(map[uint16]string, slotCountFor(false))
		for s := 0; s < slotCountFor(false); s++ {
			whole[uint16(s)] = c.OwnerNode
		}
		owners[k] = whole
	}

	repaired := 0
	for _, r := range reports {
		m := r.member
		for _, q := range r.queues {
			k := key(q.Org, q.Name)
			place, known := owners[k]
			if !known {
				continue // deleted, or not read this pass
			}

			var orphaned, stranded []uint16
			for _, slot := range q.Slots {
				if owner, ok := place[slot]; ok && owner != "" && owner != m.ID {
					orphaned = append(orphaned, slot)
				}
			}
			for _, slot := range q.Frozen {
				if owner, ok := place[slot]; ok && owner == m.ID {
					stranded = append(stranded, slot)
				}
			}

			// It owns these but they are held out of service, so a handoff
			// started and never finished. Put them back.
			if len(stranded) > 0 {
				if err := l.nodesGRPCRepo.AbortMove(ctx, m.Addr, specs[k], stranded, ""); err != nil {
					l.log.Warn("reconcile: thaw", "node", m.ID, "queue", q.Name, "err", err)
				} else {
					l.log.Info("reconcile: thawed slots a stalled move left frozen",
						"node", m.ID, "queue", q.Name, "slots", len(stranded))
					repaired += len(stranded)
				}
			}

			// It holds data for slots someone else owns. The move committed;
			// this copy is the leftover.
			if len(orphaned) > 0 {
				if err := l.nodesGRPCRepo.DiscardMove(ctx, m.Addr, specs[k], orphaned, ""); err != nil {
					l.log.Warn("reconcile: discard", "node", m.ID, "queue", q.Name, "err", err)
				} else {
					l.log.Info("reconcile: dropped slots this node no longer owns",
						"node", m.ID, "queue", q.Name, "slots", len(orphaned))
					repaired += len(orphaned)
				}
			}
		}
	}
	if repaired > 0 {
		l.log.Info("reconciled", "slots", repaired)
	}
}

// RunReconciler sweeps on a timer. Slower than the rebalancer: it is a safety
// net for interrupted work, not the path a normal move takes.
func (l *GatewayLogic) RunReconciler(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.Reconcile(ctx)
		}
	}
}
