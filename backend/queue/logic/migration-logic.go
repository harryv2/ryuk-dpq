package logic

import (
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// Freeze stops a queue serving and hands back everything it held, keyed by slot
// so placement survives the move.
func (l *QueueLogic) Freeze(spec entity.QueueSpec, only []uint16) (map[uint16][]entity.WireMessage, error) {
	lq, ok := l.lookup(spec.Key())
	if !ok {
		return map[uint16][]entity.WireMessage{}, nil // nothing here to move
	}
	bySlot := lq.q.Freeze()

	// An empty filter means every slot this node holds. A distributed queue
	// names the slots that actually moved, so the rest stay put.
	keep := map[uint16]bool{}
	for _, s := range only {
		keep[s] = true
	}

	out := make(map[uint16][]entity.WireMessage, len(bySlot))
	for slot, msgs := range bySlot {
		if len(keep) > 0 && !keep[slot] {
			// put it back: this slot is not moving
			restore := make([]*engine.Message, len(msgs))
			copy(restore, msgs)
			_ = lq.q.Absorb(map[uint16][]*engine.Message{slot: restore})
			continue
		}
		wire := make([]entity.WireMessage, 0, len(msgs))
		for _, m := range msgs {
			wire = append(wire, entity.ToWire(m))
		}
		out[slot] = wire
	}
	if len(keep) > 0 {
		lq.q.Thaw() // only some slots left, the queue keeps serving the rest
	}
	return out, nil
}

// Absorb merges a transfer in. The engine inserts by sequence number, so
// messages from an older generation land ahead of anything already here.
func (l *QueueLogic) Absorb(req entity.TransferRequest) error {
	lq, err := l.queueFor(req.Spec)
	if err != nil {
		return enterr.Internal("open queue", err)
	}

	// A handoff may be retried after a timeout or by the reconciler.
	if req.MoveID != "" && !l.claimMove(req.Spec.Key(), req.MoveID) {
		l.log.Info("absorb: already applied", "queue", req.Spec.Name, "move", req.MoveID)
		return nil
	}

	bySlot := make(map[uint16][]*engine.Message, len(req.BySlot))
	for slot, wire := range req.BySlot {
		msgs := make([]*engine.Message, 0, len(wire))
		for _, w := range wire {
			m := entity.FromWire(w)
			// write it here before it becomes visible here
			if err := lq.wal.AppendEnqueue(slot, m); err != nil {
				return enterr.Internal("journal transfer", err)
			}
			msgs = append(msgs, m)
		}
		bySlot[slot] = msgs
	}

	if err := lq.q.Absorb(bySlot); err != nil {
		return enterr.Internal("absorb", err)
	}
	lq.q.Thaw()

	n := 0
	for _, m := range bySlot {
		n += len(m)
	}
	if n > 0 {
		l.notifyWork(req.Spec.Key())
	}
	l.log.Info("absorbed transfer", "org", req.Spec.Org, "queue", req.Spec.Name, "messages", n)
	return nil
}

// claimMove records a handoff id and reports whether this is the first time it
// has been seen.
func (l *QueueLogic) claimMove(key engine.QueueKey, moveID string) bool {
	l.moveMu.Lock()
	defer l.moveMu.Unlock()
	if l.appliedMoves == nil {
		l.appliedMoves = map[string]bool{}
	}
	k := key.String() + "/" + moveID
	if l.appliedMoves[k] {
		return false
	}
	l.appliedMoves[k] = true
	return true
}

// PrepareMove holds the named slots out of service and returns copies of what
// they hold.
func (l *QueueLogic) PrepareMove(spec entity.QueueSpec, slots []uint16) (map[uint16][]entity.WireMessage, error) {
	lq, ok := l.lookup(spec.Key())
	if !ok {
		return map[uint16][]entity.WireMessage{}, nil // nothing here to move
	}
	bySlot := lq.q.FreezeSlots(slots)

	out := make(map[uint16][]entity.WireMessage, len(bySlot))
	for slot, msgs := range bySlot {
		wire := make([]entity.WireMessage, 0, len(msgs))
		for _, m := range msgs {
			wire = append(wire, entity.ToWire(m))
		}
		out[slot] = wire
	}
	return out, nil
}

// DiscardMove removes slots this node no longer owns. Called only after the new
// owner is serving them, so it is the point of no return.
func (l *QueueLogic) DiscardMove(spec entity.QueueSpec, slots []uint16) error {
	lq, ok := l.lookup(spec.Key())
	if !ok {
		return nil // already gone
	}
	n := lq.q.DropSlots(slots)
	for _, slot := range slots {
		if err := lq.wal.Compact(slot, nil); err != nil {
			l.log.Warn("discard: truncate log", "queue", spec.Name, "slot", slot, "err", err)
		}
	}
	l.log.Info("discarded moved slots", "org", spec.Org, "queue", spec.Name,
		"slots", len(slots), "messages", n)
	return nil
}

// AbortMove puts frozen slots back into service. The handoff is off; this node
// still holds everything.
func (l *QueueLogic) AbortMove(spec entity.QueueSpec, slots []uint16) error {
	lq, ok := l.lookup(spec.Key())
	if !ok {
		return nil
	}
	lq.q.ThawSlots(slots)
	l.log.Info("aborted move", "org", spec.Org, "queue", spec.Name, "slots", len(slots))
	return nil
}

// HeldSlots reports what this node actually has, so the gateway can compare it
// against placement and repair whatever a failed handoff left behind.
func (l *QueueLogic) HeldSlots() entity.HeldResponse {
	l.mu.RLock()
	snapshot := make(map[engine.QueueKey]*liveQueue, len(l.queues))
	for k, v := range l.queues {
		snapshot[k] = v
	}
	l.mu.RUnlock()

	out := entity.HeldResponse{NodeID: l.cfg.NodeID}
	for k, lq := range snapshot {
		out.Queues = append(out.Queues, entity.HeldSlots{
			Org: k.Org, Name: k.Name,
			Slots:  lq.q.HeldSlots(),
			Frozen: lq.q.FrozenSlots(),
		})
	}
	return out
}
