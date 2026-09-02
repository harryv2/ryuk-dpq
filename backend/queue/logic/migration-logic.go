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
// Absorb merges a transfer in. The engine inserts by sequence number, so
// messages from an older generation land ahead of anything already here.
func (l *QueueLogic) Absorb(req entity.TransferRequest) error {
	lq, err := l.queueFor(req.Spec)
	if err != nil {
		return enterr.Internal("open queue", err)
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
	l.log.Info("absorbed transfer", "org", req.Spec.Org, "queue", req.Spec.Name, "messages", n)
	return nil
}

// Compact rewrites each queue's log to hold only what is still live.
