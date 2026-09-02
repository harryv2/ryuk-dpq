package logic

import (
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
)

// Recover rebuilds every queue this node has data for. Leases do not survive a
// restart, so everything that was in flight comes back as available.
func (l *QueueLogic) Recover() error {
	keys, err := l.wals.List()
	if err != nil {
		return err
	}
	for _, key := range keys {
		spec := entity.QueueSpec{Org: key.Org, Name: key.Name}
		lq, err := l.queueFor(spec)
		if err != nil {
			l.log.Error("recover: open", "org", key.Org, "queue", key.Name, "err", err)
			continue
		}
		bySlot, err := lq.wal.Replay()
		if err != nil {
			l.log.Error("recover: replay", "org", key.Org, "queue", key.Name, "err", err)
			continue
		}
		n := 0
		for _, msgs := range bySlot {
			n += len(msgs)
		}
		if n == 0 {
			continue
		}
		if err := lq.q.Absorb(bySlot); err != nil {
			l.log.Error("recover: absorb", "org", key.Org, "queue", key.Name, "err", err)
			continue
		}
		l.log.Info("queue recovered", "org", key.Org, "queue", key.Name, "messages", n)
	}
	return nil
}

// Sweep runs the timers on every queue. Callable directly so tests drive the
// real path without waiting for real time.
