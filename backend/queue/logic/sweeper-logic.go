package logic

import (
	"time"

	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// Sweep runs the timers on every queue. Callable directly so tests drive the
// real path without waiting for real time.
func (l *QueueLogic) Sweep() {
	l.mu.RLock()
	snapshot := make(map[engine.QueueKey]*liveQueue, len(l.queues))
	for k, v := range l.queues {
		snapshot[k] = v
	}
	l.mu.RUnlock()

	for key, lq := range snapshot {
		res := lq.q.Sweep()
		for _, m := range res.DeadLettered {
			l.offerDeadLetter(key, m)
		}
		if n := res.Redelivered + res.Released; n > 0 {
			l.notifyWork(key, 0, n)
		}
	}
}

// RunSweeper drives redelivery, expiry and delayed release until stop closes.
// RunSweeper drives redelivery, expiry and delayed release until stop closes.
func (l *QueueLogic) RunSweeper(stop <-chan struct{}) {
	t := time.NewTicker(l.cfg.SweepEvery)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			l.Sweep()
		}
	}
}

// Freeze stops a queue serving and hands back everything it held, keyed by slot
// so placement survives the move.
// Compact rewrites each queue's log to hold only what is still live.
func (l *QueueLogic) Compact() {
	l.mu.RLock()
	snapshot := make(map[engine.QueueKey]*liveQueue, len(l.queues))
	for k, v := range l.queues {
		snapshot[k] = v
	}
	l.mu.RUnlock()

	for key, lq := range snapshot {
		bySlot, err := lq.q.Snapshot()
		if err != nil {
			continue
		}
		for slot, msgs := range bySlot {
			if err := lq.wal.Compact(slot, msgs); err != nil {
				l.log.Warn("compact", "org", key.Org, "queue", key.Name, "slot", slot, "err", err)
			}
		}
	}
}
