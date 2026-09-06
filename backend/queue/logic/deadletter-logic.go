package logic

import (
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// maxPendingDeadLetters bounds what one queue can hold if the gateway stops
// draining.
const maxPendingDeadLetters = 10000

// offerDeadLetter records a message the queue has given up on.
func (l *QueueLogic) offerDeadLetter(key engine.QueueKey, slot uint16, m *engine.Message) {
	lq, ok := l.lookup(key)
	if !ok {
		return
	}
	if err := lq.wal.AppendDeadLetter(slot, m); err != nil {
		l.log.Error("dead-letter: journal", "org", key.Org, "queue", key.Name, "id", m.ID, "err", err)
		return
	}

	lq.dlMu.Lock()
	defer lq.dlMu.Unlock()
	if lq.dl == nil {
		lq.dl = map[string]entity.PendingDeadLetter{}
	}
	if len(lq.dl) >= maxPendingDeadLetters {
		l.log.Warn("dead-letter: too many waiting, dropping",
			"org", key.Org, "queue", key.Name, "id", m.ID)
		return
	}
	lq.dl[m.ID] = entity.PendingDeadLetter{Slot: slot, Msg: m}
}

// DeadLetters reports what every queue on this node has given up on. Nothing is
// removed: the gateway has to confirm it moved them first.
func (l *QueueLogic) DeadLetters() entity.DeadLetterResponse {
	l.mu.RLock()
	snapshot := make(map[engine.QueueKey]*liveQueue, len(l.queues))
	for k, v := range l.queues {
		snapshot[k] = v
	}
	l.mu.RUnlock()

	out := entity.DeadLetterResponse{}
	for key, lq := range snapshot {
		lq.dlMu.Lock()
		for _, d := range lq.dl {
			out.DeadLetters = append(out.DeadLetters, entity.DeadLetterItem{
				Org: key.Org, Name: key.Name, Slot: d.Slot, Msg: d.Msg,
			})
		}
		lq.dlMu.Unlock()
	}
	return out
}

// AckDeadLetters drops the ones the gateway has moved. Called after they are in
// the dead-letter queue, never before.
func (l *QueueLogic) AckDeadLetters(ids []string) error {
	l.mu.RLock()
	snapshot := make([]*liveQueue, 0, len(l.queues))
	for _, v := range l.queues {
		snapshot = append(snapshot, v)
	}
	l.mu.RUnlock()

	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}

	for _, lq := range snapshot {
		lq.dlMu.Lock()
		var drained []string
		for id := range lq.dl {
			if want[id] {
				drained = append(drained, id)
			}
		}
		for _, id := range drained {
			delete(lq.dl, id)
		}
		lq.dlMu.Unlock()

		for _, id := range drained {
			if err := lq.wal.AppendDeadLetterDrained(id); err != nil {
				l.log.Warn("dead-letter: journal drained", "id", id, "err", err)
			}
		}
	}
	return nil
}

// recoverDeadLetters restores what the node owed the gateway when it stopped.
func (l *QueueLogic) recoverDeadLetters(lq *liveQueue) int {
	pending, err := lq.wal.ReplayDeadLetters()
	if err != nil {
		l.log.Error("dead-letter: replay", "err", err)
		return 0
	}
	if len(pending) == 0 {
		return 0
	}
	lq.dlMu.Lock()
	if lq.dl == nil {
		lq.dl = map[string]entity.PendingDeadLetter{}
	}
	for _, d := range pending {
		lq.dl[d.Msg.ID] = d
	}
	lq.dlMu.Unlock()

	// The log is full of drained records by now; rewriting it to what is
	// actually outstanding keeps it from growing for the life of the queue.
	if err := lq.wal.CompactDeadLetters(pending); err != nil {
		l.log.Warn("dead-letter: compact", "err", err)
	}
	return len(pending)
}
