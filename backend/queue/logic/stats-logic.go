package logic

import (
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

func (l *QueueLogic) Stats(spec entity.QueueSpec) (entity.QueueStats, error) {
	lq, ok := l.lookup(spec.Key())
	if !ok {
		return entity.QueueStats{}, enterr.NotFound("queue")
	}
	return toStats(spec.Key(), lq.q.Stats()), nil
}

// StatsAll answers for every queue on this node in one call, so collecting
// metrics costs one request per node rather than one per queue.
func (l *QueueLogic) StatsAll() entity.StatsAllResponse {
	l.mu.RLock()
	snapshot := make(map[engine.QueueKey]*liveQueue, len(l.queues))
	for k, v := range l.queues {
		snapshot[k] = v
	}
	l.mu.RUnlock()

	out := entity.StatsAllResponse{NodeID: l.cfg.NodeID}
	for k, lq := range snapshot {
		out.Queues = append(out.Queues, toStats(k, lq.q.Stats()))
	}
	return out
}

func toStats(k engine.QueueKey, s engine.Stats) entity.QueueStats {
	return entity.QueueStats{
		Org:          k.Org,
		Name:         k.Name,
		Ready:        s.Ready,
		InFlight:     s.InFlight,
		Delayed:      s.Delayed,
		OldestAge:    s.OldestAge,
		Enqueued:     s.Enqueued,
		Acked:        s.Acked,
		Expired:      s.Expired,
		Requeued:     s.Requeued,
		DeadLettered: s.DeadLettered,
		Escapes:      s.Escapes,
	}
}

func (l *QueueLogic) offerDeadLetter(key engine.QueueKey, m *engine.Message) {
	select {
	case l.deadLetters <- deadLetter{Key: key, Msg: m}:
	default:
		l.log.Warn("dead-letter buffer full, dropping", "org", key.Org, "queue", key.Name, "id", m.ID)
	}
}

// DeadLetters drains what the sweeper has moved out of queues so the caller can
// route it to a dead-letter queue.
func (l *QueueLogic) DeadLetters() []deadLetter {
	var out []deadLetter
	for {
		select {
		case d := <-l.deadLetters:
			out = append(out, d)
		default:
			return out
		}
	}
}
