package logic

import (
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// queueFor returns the live queue, creating it from the spec if this node has
// not seen it yet. Queues materialise on first use so an idle one costs nothing.
func (l *QueueLogic) queueFor(spec entity.QueueSpec) (*liveQueue, error) {
	key := spec.Key()

	l.mu.RLock()
	lq, ok := l.queues[key]
	l.mu.RUnlock()
	if ok {
		// The spec travels with every request, so a settings change reaches the
		// node on the next call rather than needing to be pushed to it.
		if lq.q.Reconfigure(spec.EngineConfig()) {
			l.log.Info("queue reconfigured", "org", key.Org, "queue", key.Name)
		}
		return lq, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if lq, ok := l.queues[key]; ok {
		return lq, nil
	}

	wal, err := l.wals.Open(key)
	if err != nil {
		return nil, err
	}
	lq = &liveQueue{
		q:   engine.New(spec.EngineConfig(), l.clock, wal, spec.Generation),
		wal: wal,
	}
	l.queues[key] = lq
	l.log.Info("queue opened", "org", key.Org, "queue", key.Name,
		"generation", spec.Generation, "distributed", spec.Distributed)
	return lq, nil
}

func (l *QueueLogic) lookup(key engine.QueueKey) (*liveQueue, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	lq, ok := l.queues[key]
	return lq, ok
}

func (l *QueueLogic) Held() []engine.QueueKey {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]engine.QueueKey, 0, len(l.queues))
	for k := range l.queues {
		out = append(out, k)
	}
	return out
}

func (l *QueueLogic) Drop(key engine.QueueKey) error {
	l.mu.Lock()
	lq, ok := l.queues[key]
	delete(l.queues, key)
	l.mu.Unlock()

	if ok {
		_ = lq.wal.Close()
	}
	return l.wals.Remove(key)
}
