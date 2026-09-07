package logic

import (
	"sync"

	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// Notification tells a gateway that a queue on this node gained work.
type Notification struct {
	Org  string
	Name string
}

// subscribers holds one channel per connected gateway. A gateway keeps one
// stream per node, so this is a handful of entries, not one per consumer.
type subscribers struct {
	mu   sync.RWMutex
	next uint64
	m    map[uint64]chan Notification
}

func newSubscribers() *subscribers {
	return &subscribers{m: make(map[uint64]chan Notification)}
}

func (s *subscribers) add() (uint64, chan Notification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	ch := make(chan Notification, 64)
	s.m[s.next] = ch
	return s.next, ch
}

func (s *subscribers) remove(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.m[id]; ok {
		delete(s.m, id)
		close(ch)
	}
}

// publish tells every connected gateway. The node cannot tell which of them has
// a consumer waiting on this queue.
func (s *subscribers) publish(n Notification) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ch := range s.m {
		select {
		case ch <- n:
		default: // a slow gateway is skipped rather than blocking the enqueue
		}
	}
}

func (l *QueueLogic) Subscribe() (uint64, <-chan Notification) {
	return l.subs.add()
}

func (l *QueueLogic) Unsubscribe(id uint64) { l.subs.remove(id) }

func (l *QueueLogic) notifyWork(key engine.QueueKey) {
	l.subs.publish(Notification{Org: key.Org, Name: key.Name})
}
