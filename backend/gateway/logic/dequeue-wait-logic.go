package logic

import (
	"context"
	"sync"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

// waiters lets a parked consumer be woken instead of polling. One stream per
// node carries every queue's notifications, so ten thousand waiting consumers
// cost ten thousand map entries and no extra connections.
type waiters struct {
	mu sync.Mutex
	m  map[string][]chan struct{}
}

func newWaiters() *waiters { return &waiters{m: map[string][]chan struct{}{}} }

func (w *waiters) park(k string) chan struct{} {
	ch := make(chan struct{}, 1)
	w.mu.Lock()
	w.m[k] = append(w.m[k], ch)
	w.mu.Unlock()
	return ch
}

func (w *waiters) unpark(k string, ch chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	list := w.m[k]
	for i, c := range list {
		if c == ch {
			w.m[k] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(w.m[k]) == 0 {
		delete(w.m, k)
	}
}

// wake releases one waiter. Waking all of them would send several dequeues for
// one message and most would come back empty.
func (w *waiters) wake(k string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if list := w.m[k]; len(list) > 0 {
		select {
		case list[0] <- struct{}{}:
		default:
		}
	}
}

// RunSubscriber keeps one notification stream open per node and forwards what
// arrives to whichever consumer is parked on that queue.
func (l *GatewayLogic) RunSubscriber(ctx context.Context, gatewayID string) {
	open := map[string]context.CancelFunc{}
	defer func() {
		for _, cancel := range open {
			cancel()
		}
	}()

	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	for {
		current := map[string]entity.Member{}
		for _, m := range l.membershipRepo.Members() {
			current[m.ID] = m
		}
		for id, m := range current {
			if _, ok := open[id]; ok {
				continue
			}
			streamCtx, cancel := context.WithCancel(ctx)
			open[id] = cancel
			go l.streamFrom(streamCtx, m, gatewayID, func() {
				l.subMu.Lock()
				delete(l.subOpen, m.ID)
				l.subMu.Unlock()
			})
			l.subMu.Lock()
			l.subOpen[m.ID] = true
			l.subMu.Unlock()
		}
		for id, cancel := range open {
			if _, ok := current[id]; !ok {
				cancel()
				delete(open, id)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-l.membershipRepo.Changed():
		case <-tick.C:
		}
	}
}

func (l *GatewayLogic) streamFrom(ctx context.Context, m entity.Member, gatewayID string, done func()) {
	defer done()
	for {
		ch, err := l.nodesGRPCRepo.Subscribe(ctx, m.Addr, gatewayID)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}
		for n := range ch {
			l.wait.wake(key(n.Org, n.Name))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second): // the stream dropped, reconnect
		}
	}
}
