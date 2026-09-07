package logic

import (
	"context"
	"sync"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

// waiters lets a parked consumer be woken instead of polling.
type waiters struct {
	mu   sync.Mutex
	m    map[string][]chan struct{}
	next map[string]int
}

func newWaiters() *waiters {
	return &waiters{m: map[string][]chan struct{}{}, next: map[string]int{}}
}

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
		delete(w.next, k)
		return
	}
	// An unclaimed wake would be stranded, so pass it on.
	select {
	case <-ch:
		w.deliver(k)
	default:
	}
}

// wake releases one waiter. Waking all of them would send several dequeues for
// one message and most would come back empty.
func (w *waiters) wake(k string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deliver(k)
}

// Always trying the head would drop every wake arriving while the head still
// holds one, leaving the rest asleep.
//
// caller holds mu
func (w *waiters) deliver(k string) {
	list := w.m[k]
	if len(list) == 0 {
		return
	}
	start := w.next[k] % len(list)
	for i := 0; i < len(list); i++ {
		idx := (start + i) % len(list)
		select {
		case list[idx] <- struct{}{}:
			w.next[k] = (idx + 1) % len(list)
			return
		default:
		}
	}
}

// RunSubscriber keeps one notification stream open per node and forwards what
// arrives to whichever consumer is parked on that queue.
func (l *GatewayLogic) RunSubscriber(ctx context.Context, gatewayID string) {
	type stream struct {
		addr   string
		cancel context.CancelFunc
	}
	open := map[string]stream{}
	defer func() {
		for _, s := range open {
			s.cancel()
		}
	}()

	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	// Taken once: every call registers a listener.
	changed := l.membershipRepo.Changed()

	for {
		current := map[string]entity.Member{}
		for _, m := range l.membershipRepo.Members() {
			current[m.ID] = m
		}
		for id, m := range current {
			// A node that re-registers elsewhere keeps its id, so the address
			// has to be part of the comparison.
			if s, ok := open[id]; ok && s.addr == m.Addr {
				continue
			}
			if s, ok := open[id]; ok {
				s.cancel()
			}
			streamCtx, cancel := context.WithCancel(ctx)
			open[id] = stream{addr: m.Addr, cancel: cancel}
			go l.streamFrom(streamCtx, m, gatewayID)
		}
		for id, s := range open {
			if _, ok := current[id]; !ok {
				s.cancel()
				delete(open, id)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-tick.C:
		}
	}
}

// Without backing off, every gateway retries a registered but unreachable node
// once a second each, forever.
const (
	minStreamBackoff = 250 * time.Millisecond
	maxStreamBackoff = 5 * time.Second
)

func (l *GatewayLogic) streamFrom(ctx context.Context, m entity.Member, gatewayID string) {
	backoff := minStreamBackoff
	for {
		ch, err := l.nodesGRPCRepo.Subscribe(ctx, m.Addr, gatewayID)
		if err == nil {
			backoff = minStreamBackoff
			for n := range ch {
				l.wait.wake(key(n.Org, n.Name))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff): // the stream dropped, reconnect
		}
		backoff *= 2
		if backoff > maxStreamBackoff {
			backoff = maxStreamBackoff
		}
	}
}
