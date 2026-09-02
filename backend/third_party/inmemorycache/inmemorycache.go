// Package cache is a small TTL map. Queue configs change rarely, so everything
// they control tolerates being a little stale.
package inmemorycache

import (
	"sync"
	"time"
)

type entry[V any] struct {
	val V
	exp time.Time
}

type TTL[K comparable, V any] struct {
	mu  sync.RWMutex
	m   map[K]entry[V]
	ttl time.Duration
	now func() time.Time
}

func New[K comparable, V any](ttl time.Duration) *TTL[K, V] {
	return &TTL[K, V]{m: make(map[K]entry[V]), ttl: ttl, now: time.Now}
}

func (c *TTL[K, V]) Get(k K) (V, bool) {
	c.mu.RLock()
	e, ok := c.m[k]
	c.mu.RUnlock()
	if !ok || c.now().After(e.exp) {
		var zero V
		return zero, false
	}
	return e.val, true
}

func (c *TTL[K, V]) Put(k K, v V) { c.PutFor(k, v, c.ttl) }

func (c *TTL[K, V]) PutFor(k K, v V, ttl time.Duration) {
	c.mu.Lock()
	c.m[k] = entry[V]{val: v, exp: c.now().Add(ttl)}
	c.mu.Unlock()
}

func (c *TTL[K, V]) Evict(k K) {
	c.mu.Lock()
	delete(c.m, k)
	c.mu.Unlock()
}

func (c *TTL[K, V]) Snapshot() map[K]V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[K]V, len(c.m))
	now := c.now()
	for k, e := range c.m {
		if now.Before(e.exp) {
			out[k] = e.val
		}
	}
	return out
}
