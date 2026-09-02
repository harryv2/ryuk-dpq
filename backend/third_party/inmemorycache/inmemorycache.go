// Package inmemorycache is a process-local cache over ristretto. Values are
// stored as JSON bytes so one cache can hold every kind of value the callers
// need, each key namespaced by its own prefix.
package inmemorycache

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/dgraph-io/ristretto"
)

type InMemoryCache struct {
	client *ristretto.Cache
	logger *slog.Logger
}

// NewInMemoryCache builds the cache. Ristretto admits keys by frequency rather
// than keeping everything, so a Set is allowed to be dropped -- a caller must
// treat a miss as normal and go to the source, which read-through already does.
func NewInMemoryCache(logger *slog.Logger) *InMemoryCache {
	const (
		numCounters = 1e7     // keys to track the frequency of
		maxCost     = 1 << 28 // 256 MiB
	)
	client, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: numCounters,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		panic(err)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &InMemoryCache{client: client, logger: logger}
}

func (c *InMemoryCache) Set(key string, value []byte, ttl time.Duration) error {
	if ok := c.client.SetWithTTL(key, value, int64(len(key)+len(value)), ttl); !ok {
		return fmt.Errorf("inmemorycache: %q was not admitted", key)
	}
	// Ristretto applies writes through a buffer, so without this a Get straight
	// after a Set can miss.
	c.client.Wait()
	return nil
}

func (c *InMemoryCache) Get(key string) ([]byte, bool) {
	v, ok := c.client.Get(key)
	if !ok {
		return nil, false
	}
	b, ok := v.([]byte)
	return b, ok
}

func (c *InMemoryCache) Has(key string) bool {
	_, ok := c.client.Get(key)
	return ok
}

func (c *InMemoryCache) Del(key string) { c.client.Del(key) }

func (c *InMemoryCache) Close() { c.client.Close() }

// MarshalAndSet stores a value as JSON. A failure to cache is not a failure of
// the caller's work, so it is logged rather than returned.
func (c *InMemoryCache) MarshalAndSet(key string, val any, ttl time.Duration) {
	b, err := json.Marshal(val)
	if err != nil {
		c.logger.Error("cache: could not encode value", "key", key, "err", err)
		return
	}
	if err := c.Set(key, b, ttl); err != nil {
		c.logger.Debug("cache: value not stored", "key", key, "err", err)
	}
}

// GetAndParse reads a value into out. It reports whether the key was present;
// a decode failure is returned so a caller can tell a miss from bad data.
func (c *InMemoryCache) GetAndParse(key string, out any) (bool, error) {
	b, ok := c.Get(key)
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return true, err
	}
	return true, nil
}
