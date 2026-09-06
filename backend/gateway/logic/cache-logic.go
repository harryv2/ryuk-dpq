package logic

import (
	"math/rand"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

const (
	prefixQueue     = "queue:"      // org/name       -> QueueConfig
	prefixStats     = "stats:"      // org/name       -> NodeStats, summed
	prefixNodeStats = "stats-node:" // org/name@node  -> NodeStats, one node's slice
	prefixOwner     = "owner:"      // org/name       -> node id, from the last collection
	prefixRates     = "rates:"      // org/name       -> throughput, from the last two collections
	prefixSample    = "sample:"     // org/name       -> the counters the rates were derived from
)

func queueCacheKey(org, name string) string  { return prefixQueue + key(org, name) }
func statsCacheKey(org, name string) string  { return prefixStats + key(org, name) }
func ownerCacheKey(org, name string) string  { return prefixOwner + key(org, name) }
func ratesCacheKey(org, name string) string  { return prefixRates + key(org, name) }
func sampleCacheKey(org, name string) string { return prefixSample + key(org, name) }
func nodeStatsCacheKey(org, name, node string) string {
	return prefixNodeStats + key(org, name) + "@" + node
}

// negativeTTL keeps a typo in a loop off the database. Short, because a queue
// created a moment ago has to be usable immediately.
const negativeTTL = 2 * time.Second

// envelope lets a miss be cached as a fact, rather than being indistinguishable
// from an entry that was never written.
type envelope[T any] struct {
	Value    T      `json:"value,omitempty"`
	NotFound string `json:"notFound,omitempty"`
}

// fetchViaCache is the whole read-through in one place: look, fall back to the
// source, store the result, remember a miss briefly.
func fetchViaCache[T any](l *GatewayLogic, cacheKey string, ttl time.Duration, source func() (T, error)) (T, error) {
	var env envelope[T]
	if found, err := l.cache.GetAndParse(cacheKey, &env); found {
		if err != nil {
			// Bad data is worse than none: drop it and read through.
			l.log.Warn("cache: dropping unreadable entry", "key", cacheKey, "err", err)
			l.cache.Del(cacheKey)
		} else if env.NotFound != "" {
			return env.Value, enterr.New(enterr.CodeNotFound, env.NotFound)
		} else {
			return env.Value, nil
		}
	}

	value, err := source()
	if err != nil {
		if enterr.CodeOf(err) == enterr.CodeNotFound {
			l.cache.MarshalAndSet(cacheKey, envelope[T]{NotFound: err.Error()}, negativeTTL)
		}
		return value, err
	}

	l.cache.MarshalAndSet(cacheKey, envelope[T]{Value: value}, jitter(ttl))
	return value, nil
}

// readCache is for values a background job writes and the request path only
// reads, where there is no source to fall back to.
func readCache[T any](l *GatewayLogic, cacheKey string) (T, bool) {
	var env envelope[T]
	found, err := l.cache.GetAndParse(cacheKey, &env)
	if !found || err != nil || env.NotFound != "" {
		var zero T
		return zero, false
	}
	return env.Value, true
}

func writeCache[T any](l *GatewayLogic, cacheKey string, ttl time.Duration, value T) {
	l.cache.MarshalAndSet(cacheKey, envelope[T]{Value: value}, jitter(ttl))
}

func (l *GatewayLogic) evictCache(cacheKey string) { l.cache.Del(cacheKey) }

// jitter spreads expiry times out. Entries written together otherwise expire
// together, and every one of them reaches the database in the same instant.
func jitter(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	return ttl + time.Duration(rand.Int63n(int64(ttl)/3+1))
}
