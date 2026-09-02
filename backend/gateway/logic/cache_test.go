package logic

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
	"go.uber.org/mock/gomock"
)

func TestFetchViaCacheReadsSourceOnce(t *testing.T) {
	l, _ := setup(t)
	calls := 0
	load := func() (entity.QueueConfig, error) {
		calls++
		return entity.QueueConfig{Org: "org1", Name: "q", Generation: 7}, nil
	}

	for i := 0; i < 3; i++ {
		got, err := fetchViaCache(l, queueCacheKey("org1", "q"), time.Minute, load)
		if err != nil {
			t.Fatal(err)
		}
		if got.Generation != 7 {
			t.Fatalf("round %d: generation %d, want 7", i, got.Generation)
		}
	}
	if calls != 1 {
		t.Fatalf("the source ran %d times, want 1", calls)
	}
}

func TestFetchViaCacheRemembersAMiss(t *testing.T) {
	l, _ := setup(t)
	calls := 0
	load := func() (entity.QueueConfig, error) {
		calls++
		return entity.QueueConfig{}, enterr.NotFound("queue")
	}

	// A name that does not exist must not reach the database on every call, or
	// a typo in a loop becomes a query per iteration.
	for i := 0; i < 3; i++ {
		_, err := fetchViaCache(l, queueCacheKey("org1", "nope"), time.Minute, load)
		if enterr.CodeOf(err) != enterr.CodeNotFound {
			t.Fatalf("round %d: want a not-found error, got %v", i, err)
		}
	}
	if calls != 1 {
		t.Fatalf("the source ran %d times, want 1", calls)
	}
}

func TestFetchViaCacheDoesNotStoreOtherErrors(t *testing.T) {
	l, _ := setup(t)
	calls := 0
	load := func() (entity.QueueConfig, error) {
		calls++
		return entity.QueueConfig{}, enterr.Internal("read", errors.New("db down"))
	}

	// A database that is briefly down must not turn into a cached failure that
	// outlives it.
	for i := 0; i < 3; i++ {
		if _, err := fetchViaCache(l, queueCacheKey("org1", "q"), time.Minute, load); err == nil {
			t.Fatal("expected the error to surface")
		}
	}
	if calls != 3 {
		t.Fatalf("the source ran %d times, want 3", calls)
	}
}

func TestEvictSendsTheNextReadToTheSource(t *testing.T) {
	l, _ := setup(t)
	calls := 0
	load := func() (entity.QueueConfig, error) {
		calls++
		return entity.QueueConfig{Org: "org1", Name: "q"}, nil
	}

	if _, err := fetchViaCache(l, queueCacheKey("org1", "q"), time.Minute, load); err != nil {
		t.Fatal(err)
	}
	l.evictCache(queueCacheKey("org1", "q"))
	if _, err := fetchViaCache(l, queueCacheKey("org1", "q"), time.Minute, load); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("the source ran %d times, want 2", calls)
	}
}

func TestReadAndWriteCacheRoundTrip(t *testing.T) {
	l, _ := setup(t)
	want := entity.NodeStats{Org: "org1", Name: "q", Ready: [3]int64{1, 2, 3}, InFlight: 4}
	writeCache(l, statsCacheKey("org1", "q"), time.Minute, want)

	got, ok := readCache[entity.NodeStats](l, statsCacheKey("org1", "q"))
	if !ok {
		t.Fatal("nothing came back")
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	if _, ok := readCache[entity.NodeStats](l, statsCacheKey("org1", "other")); ok {
		t.Fatal("a key that was never written should miss")
	}
}

// Values of different types share one cache, so the prefixes have to keep them
// apart: reading a queue key as stats must not return a half-decoded config.
func TestPrefixesKeepValueKindsApart(t *testing.T) {
	l, _ := setup(t)
	writeCache(l, queueCacheKey("org1", "q"), time.Minute,
		entity.QueueConfig{Org: "org1", Name: "q", Generation: 9})
	writeCache(l, statsCacheKey("org1", "q"), time.Minute,
		entity.NodeStats{Org: "org1", Name: "q", InFlight: 11})
	writeCache(l, ownerCacheKey("org1", "q"), time.Minute, "node-3")

	cfg, _ := readCache[entity.QueueConfig](l, queueCacheKey("org1", "q"))
	st, _ := readCache[entity.NodeStats](l, statsCacheKey("org1", "q"))
	owner, _ := readCache[string](l, ownerCacheKey("org1", "q"))

	if cfg.Generation != 9 || st.InFlight != 11 || owner != "node-3" {
		t.Fatalf("values crossed over: cfg=%+v stats=%+v owner=%q", cfg, st, owner)
	}
}

func TestJitterStaysWithinAThirdOfTheTTL(t *testing.T) {
	base := 30 * time.Second
	for i := 0; i < 200; i++ {
		got := jitter(base)
		if got < base || got > base+base/3+time.Nanosecond {
			t.Fatalf("jitter(%v) = %v, outside the expected range", base, got)
		}
	}
	if jitter(0) != 0 {
		t.Fatal("a zero ttl should stay zero")
	}
}

// config is the one read-through in the request path, so it has to go through
// the cache rather than reading the table on every message.
func TestConfigReadsThroughTheCache(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "q").
		Return(cfgFor("org1", "q", "node-1"), nil).Times(1)

	for i := 0; i < 4; i++ {
		if _, err := l.config(context.Background(), "org1", "q"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestThroughputIsDerivedFromTwoReadings(t *testing.T) {
	l, _ := setup(t)

	// One reading gives no rate: a rate needs a change over an interval.
	l.recordRates(entity.NodeStats{Org: "org1", Name: "q", Enqueued: 100, Acked: 40})
	if _, ok := readCache[QueueRates](l, ratesCacheKey("org1", "q")); ok {
		t.Fatal("a rate was reported from a single reading")
	}

	time.Sleep(60 * time.Millisecond)
	l.recordRates(entity.NodeStats{Org: "org1", Name: "q", Enqueued: 130, Acked: 55})

	r, ok := readCache[QueueRates](l, ratesCacheKey("org1", "q"))
	if !ok {
		t.Fatal("no rate after two readings")
	}
	if r.Enqueue <= 0 || r.Ack <= 0 || r.Ack >= r.Enqueue {
		t.Fatalf("rates look wrong: %+v", r)
	}
}

func TestCounterResetDoesNotProduceANegativeRate(t *testing.T) {
	// Counters live in node memory, so a restarted node starts again at zero.
	if got := perSecond(500, 10, 2); got != 0 {
		t.Fatalf("a counter that went backwards gave %v, want 0", got)
	}
	if got := perSecond(10, 30, 2); got != 10 {
		t.Fatalf("perSecond(10, 30, 2) = %v, want 10", got)
	}
}
