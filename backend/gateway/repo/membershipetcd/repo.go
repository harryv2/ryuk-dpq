// Package membershipetcd watches the live node list.
package membershipetcd

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	minWatchBackoff = 250 * time.Millisecond
	maxWatchBackoff = 5 * time.Second
)

const (
	prefix = "/ryuk/members/"
	// Everything the system owns, which is what the registry view lists.
	registryPrefix = "/ryuk/"
)

type Repo struct {
	cli *clientv3.Client
	log *slog.Logger

	mu      sync.RWMutex
	members map[string]entity.Member

	once sync.Once

	// One channel per listener, not one shared.
	listenMu  sync.Mutex
	listeners []chan struct{}
}

// Endpoints is a named type so the wire graph can tell it from any other
// string slice.
type Endpoints []string

func New(endpoints Endpoints, log *slog.Logger) (*Repo, func(), error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, nil, err
	}
	r := &Repo{
		cli:     cli,
		log:     log,
		members: map[string]entity.Member{},
	}
	return r, func() { _ = cli.Close() }, nil
}

// Watch loads the current members and then follows changes.
func (r *Repo) Watch(ctx context.Context) error {
	rev, err := r.resync(ctx)
	if err != nil {
		return err
	}
	r.once.Do(func() { go r.follow(ctx, rev+1) })
	return nil
}

// resync replaces the member list with what etcd holds now and reports the
// revision it read at, so a watch can start from exactly there and miss
// nothing.
func (r *Repo) resync(ctx context.Context) (int64, error) {
	resp, err := r.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	members := make(map[string]entity.Member, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var m entity.Member
		if json.Unmarshal(kv.Value, &m) == nil {
			members[m.ID] = m
		}
	}
	r.setMembers(members)
	return resp.Header.Revision, nil
}

func (r *Repo) setMembers(members map[string]entity.Member) {
	r.mu.Lock()
	r.members = members
	r.mu.Unlock()
	r.notify()
}

// follow keeps a watch open for the life of the process.
func (r *Repo) follow(ctx context.Context, rev int64) {
	backoff := minWatchBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		rev = r.watchFrom(ctx, rev)
		if ctx.Err() != nil {
			return
		}
		r.log.Warn("membership watch ended, reconnecting", "in", backoff)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		fresh, err := r.resync(ctx)
		if err != nil {
			r.log.Warn("membership resync", "err", err)
			backoff *= 2
			if backoff > maxWatchBackoff {
				backoff = maxWatchBackoff
			}
			continue
		}
		rev, backoff = fresh+1, minWatchBackoff
	}
}

// watchFrom consumes one watch until it ends, and reports the revision to
// resume from.
func (r *Repo) watchFrom(ctx context.Context, rev int64) int64 {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for resp := range r.cli.Watch(wctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev)) {
		if err := resp.Err(); err != nil {
			// A compacted revision cannot be resumed from. The resync that
			// follows is what recovers it.
			r.log.Warn("membership watch", "err", err)
			return rev
		}
		for _, ev := range resp.Events {
			r.apply(ev)
		}
		rev = resp.Header.Revision + 1
		r.notify()
	}
	return rev
}

func (r *Repo) apply(ev *clientv3.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ev.Type == clientv3.EventTypeDelete {
		id := string(ev.Kv.Key)[len(prefix):]
		delete(r.members, id)
		r.log.Info("member left", "node", id)
		return
	}
	var m entity.Member
	if json.Unmarshal(ev.Kv.Value, &m) != nil {
		return
	}
	if _, known := r.members[m.ID]; !known {
		r.log.Info("member joined", "node", m.ID, "addr", m.Addr)
	}
	r.members[m.ID] = m
}

func (r *Repo) notify() {
	r.listenMu.Lock()
	defer r.listenMu.Unlock()
	for _, ch := range r.listeners {
		// Buffered by one: a listener that has not drained its previous signal
		// already knows the membership moved, so dropping this one loses
		// nothing.
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Changed returns a channel that receives when the member list moves.
func (r *Repo) Changed() <-chan struct{} {
	ch := make(chan struct{}, 1)
	r.listenMu.Lock()
	r.listeners = append(r.listeners, ch)
	r.listenMu.Unlock()
	return ch
}

// Members is sorted so two components computing placement from the same set get
// the same answer whatever order the map happened to iterate in.
func (r *Repo) Members() []entity.Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]entity.Member, 0, len(r.members))
	for _, m := range r.members {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Repo) Lookup(id string) (entity.Member, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.members[id]
	return m, ok
}

// Entries reads every key the system owns, with the lease still holding each
// one.
func (r *Repo) Entries(ctx context.Context) ([]entity.RegistryEntry, error) {
	resp, err := r.cli.Get(ctx, registryPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]entity.RegistryEntry, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		e := entity.RegistryEntry{
			Key:     string(kv.Key),
			Value:   string(kv.Value),
			Version: kv.Version,
		}
		if kv.Lease != 0 {
			e.Lease = strconv.FormatInt(kv.Lease, 16)
			// A key whose lease has gone is about to vanish; reporting the
			// remaining seconds is what makes that visible before it does.
			if ttl, err := r.cli.TimeToLive(ctx, clientv3.LeaseID(kv.Lease)); err == nil {
				e.TTLSeconds = ttl.TTL
			}
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
