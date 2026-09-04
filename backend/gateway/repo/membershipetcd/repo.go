// Package membershipetcd watches the live node list. A node holds a lease and
// keeps renewing it, so a node that stops renewing simply disappears -- no
// heartbeat table and no reaper.
package membershipetcd

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const prefix = "/ryuk/members/"

type Repo struct {
	cli *clientv3.Client
	log *slog.Logger

	mu      sync.RWMutex
	members map[string]entity.Member

	once sync.Once

	// One channel per listener, not one shared. A membership change has to
	// reach every watcher: the rebalancer and the notification subscriber both
	// wait on this, and a single buffered channel delivers to whichever
	// receives first, silently starving the other.
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

// Watch loads the current members and then follows changes. Watching rather
// than polling is why a membership change reaches every gateway in
// milliseconds instead of a poll interval.
func (r *Repo) Watch(ctx context.Context) error {
	resp, err := r.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	r.mu.Lock()
	for _, kv := range resp.Kvs {
		var m entity.Member
		if json.Unmarshal(kv.Value, &m) == nil {
			r.members[m.ID] = m
		}
	}
	r.mu.Unlock()
	r.notify()

	r.once.Do(func() { go r.follow(ctx, resp.Header.Revision+1) })
	return nil
}

func (r *Repo) follow(ctx context.Context, rev int64) {
	ch := r.cli.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev))
	for resp := range ch {
		for _, ev := range resp.Events {
			r.mu.Lock()
			if ev.Type == clientv3.EventTypeDelete {
				id := string(ev.Kv.Key)[len(prefix):]
				delete(r.members, id)
				r.log.Info("member left", "node", id)
			} else {
				var m entity.Member
				if json.Unmarshal(ev.Kv.Value, &m) == nil {
					if _, known := r.members[m.ID]; !known {
						r.log.Info("member joined", "node", m.ID, "addr", m.Addr)
					}
					r.members[m.ID] = m
				}
			}
			r.mu.Unlock()
		}
		r.notify()
	}
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

// Changed returns a channel that receives when the member list moves. Each call
// registers a new listener, so call it once and keep the result rather than
// inside a select loop.
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
