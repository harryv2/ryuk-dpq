// Package membershipetcd publishes this node under a lease it keeps renewing.
package membershipetcd

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const prefix = "/ryuk/members/"

const (
	minRegisterBackoff = 250 * time.Millisecond
	maxRegisterBackoff = 5 * time.Second
)

type Repo struct {
	cli *clientv3.Client
	log *slog.Logger
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
	r := &Repo{cli: cli, log: log}
	return r, func() { _ = cli.Close() }, nil
}

func (r *Repo) Close() error { return r.cli.Close() }

func (r *Repo) Register(ctx context.Context, id, addr string, ttlSeconds int64) error {
	ch, err := r.publish(ctx, id, addr, ttlSeconds)
	if err != nil {
		return err
	}
	r.log.Info("registered", "node", id, "addr", addr, "ttlSeconds", ttlSeconds)
	go r.keepRegistered(ctx, id, addr, ttlSeconds, ch)
	return nil
}

// publish grants a lease, writes this node under it and starts renewing. The
// channel it returns closes when the lease is gone.
func (r *Repo) publish(
	ctx context.Context, id, addr string, ttlSeconds int64,
) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	lease, err := r.cli.Grant(ctx, ttlSeconds)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]string{"id": id, "addr": addr})
	if err != nil {
		return nil, err
	}
	if _, err := r.cli.Put(ctx, prefix+id, string(body), clientv3.WithLease(lease.ID)); err != nil {
		return nil, err
	}
	return r.cli.KeepAlive(ctx, lease.ID)
}

// keepRegistered publishes the node again whenever its lease ends.
func (r *Repo) keepRegistered(
	ctx context.Context, id, addr string, ttlSeconds int64,
	ch <-chan *clientv3.LeaseKeepAliveResponse,
) {
	backoff := minRegisterBackoff
	for {
		for range ch {
		}
		if ctx.Err() != nil {
			return
		}
		r.log.Warn("membership lease ended, re-registering", "node", id)

		ch = nil
		for ch == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			next, err := r.publish(ctx, id, addr, ttlSeconds)
			if err != nil {
				r.log.Warn("re-register", "node", id, "err", err)
				backoff *= 2
				if backoff > maxRegisterBackoff {
					backoff = maxRegisterBackoff
				}
				continue
			}
			r.log.Info("re-registered", "node", id, "addr", addr)
			ch, backoff = next, minRegisterBackoff
		}
	}
}
