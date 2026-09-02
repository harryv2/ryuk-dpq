// Package membershipetcd publishes this node under a lease it keeps renewing.
// Stop renewing -- crash, kill, scale down -- and the key disappears, which is
// the whole liveness mechanism.
package membershipetcd

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const prefix = "/ryuk/members/"

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
	lease, err := r.cli.Grant(ctx, ttlSeconds)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"id": id, "addr": addr})
	if err != nil {
		return err
	}
	if _, err := r.cli.Put(ctx, prefix+id, string(body), clientv3.WithLease(lease.ID)); err != nil {
		return err
	}
	ch, err := r.cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	go func() {
		for range ch {
		}
		r.log.Warn("membership lease ended", "node", id)
	}()
	r.log.Info("registered", "node", id, "addr", addr, "ttlSeconds", ttlSeconds)
	return nil
}
