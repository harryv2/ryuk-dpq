// Package postgres holds the queue settings and where each queue lives. One
// file per table, plus this connection wrapper.
package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is applied at startup. Small enough that a migration tool would be
// more machinery than it saves.
const Schema = `
CREATE TABLE IF NOT EXISTS queues (
    org_id      TEXT   NOT NULL,
    name        TEXT   NOT NULL,
    settings    JSONB  NOT NULL,
    distributed BOOLEAN NOT NULL DEFAULT false,
    state       TEXT   NOT NULL DEFAULT 'active',
    owner_node  TEXT,
    generation  BIGINT NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, name)
);

CREATE TABLE IF NOT EXISTS slot_placement (
    org_id     TEXT     NOT NULL,
    queue_name TEXT     NOT NULL,
    slot       SMALLINT NOT NULL,
    owner_node TEXT     NOT NULL,
    generation BIGINT   NOT NULL DEFAULT 1,
    PRIMARY KEY (org_id, queue_name, slot)
);

CREATE INDEX IF NOT EXISTS queues_owner_idx ON queues (owner_node);
`

// DSN is a named type so the wire graph can tell it from any other string.
type DSN string

type DBWrapper struct {
	Pool *pgxpool.Pool
}

func NewDBWrapper(ctx context.Context, dsn DSN) (*DBWrapper, func(), error) {
	pool, err := pgxpool.New(ctx, string(dsn))
	if err != nil {
		return nil, nil, err
	}

	// Postgres may still be starting when we are, so give it a few tries
	// rather than failing the whole process.
	var pingErr error
	for i := 0; i < 10; i++ {
		if pingErr = pool.Ping(ctx); pingErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if pingErr != nil {
		pool.Close()
		return nil, nil, pingErr
	}

	if _, err := pool.Exec(ctx, Schema); err != nil {
		pool.Close()
		return nil, nil, err
	}
	return &DBWrapper{Pool: pool}, pool.Close, nil
}
