package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
	"github.com/jackc/pgx/v5"
)

const queueColumns = `org_id, name, settings, distributed, state,
	COALESCE(owner_node,''), generation, created_at, updated_at`

type QueuesTable struct {
	wrapper *DBWrapper
}

func NewQueuesTable(w *DBWrapper) *QueuesTable { return &QueuesTable{wrapper: w} }

// Create returns the existing row when the name is taken. The primary key does
// the concurrency work: two gateways creating the same queue cannot both win.
func (t *QueuesTable) Create(ctx context.Context, cfg entity.QueueConfig) (entity.QueueConfig, bool, error) {
	body, err := json.Marshal(cfg.Settings)
	if err != nil {
		return cfg, false, err
	}
	var created bool
	err = t.wrapper.Pool.QueryRow(ctx, `
		INSERT INTO queues (org_id, name, settings, distributed, state, owner_node, generation)
		VALUES ($1,$2,$3,$4,'active',$5,1)
		ON CONFLICT (org_id, name) DO NOTHING
		RETURNING true`,
		cfg.Org, cfg.Name, body, cfg.Distributed, nullable(cfg.OwnerNode),
	).Scan(&created)

	if errors.Is(err, pgx.ErrNoRows) {
		existing, getErr := t.Get(ctx, cfg.Org, cfg.Name)
		return existing, false, getErr
	}
	if err != nil {
		return cfg, false, err
	}
	out, err := t.Get(ctx, cfg.Org, cfg.Name)
	return out, true, err
}

func (t *QueuesTable) Get(ctx context.Context, org, name string) (entity.QueueConfig, error) {
	row := t.wrapper.Pool.QueryRow(ctx,
		`SELECT `+queueColumns+` FROM queues WHERE org_id=$1 AND name=$2`, org, name)

	cfg, err := scanQueue(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return cfg, enterr.NotFound("queue")
	}
	return cfg, err
}

func (t *QueuesTable) ListByOrg(ctx context.Context, org string) ([]entity.QueueConfig, error) {
	return t.list(ctx, `SELECT `+queueColumns+` FROM queues WHERE org_id=$1 ORDER BY name`, org)
}

func (t *QueuesTable) ListAll(ctx context.Context) ([]entity.QueueConfig, error) {
	return t.list(ctx, `SELECT `+queueColumns+` FROM queues ORDER BY org_id, name`)
}

func (t *QueuesTable) list(ctx context.Context, q string, args ...any) ([]entity.QueueConfig, error) {
	rows, err := t.wrapper.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []entity.QueueConfig
	for rows.Next() {
		cfg, err := scanQueue(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cfg)
	}
	return out, rows.Err()
}

// SetOwner records where a queue is now and bumps the generation, so sequence
// numbers from the new owner sort after the old owner's.
func (t *QueuesTable) SetOwner(ctx context.Context, org, name, owner string, generation uint64) error {
	_, err := t.wrapper.Pool.Exec(ctx,
		`UPDATE queues SET owner_node=$3, generation=$4, updated_at=now()
		 WHERE org_id=$1 AND name=$2`, org, name, owner, int64(generation))
	return err
}

// SetState guards the move into migrating: whichever gateway wins the update
// owns the migration, and the others get a conflict and skip it. This is the
// only coordination between gateways in the system.
func (t *QueuesTable) SetState(ctx context.Context, org, name string, state entity.QueueState) error {
	q := `UPDATE queues SET state=$3, updated_at=now() WHERE org_id=$1 AND name=$2`
	if state == entity.StateMigrating {
		q += ` AND state='active'`
	}
	tag, err := t.wrapper.Pool.Exec(ctx, q, org, name, string(state))
	if err != nil {
		return err
	}
	if state == entity.StateMigrating && tag.RowsAffected() == 0 {
		return enterr.New(enterr.CodeConflict, "queue is already migrating")
	}
	return nil
}

func (t *QueuesTable) Delete(ctx context.Context, org, name string) error {
	_, err := t.wrapper.Pool.Exec(ctx, `DELETE FROM queues WHERE org_id=$1 AND name=$2`, org, name)
	return err
}

type scanner interface{ Scan(dest ...any) error }

func scanQueue(row scanner) (entity.QueueConfig, error) {
	var (
		cfg      entity.QueueConfig
		settings []byte
		state    string
		gen      int64
		created  time.Time
		updated  time.Time
	)
	err := row.Scan(&cfg.Org, &cfg.Name, &settings, &cfg.Distributed, &state,
		&cfg.OwnerNode, &gen, &created, &updated)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(settings, &cfg.Settings); err != nil {
		return cfg, err
	}
	cfg.State = entity.QueueState(state)
	cfg.Generation = uint64(gen)
	cfg.CreatedAt, cfg.UpdatedAt = created, updated
	return cfg, nil
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
