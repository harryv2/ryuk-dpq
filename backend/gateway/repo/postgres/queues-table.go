package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
	"github.com/jackc/pgx/v5"
)

const queueColumns = `org_id, name, settings, distributed, state,
	COALESCE(owner_node,''), generation, created_at, updated_at`

type QueuesTable struct {
	wrapper *DBWrapper
	// holder identifies this gateway in the migration lock, so a lock left by a
	// process that died can be told from one still in use.
	holder string
}

func NewQueuesTable(w *DBWrapper) *QueuesTable {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "gateway"
	}
	return &QueuesTable{wrapper: w, holder: fmt.Sprintf("%s/%d", host, os.Getpid())}
}

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

// migrationLease is how long a gateway may hold a queue in 'migrating' before
// another one may take it over. Long enough that a slow but live migration is
// never stolen; short enough that a dead gateway does not strand a queue.
const migrationLease = 2 * time.Minute

// SetState guards the move into migrating: whichever gateway wins the update
// owns the migration, and the others get a conflict and skip it. This is the
// only coordination between gateways in the system.
func (t *QueuesTable) SetState(ctx context.Context, org, name string, state entity.QueueState) error {
	if state == entity.StateMigrating {
		return t.takeMigrationLock(ctx, org, name)
	}
	q := `UPDATE queues SET state=$3, migrating_by=NULL, migrating_since=NULL, updated_at=now()
	       WHERE org_id=$1 AND name=$2`
	_, err := t.wrapper.Pool.Exec(ctx, q, org, name, string(state))
	return err
}

// takeMigrationLock claims the right to migrate this queue. Only one gateway can
// hold it, and a lock left behind by a gateway that died is reclaimed once its
// lease expires -- otherwise the queue would never rebalance again.
func (t *QueuesTable) takeMigrationLock(ctx context.Context, org, name string) error {
	const q = `
		UPDATE queues
		   SET state='migrating', migrating_by=$3, migrating_since=now(), updated_at=now()
		 WHERE org_id=$1 AND name=$2
		   AND (state='active'
		        OR (state='migrating' AND migrating_since < now() - $4::interval))`
	tag, err := t.wrapper.Pool.Exec(ctx, q, org, name, t.holder, migrationLease.String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
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

func (t *QueuesTable) UpdateSettings(ctx context.Context, org, name string, s entity.QueueSettings) error {
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tag, err := t.wrapper.Pool.Exec(ctx,
		`UPDATE queues SET settings=$3, updated_at=now() WHERE org_id=$1 AND name=$2`,
		org, name, body)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return enterr.NotFound("queue")
	}
	return nil
}
