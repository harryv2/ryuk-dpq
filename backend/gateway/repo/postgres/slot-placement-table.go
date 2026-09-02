package postgres

import (
	"context"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

type SlotPlacementTable struct {
	wrapper *DBWrapper
}

func NewSlotPlacementTable(w *DBWrapper) *SlotPlacementTable {
	return &SlotPlacementTable{wrapper: w}
}

func (t *SlotPlacementTable) ListByQueue(ctx context.Context, org, name string) (map[uint16]string, error) {
	rows, err := t.wrapper.Pool.Query(ctx,
		`SELECT slot, owner_node FROM slot_placement WHERE org_id=$1 AND queue_name=$2`, org, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[uint16]string{}
	for rows.Next() {
		var slot int16
		var owner string
		if err := rows.Scan(&slot, &owner); err != nil {
			return nil, err
		}
		out[uint16(slot)] = owner
	}
	return out, rows.Err()
}

func (t *SlotPlacementTable) SetOwner(ctx context.Context, org, name string, slot uint16, owner string, generation uint64) error {
	_, err := t.wrapper.Pool.Exec(ctx, `
		INSERT INTO slot_placement (org_id, queue_name, slot, owner_node, generation)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (org_id, queue_name, slot)
		DO UPDATE SET owner_node=EXCLUDED.owner_node, generation=EXCLUDED.generation`,
		org, name, int16(slot), owner, int64(generation))
	return err
}

func (t *SlotPlacementTable) DeleteByQueue(ctx context.Context, org, name string) error {
	_, err := t.wrapper.Pool.Exec(ctx,
		`DELETE FROM slot_placement WHERE org_id=$1 AND queue_name=$2`, org, name)
	return err
}

var _ entity.SlotPlacementTableRepo = (*SlotPlacementTable)(nil)
