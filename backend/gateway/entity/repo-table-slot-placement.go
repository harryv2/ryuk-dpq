package entity

import "context"

//go:generate mockgen -source=repo-table-slot-placement.go -destination=../mocks/mock_slot_placement_table_repo.go -package=mocks

// SlotPlacement is where one slot of a distributed queue lives. A normal queue
// has no rows here: it is placed as a whole, so storing sixteen identical rows
// would be recording the same fact sixteen times.
type SlotPlacement struct {
	Org        string
	QueueName  string
	Slot       uint16
	OwnerNode  string
	Generation uint64
}

// SlotPlacementTableRepo owns the slot_placement table.
type SlotPlacementTableRepo interface {
	ListByQueue(ctx context.Context, org, name string) (map[uint16]string, error)
	SetOwner(ctx context.Context, org, name string, slot uint16, owner string, generation uint64) error
	DeleteByQueue(ctx context.Context, org, name string) error
}
