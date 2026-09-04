package logic

import (
	"context"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

// assignOwner picks where a queue should go. Rendezvous hashing is a pure
// function of the member list, so every gateway gets the same answer and
// nothing has to be elected.
func (l *GatewayLogic) assignOwner(org, name string, slot int, distributed bool, width int) (entity.Member, error) {
	members := l.membershipRepo.Members()
	if len(members) == 0 {
		return entity.Member{}, enterr.New(enterr.CodeExhausted, "no nodes available")
	}
	// A distributed queue is confined to its candidate set. A single-node queue
	// is placed whole, so it may land anywhere.
	if distributed {
		members = entity.CandidatesFor(key(org, name), members, width)
	}
	m, ok := entity.OwnerFor(entity.OwnerKey(org, name, slot, distributed), members)
	if !ok {
		return entity.Member{}, enterr.New(enterr.CodeExhausted, "no nodes available")
	}
	return m, nil
}

// ownerAddr resolves where a queue actually is. The stored owner wins over the
// hash: without replication the data exists in one place, so ownership has to
// follow it rather than follow the current member list.
func (l *GatewayLogic) ownerAddr(ctx context.Context, cfg entity.QueueConfig, slot int) (string, string, error) {
	stored := cfg.OwnerNode
	if cfg.Distributed {
		stored = cfg.SlotOwners[uint16(slot)]
	}

	if stored != "" {
		if m, ok := l.membershipRepo.Lookup(stored); ok {
			return stored, m.Addr, nil
		}
		// the owner is down; nothing is reassigned, because its data is only there
		return stored, "", enterr.New(enterr.CodeExhausted,
			"queue owner "+stored+" is not available")
	}

	m, err := l.assignOwner(cfg.Org, cfg.Name, slot, cfg.Distributed, cfg.Settings.PlacementWidth)
	if err != nil {
		return "", "", err
	}
	if cfg.Distributed {
		err = l.slotsPlacementTableRepo.SetOwner(ctx, cfg.Org, cfg.Name, uint16(slot), m.ID, cfg.Generation)
	} else {
		err = l.queueTableRepo.SetOwner(ctx, cfg.Org, cfg.Name, m.ID, cfg.Generation)
	}
	if err != nil {
		return "", "", enterr.Internal("record owner", err)
	}
	l.evictCache(queueCacheKey(cfg.Org, cfg.Name))
	return m.ID, m.Addr, nil
}
