package logic

import (
	"context"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

// GetNodeDetails answers for one machine: what it holds, and how much of each
// queue is its share.
func (l *GatewayLogic) GetNodeDetails(ctx context.Context, org, nodeID string) (entity.NodeDetailResponse, error) {
	m, ok := l.membershipRepo.Lookup(nodeID)
	if !ok {
		return entity.NodeDetailResponse{}, enterr.NotFound("node")
	}

	cfgs, err := l.queueTableRepo.ListByOrg(ctx, org)
	if err != nil {
		return entity.NodeDetailResponse{}, enterr.Internal("list queues", err)
	}
	cfgs = l.withPlacement(ctx, cfgs)

	slotsHere := map[string]int{}
	byName := map[string]entity.QueueConfig{}
	for _, c := range cfgs {
		byName[c.Name] = c
		if !c.Distributed {
			if c.OwnerNode == nodeID {
				slotsHere[c.Name] = slotCountFor(false)
			}
			continue
		}
		for _, owner := range c.SlotOwners {
			if owner == nodeID {
				slotsHere[c.Name]++
			}
		}
	}

	resp := entity.NodeDetailResponse{ID: m.ID, Addr: m.Addr, Live: true}

	_, all, err := l.nodesGRPCRepo.StatsAll(ctx, m.Addr)
	if err != nil {
		// Registered but not answering. Placement still says what it owns, so
		// that is worth showing rather than failing the page.
		resp.Live = false
		for name, n := range slotsHere {
			resp.Queues = append(resp.Queues, entity.NodeQueueDetail{
				Queue: name, Distributed: byName[name].Distributed,
				Slots: n, TotalSlots: slotCountFor(byName[name].Distributed),
			})
		}
		return resp, nil
	}

	for _, s := range all {
		if s.Org != org {
			continue // a node holds queues for every org; this page is one org's
		}
		cfg, known := byName[s.Name]
		if !known {
			continue
		}
		q := entity.NodeQueueDetail{
			Queue:        s.Name,
			Distributed:  cfg.Distributed,
			Slots:        slotsHere[s.Name],
			TotalSlots:   slotCountFor(cfg.Distributed),
			Ready:        s.Ready[0] + s.Ready[1] + s.Ready[2],
			ByPriority:   map[string]int64{"low": s.Ready[0], "medium": s.Ready[1], "high": s.Ready[2]},
			InFlight:     s.InFlight,
			Delayed:      s.Delayed,
			OldestAge:    s.OldestAge.Seconds(),
			Enqueued:     s.Enqueued,
			Acked:        s.Acked,
			Expired:      s.Expired,
			Redelivered:  s.Requeued,
			DeadLettered: s.DeadLettered,
			Escapes:      s.Escapes,
		}
		resp.Queues = append(resp.Queues, q)

		resp.Ready += q.Ready
		resp.InFlight += q.InFlight
		resp.Delayed += q.Delayed
		resp.Slots += q.Slots
		if q.OldestAge > resp.OldestAge {
			resp.OldestAge = q.OldestAge
		}
	}
	return resp, nil
}
