package logic

import (
	"context"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

// Collect asks each node once for every queue it holds. One request per node
// per interval, whatever the queue count, and both the stats endpoint and the
// metrics endpoint read the same cache so they cannot disagree.
func (l *GatewayLogic) Collect(ctx context.Context) {
	// Totals are accumulated across nodes before being stored. A distributed
	// queue has a slice of itself on each machine, and writing each slice under
	// the queue's own key would leave whichever node answered last, so the
	// queue list would report a fraction of its depth.
	totals := map[string]entity.NodeStats{}

	for _, m := range l.members.Members() {
		nodeID, all, err := l.nodes.StatsAll(ctx, m.Addr)
		if err != nil {
			l.log.Debug("collect failed", "node", m.ID, "err", err)
			continue
		}
		for _, s := range all {
			k := key(s.Org, s.Name)
			l.stats.Put(k+"@"+nodeID, s)
			l.owner.Put(k, nodeID)
			totals[k] = addStats(totals[k], s)
		}
	}

	for k, s := range totals {
		l.stats.Put(k, s)
	}
}

// addStats sums one node's view into the running total. Ages are a maximum
// rather than a sum: the oldest message is one message, not the total wait.
func addStats(into, s entity.NodeStats) entity.NodeStats {
	if into.Org == "" {
		into.Org, into.Name = s.Org, s.Name
	}
	for i := range s.Ready {
		into.Ready[i] += s.Ready[i]
	}
	into.InFlight += s.InFlight
	into.Delayed += s.Delayed
	into.Enqueued += s.Enqueued
	into.Acked += s.Acked
	into.Expired += s.Expired
	into.Requeued += s.Requeued
	into.DeadLettered += s.DeadLettered
	into.Escapes += s.Escapes
	if s.OldestAge > into.OldestAge {
		into.OldestAge = s.OldestAge
	}
	return into
}

func (l *GatewayLogic) RunCollector(ctx context.Context) {
	t := time.NewTicker(l.cfg.CollectEvery)
	defer t.Stop()
	l.Collect(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.Collect(ctx)
		}
	}
}

func (l *GatewayLogic) Stats(ctx context.Context, org, name string) (entity.QueueStatsResponse, error) {
	cfg, err := l.config(ctx, org, name)
	if err != nil {
		return entity.QueueStatsResponse{}, err
	}

	// A normal queue lives on one machine, so one request gives an exact answer.
	if !cfg.Distributed {
		_, addr, err := l.ownerAddr(ctx, cfg, 0)
		if err == nil && addr != "" {
			if s, err := l.nodes.Stats(ctx, addr, cfg.Spec()); err == nil {
				return toStatsResponse(cfg, s, true), nil
			}
		}
		if s, ok := l.stats.Get(key(org, name)); ok {
			return toStatsResponse(cfg, s, false), nil
		}
		return toStatsResponse(cfg, entity.NodeStats{Org: org, Name: name}, false), nil
	}

	// A distributed queue is spread, so ask every machine holding a slot and add
	// the answers up. They describe slightly different instants, which is why
	// the response says exact=false.
	var total entity.NodeStats
	total.Org, total.Name = org, name
	missing := 0

	for owner, held := range slotsByOwner(cfg) {
		m, live := l.members.Lookup(owner)
		if !live {
			// Its data is only there, so nothing can answer for those slots.
			// Skipping them silently is what makes a depth of zero look real
			// while the messages are still sitting on a machine that is down.
			missing += held
			continue
		}
		s, err := l.nodes.Stats(ctx, m.Addr, cfg.Spec())
		if err != nil {
			// A node that owns slots but has not been sent a message for them
			// yet has no queue in memory and says so. That is zero, not
			// unreachable -- counting it as unavailable makes a healthy cluster
			// look broken.
			if enterr.CodeOf(err) == enterr.CodeNotFound {
				continue
			}
			missing += held
			continue
		}
		total = addStats(total, s)
	}

	resp := toStatsResponse(cfg, total, false)
	resp.UnavailableSlots = missing
	return resp, nil
}

// slotsByOwner counts how many slots of this queue each machine holds, so an
// unreachable machine can be reported as the number of slots it took with it.
func slotsByOwner(cfg entity.QueueConfig) map[string]int {
	out := map[string]int{}
	for _, owner := range cfg.SlotOwners {
		if owner != "" {
			out[owner]++
		}
	}
	return out
}

func (l *GatewayLogic) Metrics(ctx context.Context, org string) ([]entity.QueueStatsResponse, error) {
	cfgs, err := l.queues.ListByOrg(ctx, org)
	if err != nil {
		return nil, enterr.Internal("list queues", err)
	}
	out := make([]entity.QueueStatsResponse, 0, len(cfgs))
	for _, c := range cfgs {
		s, _ := l.stats.Get(key(c.Org, c.Name))
		out = append(out, toStatsResponse(c, s, !c.Distributed))
	}
	return out, nil
}

func (l *GatewayLogic) Cluster(ctx context.Context) (entity.ClusterResponse, error) {
	cfgs, err := l.queues.ListAll(ctx)
	if err != nil {
		return entity.ClusterResponse{}, enterr.Internal("list queues", err)
	}
	cfgs = l.withPlacement(ctx, cfgs)

	// Counted per node and per queue, so a distributed queue shows how much of
	// itself sits on each node instead of appearing once per node it touches.
	type place struct{ node, queue string }
	slots := map[place]int{}
	meta := map[string]entity.ClusterPlacement{}
	order := map[string][]string{}

	add := func(node string, c entity.QueueConfig) {
		k := key(c.Org, c.Name)
		if _, ok := meta[k]; !ok {
			meta[k] = entity.ClusterPlacement{
				Queue: c.Name, Org: c.Org, Distributed: c.Distributed,
				TotalSlots: slotCountFor(c.Distributed),
			}
		}
		if slots[place{node, k}] == 0 {
			order[node] = append(order[node], k)
		}
		slots[place{node, k}]++
	}

	for _, c := range cfgs {
		if c.Distributed {
			for _, owner := range c.SlotOwners {
				if owner != "" {
					add(owner, c)
				}
			}
			continue
		}
		if c.OwnerNode != "" {
			// A normal queue is placed whole, so its node holds every slot.
			add(c.OwnerNode, c)
			slots[place{c.OwnerNode, key(c.Org, c.Name)}] = slotCountFor(false)
		}
	}

	out := entity.ClusterResponse{}
	for _, m := range l.members.Members() {
		node := entity.ClusterNode{ID: m.ID, Addr: m.Addr, Queues: []entity.ClusterPlacement{}}
		for _, k := range order[m.ID] {
			p := meta[k]
			p.Slots = slots[place{m.ID, k}]
			node.Queues = append(node.Queues, p)
		}
		out.Nodes = append(out.Nodes, node)
	}
	return out, nil
}

func toStatsResponse(cfg entity.QueueConfig, s entity.NodeStats, exact bool) entity.QueueStatsResponse {
	return entity.QueueStatsResponse{
		Queue:    cfg.Name,
		Messages: s.Ready[0] + s.Ready[1] + s.Ready[2],
		InFlight: s.InFlight,
		Delayed:  s.Delayed,
		ByPriority: map[string]int64{
			"low": s.Ready[0], "medium": s.Ready[1], "high": s.Ready[2],
		},
		OldestMessageAgeSeconds: s.OldestAge.Seconds(),
		DeadLettered:            s.DeadLettered,
		Enqueued:                s.Enqueued,
		Acked:                   s.Acked,
		Expired:                 s.Expired,
		Redelivered:             s.Requeued,
		StarvationEscapes:       s.Escapes,
		OwnerNode:               cfg.OwnerNode,
		Distributed:             cfg.Distributed,
		Exact:                   exact,
		AsOf:                    time.Now().UTC(),
	}
}
