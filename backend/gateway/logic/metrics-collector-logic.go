package logic

import (
	"context"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

// Collect asks each node once for every queue it holds: one request per node
// per interval, whatever the queue count.
func (l *GatewayLogic) Collect(ctx context.Context) {
	// Summed before storing. Writing each node's slice under the queue's own key
	// would leave whichever answered last, so the list would show a fraction.
	totals := map[string]entity.NodeStats{}

	for _, m := range l.membershipRepo.Members() {
		nodeID, all, err := l.nodesGRPCRepo.StatsAll(ctx, m.Addr)
		if err != nil {
			l.log.Debug("collect failed", "node", m.ID, "err", err)
			continue
		}
		for _, s := range all {
			k := key(s.Org, s.Name)
			writeCache(l, nodeStatsCacheKey(s.Org, s.Name, nodeID), l.statsTTL(), s)
			writeCache(l, ownerCacheKey(s.Org, s.Name), l.statsTTL(), nodeID)
			totals[k] = addStats(totals[k], s)
		}
	}

	for k, s := range totals {
		writeCache(l, prefixStats+k, l.statsTTL(), s)
		l.recordRates(s)
	}
}

// QueueRates is throughput per second, worked out from the change in the
// lifetime counters between two collections.
type QueueRates struct {
	Enqueue float64 `json:"enqueue"`
	Ack     float64 `json:"ack"`
}

// rateSample is the pair of counters a rate was last derived from.
type rateSample struct {
	Enqueued uint64    `json:"enqueued"`
	Acked    uint64    `json:"acked"`
	At       time.Time `json:"at"`
}

func (l *GatewayLogic) recordRates(s entity.NodeStats) {
	now := time.Now()
	next := rateSample{Enqueued: s.Enqueued, Acked: s.Acked, At: now}

	if prev, ok := readCache[rateSample](l, sampleCacheKey(s.Org, s.Name)); ok {
		if elapsed := now.Sub(prev.At).Seconds(); elapsed > 0 {
			writeCache(l, ratesCacheKey(s.Org, s.Name), l.statsTTL(), QueueRates{
				Enqueue: perSecond(prev.Enqueued, s.Enqueued, elapsed),
				Ack:     perSecond(prev.Acked, s.Acked, elapsed),
			})
		}
	}
	writeCache(l, sampleCacheKey(s.Org, s.Name), l.statsTTL(), next)
}

// perSecond ignores a counter that went backwards. Counters live in node memory,
// so a restarted node starts again from zero and the delta is meaningless.
func perSecond(before, after uint64, elapsed float64) float64 {
	if after < before {
		return 0
	}
	return float64(after-before) / elapsed
}

// statsTTL outlives a few collection rounds, so one slow round does not empty
// the queue list.
func (l *GatewayLogic) statsTTL() time.Duration { return 3 * l.cfg.CollectEvery }

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
			if s, err := l.nodesGRPCRepo.Stats(ctx, addr, cfg.Spec()); err == nil {
				return l.withRates(toStatsResponse(cfg, s, true), org, name), nil
			}
		}
		if s, ok := readCache[entity.NodeStats](l, statsCacheKey(org, name)); ok {
			return l.withRates(toStatsResponse(cfg, s, false), org, name), nil
		}
		return l.withRates(toStatsResponse(cfg, entity.NodeStats{Org: org, Name: name}, false), org, name), nil
	}

	// A distributed queue is spread, so ask every machine holding a slot and add
	// the answers up. They describe slightly different instants, which is why
	// the response says exact=false.
	var total entity.NodeStats
	total.Org, total.Name = org, name
	missing := 0

	for owner, held := range slotsByOwner(cfg) {
		m, live := l.membershipRepo.Lookup(owner)
		if !live {
			// Its data is only there. Skipping silently is what makes a depth of
			// zero look real while the messages sit on a machine that is down.
			missing += held
			continue
		}
		s, err := l.nodesGRPCRepo.Stats(ctx, m.Addr, cfg.Spec())
		if err != nil {
			// It owns slotsPlacementTableRepo but has never been sent a message for them, so it has
			// no queue in memory. That is zero, not unreachable.
			if enterr.CodeOf(err) == enterr.CodeNotFound {
				continue
			}
			missing += held
			continue
		}
		total = addStats(total, s)
	}

	resp := l.withRates(toStatsResponse(cfg, total, false), org, name)
	resp.UnavailableSlots = missing
	return resp, nil
}

// slotsByOwner counts how many slotsPlacementTableRepo of this queue each machine holds, so an
// unreachable machine can be reported as the number of slotsPlacementTableRepo it took with it.
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
	cfgs, err := l.queueTableRepo.ListByOrg(ctx, org)
	if err != nil {
		return nil, enterr.Internal("list queueTableRepo", err)
	}
	out := make([]entity.QueueStatsResponse, 0, len(cfgs))
	for _, c := range cfgs {
		s, _ := readCache[entity.NodeStats](l, statsCacheKey(c.Org, c.Name))
		out = append(out, l.withRates(toStatsResponse(c, s, !c.Distributed), c.Org, c.Name))
	}
	return out, nil
}

func (l *GatewayLogic) Cluster(ctx context.Context) (entity.ClusterResponse, error) {
	cfgs, err := l.queueTableRepo.ListAll(ctx)
	if err != nil {
		return entity.ClusterResponse{}, enterr.Internal("list queueTableRepo", err)
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
	for _, m := range l.membershipRepo.Members() {
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

// withRates fills in throughput, which the collector works out rather than the
// node: a node reports totals, and a rate needs two readings.
func (l *GatewayLogic) withRates(resp entity.QueueStatsResponse, org, name string) entity.QueueStatsResponse {
	if r, ok := readCache[QueueRates](l, ratesCacheKey(org, name)); ok {
		resp.EnqueueRate, resp.AckRate = r.Enqueue, r.Ack
	}
	return resp
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
