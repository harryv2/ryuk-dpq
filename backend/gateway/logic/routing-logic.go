package logic

import (
	"context"
	"encoding/base64"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/slotting"
)

// moveBackoff is how long to wait before re-reading placement and trying again.
// The measured freeze during a handoff is ~2ms, so the first pause alone covers
// the common case and the rest cover a slot large enough to take longer.
var moveBackoff = []time.Duration{5 * time.Millisecond, 25 * time.Millisecond, 100 * time.Millisecond}

// withOwner resolves the owner and runs the call. If the node says the queue
// moved, the cached config is dropped and the call is retried against the new
// owner, so a migration is invisible to callers.
func (l *GatewayLogic) withOwner(
	ctx context.Context,
	cfg entity.QueueConfig,
	slot int,
	call func(addr string, spec entity.QueueSpec) error,
) error {
	// A slot being handed over is held for a couple of milliseconds. Retrying
	// immediately can land inside the same window, so each attempt waits a
	// little longer than the last.
	for attempt := 0; attempt < len(moveBackoff)+1; attempt++ {
		_, addr, err := l.ownerAddr(ctx, cfg, slot)
		if err != nil {
			return err
		}
		err = call(addr, cfg.Spec())
		if err == nil {
			return nil
		}
		if enterr.CodeOf(err) != enterr.CodeMoved || attempt == len(moveBackoff) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(moveBackoff[attempt]):
		}
		l.evictCache(queueCacheKey(cfg.Org, cfg.Name))
		if cfg, err = l.config(ctx, cfg.Org, cfg.Name); err != nil {
			return err
		}
	}
	return enterr.New(enterr.CodeExhausted, "queue moved repeatedly")
}

// rankedOwners lists the machines holding this queue, most urgent work first.
// The ranking comes from the collector's last sweep, so a stale entry costs one
// wasted call rather than a wrong answer.
func (l *GatewayLogic) rankedOwners(cfg entity.QueueConfig) []string {
	type owner struct {
		addr  string
		score int64
	}
	seen := map[string]bool{}
	var owners []owner

	for _, id := range cfg.SlotOwners {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		m, ok := l.membershipRepo.Lookup(id)
		if !ok {
			continue
		}
		score := int64(0)
		if st, ok := readCache[entity.NodeStats](l, nodeStatsCacheKey(cfg.Org, cfg.Name, id)); ok {
			score = st.Ready[2]*1000 + st.Ready[1]*100 + st.Ready[0]
		}
		owners = append(owners, owner{addr: m.Addr, score: score})
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].score > owners[j].score })

	out := make([]string, 0, len(owners))
	for _, o := range owners {
		out = append(out, o.addr)
	}
	return out
}

// slotCountFor is the queue's shape, decided at creation and never changed. It
// has to agree with engine.SlotCountFor: if the gateway thinks a queue has more
// slots than the node does, a message lands in a slot the dispatcher never
// looks at and is never delivered.
func slotCountFor(distributed bool) int { return slotting.CountFor(distributed) }

// An ungrouped message has no ordering to preserve, so it is scattered instead
// of all of them landing in the slot the empty string hashes to.
func slotOf(org, name, group string, slots int) uint16 {
	if group == "" {
		group = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return slotting.SlotFor(org, name, group, slots)
}

func slotFromReceipt(receipt string) (int, error) {
	b, err := base64.RawURLEncoding.DecodeString(receipt)
	if err != nil {
		return 0, enterr.Invalid("malformed receipt")
	}
	parts := strings.SplitN(string(b), ":", 2)
	n, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, enterr.Invalid("malformed receipt")
	}
	return n, nil
}
