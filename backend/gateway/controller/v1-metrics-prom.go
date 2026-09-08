package controller

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

// promQueue is one queue's numbers alongside the org they belong to, so every
// family can be walked over the whole set rather than a single org's slice.
type promQueue struct {
	org   string
	stats entity.QueueStatsResponse
}

// PrometheusMetrics renders the same numbers the JSON endpoints serve. Written
// by hand so the service needs no metrics library.
func (h *Handlers) PrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	var all []promQueue
	for org := range uniqueOrgs(h.orgs) {
		queues, err := h.app.GetMetrics(r.Context(), org)
		if err != nil {
			continue
		}
		for _, q := range queues {
			all = append(all, promQueue{org: org, stats: q})
		}
	}
	// Orgs come out of a map, so without this the sample order changes between
	// scrapes for no reason.
	sort.Slice(all, func(i, j int) bool {
		if all[i].org != all[j].org {
			return all[i].org < all[j].org
		}
		return all[i].stats.Queue < all[j].stats.Queue
	})

	var b strings.Builder
	// One HELP and TYPE per family, with every sample of that family beneath
	// it. Repeating the pair per queue is not the exposition format: Prometheus
	// happens to tolerate it, and stricter parsers reject the whole scrape.
	family := func(name, help, kind string, row func(lbl string, q entity.QueueStatsResponse)) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
		for _, m := range all {
			row(fmt.Sprintf(`org=%q,queue=%q`, m.org, m.stats.Queue), m.stats)
		}
	}

	family("ryuk_queue_ready_messages", "Messages ready to be consumed", "gauge",
		func(lbl string, q entity.QueueStatsResponse) {
			for _, p := range []string{"low", "medium", "high"} {
				fmt.Fprintf(&b, "ryuk_queue_ready_messages{%s,priority=%q} %d\n", lbl, p, q.ByPriority[p])
			}
		})
	family("ryuk_queue_inflight_messages", "Messages out with workers", "gauge",
		func(lbl string, q entity.QueueStatsResponse) {
			fmt.Fprintf(&b, "ryuk_queue_inflight_messages{%s} %d\n", lbl, q.InFlight)
		})
	family("ryuk_queue_oldest_message_age_seconds", "Age of the oldest available message", "gauge",
		func(lbl string, q entity.QueueStatsResponse) {
			fmt.Fprintf(&b, "ryuk_queue_oldest_message_age_seconds{%s} %f\n", lbl, q.OldestMessageAgeSeconds)
		})
	family("ryuk_queue_enqueued_total", "Messages submitted", "counter",
		func(lbl string, q entity.QueueStatsResponse) {
			fmt.Fprintf(&b, "ryuk_queue_enqueued_total{%s} %d\n", lbl, q.Enqueued)
		})
	family("ryuk_queue_acknowledged_total", "Messages acknowledged", "counter",
		func(lbl string, q entity.QueueStatsResponse) {
			fmt.Fprintf(&b, "ryuk_queue_acknowledged_total{%s} %d\n", lbl, q.Acked)
		})
	family("ryuk_queue_dead_lettered_total", "Messages moved to the dead-letter queue", "counter",
		func(lbl string, q entity.QueueStatsResponse) {
			fmt.Fprintf(&b, "ryuk_queue_dead_lettered_total{%s} %d\n", lbl, q.DeadLettered)
		})
	family("ryuk_queue_enqueue_rate", "Messages enqueued per second", "gauge",
		func(lbl string, q entity.QueueStatsResponse) {
			fmt.Fprintf(&b, "ryuk_queue_enqueue_rate{%s} %f\n", lbl, q.EnqueueRate)
		})
	family("ryuk_queue_ack_rate", "Messages acknowledged per second", "gauge",
		func(lbl string, q entity.QueueStatsResponse) {
			fmt.Fprintf(&b, "ryuk_queue_ack_rate{%s} %f\n", lbl, q.AckRate)
		})
	family("ryuk_queue_starvation_escapes_total", "Deliveries that used the reserved share", "counter",
		func(lbl string, q entity.QueueStatsResponse) {
			fmt.Fprintf(&b, "ryuk_queue_starvation_escapes_total{%s} %d\n", lbl, q.StarvationEscapes)
		})

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}

func uniqueOrgs(m map[string]string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, org := range m {
		out[org] = struct{}{}
	}
	return out
}
