package controller

import (
	"fmt"
	"net/http"
	"strings"
)

// PrometheusMetrics renders the same numbers the JSON endpoints serve. Written
// by hand so the service needs no metrics library.
func (h *Handlers) PrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder
	write := func(name, help, kind string, rows func()) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
		rows()
	}

	for org := range uniqueOrgs(h.orgs) {
		queues, err := h.app.Metrics(r.Context(), org)
		if err != nil {
			continue
		}
		for _, q := range queues {
			lbl := fmt.Sprintf(`org=%q,queue=%q`, org, q.Queue)
			write("ryuk_queue_ready_messages", "Messages ready to be consumed", "gauge", func() {
				for _, p := range []string{"low", "medium", "high"} {
					fmt.Fprintf(&b, "ryuk_queue_ready_messages{%s,priority=%q} %d\n", lbl, p, q.ByPriority[p])
				}
			})
			write("ryuk_queue_inflight_messages", "Messages out with workers", "gauge", func() {
				fmt.Fprintf(&b, "ryuk_queue_inflight_messages{%s} %d\n", lbl, q.InFlight)
			})
			write("ryuk_queue_oldest_message_age_seconds", "Age of the oldest available message", "gauge", func() {
				fmt.Fprintf(&b, "ryuk_queue_oldest_message_age_seconds{%s} %f\n", lbl, q.OldestMessageAgeSeconds)
			})
			write("ryuk_queue_enqueued_total", "Messages submitted", "counter", func() {
				fmt.Fprintf(&b, "ryuk_queue_enqueued_total{%s} %d\n", lbl, q.Enqueued)
			})
			write("ryuk_queue_acknowledged_total", "Messages acknowledged", "counter", func() {
				fmt.Fprintf(&b, "ryuk_queue_acknowledged_total{%s} %d\n", lbl, q.Acked)
			})
			write("ryuk_queue_dead_lettered_total", "Messages moved to the dead-letter queue", "counter", func() {
				fmt.Fprintf(&b, "ryuk_queue_dead_lettered_total{%s} %d\n", lbl, q.DeadLettered)
			})
			write("ryuk_queue_starvation_escapes_total", "Deliveries that used the reserved share", "counter", func() {
				fmt.Fprintf(&b, "ryuk_queue_starvation_escapes_total{%s} %d\n", lbl, q.StarvationEscapes)
			})
		}
	}

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
