package controller

import (
	"net/http"
)

func (h *Handlers) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/queues", h.withOrg(h.CreateQueue))
	mux.HandleFunc("GET /v1/queues", h.withOrg(h.ListQueues))
	mux.HandleFunc("DELETE /v1/queues/{name}", h.withOrg(h.DeleteQueue))
	mux.HandleFunc("POST /v1/queues/{name}/messages", h.withOrg(h.Enqueue))
	mux.HandleFunc("POST /v1/queues/{name}/messages/dequeue", h.withOrg(h.Dequeue))
	mux.HandleFunc("POST /v1/queues/{name}/messages/ack", h.withOrg(h.Ack))
	mux.HandleFunc("POST /v1/queues/{name}/messages/nack", h.withOrg(h.Nack))
	mux.HandleFunc("GET /v1/queues/{name}/stats", h.withOrg(h.Stats))
	mux.HandleFunc("GET /v1/queues/{name}/timeseries", h.withOrg(h.Timeseries))
	mux.HandleFunc("GET /v1/metrics", h.withOrg(h.Metrics))
	mux.HandleFunc("GET /v1/cluster", h.Cluster)
	mux.HandleFunc("GET /metrics", h.PrometheusMetrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	if h.staticDir != "" {
		mux.Handle("/", staticFiles(h.staticDir))
	}
	return cors(mux)
}
