package controller

import (
	"net/http"
)

// Timeseries serves a queue's history from the monitoring system, so the charts
// survive a page reload instead of starting from nothing each time.
func (h *Handlers) Timeseries(w http.ResponseWriter, r *http.Request, org string) {
	out, err := h.app.GetTimeseriesMetrics(r.Context(), org, r.PathValue("name"), r.URL.Query().Get("window"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
