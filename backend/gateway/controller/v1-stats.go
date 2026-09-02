package controller

import (
	"net/http"
)

func (h *Handlers) Stats(w http.ResponseWriter, r *http.Request, org string) {
	s, err := h.app.Stats(r.Context(), org, r.PathValue("name"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s)
}

func (h *Handlers) Metrics(w http.ResponseWriter, r *http.Request, org string) {
	out, err := h.app.Metrics(r.Context(), org)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": out})
}
