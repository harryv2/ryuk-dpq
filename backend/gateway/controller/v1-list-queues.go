package controller

import (
	"net/http"
)

func (h *Handlers) ListQueues(w http.ResponseWriter, r *http.Request, org string) {
	out, err := h.app.ListQueues(r.Context(), org)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": out})
}
