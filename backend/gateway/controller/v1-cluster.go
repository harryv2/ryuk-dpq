package controller

import (
	"net/http"
)

func (h *Handlers) Cluster(w http.ResponseWriter, r *http.Request) {
	out, err := h.app.Cluster(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
