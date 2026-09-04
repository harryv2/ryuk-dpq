package controller

import (
	"net/http"
)

func (h *Handlers) HandleGetClusterDetails(w http.ResponseWriter, r *http.Request) {
	out, err := h.app.GetClusterDetails(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
