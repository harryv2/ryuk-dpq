package controller

import (
	"net/http"
)

func (h *Handlers) HandleGetRegistry(w http.ResponseWriter, r *http.Request) {
	out, err := h.app.GetRegistry(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
