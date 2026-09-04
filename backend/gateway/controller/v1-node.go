package controller

import (
	"net/http"
)

func (h *Handlers) HandleGetNodeDetails(w http.ResponseWriter, r *http.Request, org string) {
	out, err := h.app.GetNodeDetails(r.Context(), org, r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
