package controller

import (
	"net/http"
)

func (h *Handlers) DeleteQueue(w http.ResponseWriter, r *http.Request, org string) {
	if err := h.app.DeleteQueue(r.Context(), org, r.PathValue("name")); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}
