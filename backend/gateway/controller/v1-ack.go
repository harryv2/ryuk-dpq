package controller

import (
	"net/http"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

func (h *Handlers) Ack(w http.ResponseWriter, r *http.Request, org string) {
	req, err := decode[entity.AckRequest](r)
	if err != nil {
		writeErr(w, err)
		return
	}
	req.Org, req.Queue = org, r.PathValue("name")
	if req.Receipt == "" {
		writeErr(w, enterr.Invalid("receipt is required"))
		return
	}
	if err := h.app.Ack(r.Context(), req); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *Handlers) Nack(w http.ResponseWriter, r *http.Request, org string) {
	req, err := parseAndValidateNackRequest(r, org)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.app.Nack(r.Context(), req); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func parseAndValidateNackRequest(r *http.Request, org string) (entity.NackRequest, error) {
	req, err := decode[entity.NackRequest](r)
	if err != nil {
		return req, err
	}
	req.Org, req.Queue = org, r.PathValue("name")
	if req.Receipt == "" {
		return req, enterr.Invalid("receipt is required")
	}
	req.DelayFor, err = optDuration("delay", req.Delay)
	return req, err
}
