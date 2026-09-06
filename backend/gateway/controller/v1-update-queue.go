package controller

import (
	"net/http"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

func (h *Handlers) UpdateQueue(w http.ResponseWriter, r *http.Request, org string) {
	req, err := parseAndValidateUpdateQueueRequest(r, org)
	if err != nil {
		writeErr(w, err)
		return
	}
	resp, err := h.app.UpdateQueue(r.Context(), req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func parseAndValidateUpdateQueueRequest(r *http.Request, org string) (entity.UpdateQueueRequest, error) {
	req, err := decode[entity.UpdateQueueRequest](r)
	if err != nil {
		return req, err
	}
	req.Org = org
	req.Name = r.PathValue("name")

	if req.Distributed != nil || req.PlacementWidth != 0 {
		return req, enterr.Invalid(
			"placement is fixed at creation: it decides which slot a group lives in")
	}
	if req.DeadLetterQueue != "" {
		if err := queueName("deadLetterQueue", req.DeadLetterQueue); err != nil {
			return req, err
		}
		if req.DeadLetterQueue == req.Name {
			return req, enterr.Invalid("deadLetterQueue must not be the queue itself")
		}
	}
	req.ParseQueueSettings, err = settingsFrom(req.CreateQueueRequest)
	return req, err
}
