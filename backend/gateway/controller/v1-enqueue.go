package controller

import (
	"net/http"

	"github.com/harryv2/ryuk-dpq/backend/constants"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

func (h *Handlers) Enqueue(w http.ResponseWriter, r *http.Request, org string) {
	req, err := parseAndValidateEnqueueRequest(r, org)
	if err != nil {
		writeErr(w, err)
		return
	}
	resp, err := h.app.Enqueue(r.Context(), req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func parseAndValidateEnqueueRequest(r *http.Request, org string) (entity.EnqueueRequest, error) {
	req, err := decode[entity.EnqueueRequest](r)
	if err != nil {
		return req, err
	}
	req.Org, req.Queue = org, r.PathValue("name")

	if req.Body, err = decodePayload(req.Payload, req.PayloadEncoding); err != nil {
		return req, err
	}
	if len(req.Body) > constants.MaxPayloadBytes {
		return req, enterr.Invalid("payload must be %d KiB or less", constants.MaxPayloadBytes>>10)
	}
	if req.PriorityValue, err = parsePriority(req.Priority); err != nil {
		return req, err
	}
	if req.TTLFor, err = optDuration("ttl", req.TTL); err != nil {
		return req, err
	}
	if req.DeliverIn, err = optDuration("deliverAfter", req.DeliverAfter); err != nil {
		return req, err
	}
	return req, nil
}
