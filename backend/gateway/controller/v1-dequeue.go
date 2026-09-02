package controller

import (
	"net/http"

	"github.com/harryv2/ryuk-dpq/backend/constants"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

func (h *Handlers) Dequeue(w http.ResponseWriter, r *http.Request, org string) {
	req, err := parseAndValidateDequeueRequest(r, org)
	if err != nil {
		writeErr(w, err)
		return
	}
	resp, err := h.app.Dequeue(r.Context(), req)
	if err != nil {
		writeErr(w, err)
		return
	}
	if len(resp.Messages) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func parseAndValidateDequeueRequest(r *http.Request, org string) (entity.DequeueRequest, error) {
	req, err := decode[entity.DequeueRequest](r)
	if err != nil {
		return req, err
	}
	req.Org, req.Queue = org, r.PathValue("name")

	if req.MaxMessages < 0 {
		return req, enterr.Invalid("maxMessages must not be negative")
	}
	if req.MaxMessages == 0 {
		req.MaxMessages = 1
	}
	if req.MaxMessages > constants.MaxDequeueBatch {
		req.MaxMessages = constants.MaxDequeueBatch
	}
	if req.WaitTime, err = optDuration("waitTime", req.WaitTimeRaw); err != nil {
		return req, err
	}
	// Holding a request longer ties up a connection for no gain, and most
	// proxies cut it off anyway.
	if req.WaitTime > constants.MaxWaitTime {
		req.WaitTime = constants.MaxWaitTime
	}
	return req, nil
}
