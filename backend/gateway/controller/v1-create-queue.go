package controller

import (
	"net/http"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/constants"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

func (h *Handlers) CreateQueue(w http.ResponseWriter, r *http.Request, org string) {
	req, err := parseAndValidateCreateQueueRequest(r, org)
	if err != nil {
		writeErr(w, err)
		return
	}
	resp, err := h.app.CreateQueue(r.Context(), req)
	if err != nil {
		writeErr(w, err)
		return
	}
	status := http.StatusOK
	if resp.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, resp)
}

func parseAndValidateCreateQueueRequest(r *http.Request, org string) (entity.CreateQueueRequest, error) {
	req, err := decode[entity.CreateQueueRequest](r)
	if err != nil {
		return req, err
	}
	req.Org = org

	if err := queueName("name", req.Name); err != nil {
		return req, err
	}
	if req.DeadLetterQueue != "" {
		if err := queueName("deadLetterQueue", req.DeadLetterQueue); err != nil {
			return req, err
		}
		if req.DeadLetterQueue == req.Name {
			return req, enterr.Invalid("deadLetterQueue must not be the queue itself")
		}
	}
	if req.StarvationReserve < 0 || req.StarvationReserve >= 1 {
		return req, enterr.Invalid("starvationReserve must be between 0 and 1")
	}
	if req.MaxDepth < 0 {
		return req, enterr.Invalid("maxDepth must not be negative")
	}
	if req.PlacementWidth != 0 && !req.Distributed {
		return req, enterr.Invalid("placementWidth only applies to a distributed queue")
	}
	if req.Distributed {
		if req.PlacementWidth == 0 {
			req.PlacementWidth = constants.DefaultPlacementWidth
		}
		if req.PlacementWidth < constants.MinPlacementWidth || req.PlacementWidth > constants.MaxPlacementWidth {
			return req, enterr.Invalid("placementWidth must be between %d and %d",
				constants.MinPlacementWidth, constants.MaxPlacementWidth)
		}
	}

	if req.ParseQueueSettings, err = settingsFrom(req); err != nil {
		return req, err
	}
	return req, nil
}

// settingsFrom parses and defaults the settings both creating and updating a
// queue accept, so the two cannot drift apart.
func settingsFrom(req entity.CreateQueueRequest) (entity.QueueSettings, error) {
	var err error
	s := entity.QueueSettings{
		StarvationReserve: req.StarvationReserve,
		MaxDepth:          req.MaxDepth,
		DeadLetterQueue:   req.DeadLetterQueue,
		PlacementWidth:    req.PlacementWidth,
	}
	if s.VisibilityTimeout, err = optDuration("visibilityTimeout", req.VisibilityTimeout); err != nil {
		return s, err
	}
	if s.DefaultTTL, err = optDuration("defaultTtl", req.DefaultTTL); err != nil {
		return s, err
	}
	if s.StarvationThreshold, err = optDuration("starvationThreshold", req.StarvationThreshold); err != nil {
		return s, err
	}

	if s.VisibilityTimeout == 0 {
		s.VisibilityTimeout = 30 * time.Second
	}
	if req.MaxRetries != nil {
		s.MaxRetries = *req.MaxRetries
	} else {
		s.MaxRetries = 3
	}
	if s.StarvationReserve == 0 {
		s.StarvationReserve = 0.2
	}
	if s.StarvationThreshold == 0 && s.DefaultTTL > 0 {
		s.StarvationThreshold = s.DefaultTTL / 4
	}

	// A threshold at or above the expiry turns the queue into something that
	// silently deletes low-priority work.
	if s.DefaultTTL > 0 && s.StarvationThreshold >= s.DefaultTTL {
		return s, enterr.Invalid(
			"starvationThreshold must be below defaultTtl, or messages can expire while waiting")
	}

	return s, nil
}
