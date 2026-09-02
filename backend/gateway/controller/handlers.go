package controller

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/gateway/logic"
)

type Handlers struct {
	app       logic.GatewayLogicInterface
	log       *slog.Logger
	orgs      map[string]string // bearer token -> org
	staticDir string
}

func New(app logic.GatewayLogicInterface, log *slog.Logger, orgs map[string]string, staticDir string) *Handlers {
	return &Handlers{app: app, log: log, orgs: orgs, staticDir: staticDir}
}

func decode[T any](r *http.Request) (T, error) {
	var v T
	if r.ContentLength == 0 {
		return v, nil
	}
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		return v, enterr.Invalid("malformed body: %s", err.Error())
	}
	return v, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeErr(w http.ResponseWriter, err error) {
	status := map[enterr.Code]int{
		enterr.CodeInvalid:   http.StatusBadRequest,
		enterr.CodeNotFound:  http.StatusNotFound,
		enterr.CodeConflict:  http.StatusConflict,
		enterr.CodeExhausted: http.StatusServiceUnavailable,
		enterr.CodeMoved:     http.StatusServiceUnavailable,
	}[enterr.CodeOf(err)]
	if status == 0 {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
