package controller

import (
	"net/http"
	"strings"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

type orgHandler func(w http.ResponseWriter, r *http.Request, org string)

// withOrg resolves the credential first, so everything after it is scoped to
// one org and there is no later check to forget.
func (h *Handlers) withOrg(next orgHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		token = strings.TrimSpace(token)
		org, ok := h.orgs[token]
		if !ok {
			writeErr(w, enterr.New(enterr.CodeInvalid, "unknown or missing credential"))
			return
		}
		next(w, r, org)
	}
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
