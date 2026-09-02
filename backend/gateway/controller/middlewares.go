package controller

import (
	"net/http"
	"strings"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

type orgHandler func(w http.ResponseWriter, r *http.Request, org string)

// withOrg resolves the credential first, so everything after it is scoped to
// one org and there is no later check to forget.
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

// staticFiles serves the built UI. A static export writes /cluster.html rather
// than /cluster/index.html, so a missing path is retried with .html before it
// falls back to the app shell.
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
