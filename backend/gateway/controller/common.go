package controller

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// staticFiles serves the built UI. A static export writes /cluster.html rather
// than /cluster/index.html, so a missing path is retried with .html before it
// falls back to the app shell.
func staticFiles(dir string) http.Handler {
	fs := http.FileServer(http.Dir(dir))
	exists := func(p string) bool {
		st, err := os.Stat(filepath.Join(dir, filepath.Clean(p)))
		return err == nil && !st.IsDir()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimSuffix(r.URL.Path, "/")
		if p == "" {
			fs.ServeHTTP(w, r)
			return
		}
		for _, candidate := range []string{p, p + ".html", p + "/index.html"} {
			if exists(candidate) {
				r2 := r.Clone(r.Context())
				r2.URL.Path = candidate
				fs.ServeHTTP(w, r2)
				return
			}
		}
		http.ServeFile(w, r, filepath.Join(dir, "index.html"))
	})
}
