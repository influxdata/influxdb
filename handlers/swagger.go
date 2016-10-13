package handlers

import (
	"net/http"
	"path"
)

func Spec(basePath string, b []byte, next http.Handler) http.Handler {
	if basePath == "" {
		basePath = "/"
	}
	pth := path.Join(basePath, "swagger.json")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pth {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}

		if next == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}
