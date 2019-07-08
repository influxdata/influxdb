package http

import (
	"context"
	"net/http"
)

// Flusher flushes data from a store to reset; used for testing.
type Flusher interface {
	Flush(ctx context.Context)
}

// DebugFlush clears all services for testing.
func DebugFlush(ctx context.Context, next http.Handler, f Flusher) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/debug/flush" {
			f.Flush(ctx)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
