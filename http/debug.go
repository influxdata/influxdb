package http

import "net/http"

// Flusher flushes
type Flusher interface {
	Flush()
}

// DebugFlush clears all services for testing.
func DebugFlush(next http.Handler, f Flusher) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/debug/flush" {
			f.Flush()
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
