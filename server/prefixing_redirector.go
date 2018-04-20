package server

import (
	"net/http"
)

type interceptingResponseWriter struct {
	http.ResponseWriter
	Flusher http.Flusher
}

func (i *interceptingResponseWriter) WriteHeader(status int) {
	i.ResponseWriter.WriteHeader(status)
}

// Flush is here because the underlying HTTP chunked transfer response writer
// to implement http.Flusher.  Without it data is silently buffered.  This
// was discovered when proxying kapacitor chunked logs.
func (i *interceptingResponseWriter) Flush() {
	if i.Flusher != nil {
		i.Flusher.Flush()
	}
}

// PrefixedRedirect alters the Location header of downstream http.Handlers
// to include a specified prefix
func PrefixedRedirect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		iw := &interceptingResponseWriter{
			ResponseWriter: w,
		}
		if flusher, ok := w.(http.Flusher); ok {
			iw.Flusher = flusher
		}
		next.ServeHTTP(iw, r)
	})
}
