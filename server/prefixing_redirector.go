package server

import (
	"net/http"
)

type flushingResponseWriter struct {
	http.ResponseWriter
}

func (f *flushingResponseWriter) WriteHeader(status int) {
	f.ResponseWriter.WriteHeader(status)
}

// Flush is here because the underlying HTTP chunked transfer response writer
// to implement http.Flusher.  Without it data is silently buffered.  This
// was discovered when proxying kapacitor chunked logs.
func (f *flushingResponseWriter) Flush() {
	if flusher, ok := f.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// FlushingHandler may not actually do anything, but it was ostensibly
// implemented to flush response writers that can be flushed for the
// purposes in the comment above.
func FlushingHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		iw := &flushingResponseWriter{
			ResponseWriter: w,
		}
		next.ServeHTTP(iw, r)
	})
}
