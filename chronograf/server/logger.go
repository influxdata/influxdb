package server

import (
	"net/http"
	"time"

	"github.com/influxdata/platform/chronograf"
)

// statusWriterFlusher captures the status header of an http.ResponseWriter
// and is a flusher
type statusWriter struct {
	http.ResponseWriter
	Flusher http.Flusher
	status  int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Status() int { return w.status }

// Flush is here because the underlying HTTP chunked transfer response writer
// to implement http.Flusher.  Without it data is silently buffered.  This
// was discovered when proxying kapacitor chunked logs.
func (w *statusWriter) Flush() {
	if w.Flusher != nil {
		w.Flusher.Flush()
	}
}

// Logger is middleware that logs the request
func Logger(logger chronograf.Logger, next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		logger.WithField("component", "server").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL).
			Debug("Request")

		sw := &statusWriter{
			ResponseWriter: w,
		}
		if f, ok := w.(http.Flusher); ok {
			sw.Flusher = f
		}
		next.ServeHTTP(sw, r)
		later := time.Now()
		elapsed := later.Sub(now)

		logger.
			WithField("component", "server").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("response_time", elapsed.String()).
			WithField("status", sw.Status()).
			Info("Response: ", http.StatusText(sw.Status()))
	}
	return http.HandlerFunc(fn)
}
