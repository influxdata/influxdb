package server

import (
	"net/http"
	"time"

	"github.com/influxdata/chronograf"
)

type logResponseWriter struct {
	http.ResponseWriter

	responseCode int
}

func (l *logResponseWriter) WriteHeader(status int) {
	l.responseCode = status
	l.ResponseWriter.WriteHeader(status)
}

// Logger is middleware that logs the request
func Logger(logger chronograf.Logger, next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		logger.
			WithField("component", "server").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL).
			Info("Request")

		lrr := &logResponseWriter{w, 0}
		next.ServeHTTP(lrr, r)
		later := time.Now()
		elapsed := later.Sub(now)

		logger.
			WithField("component", "server").
			WithField("remote_addr", r.RemoteAddr).
			WithField("response_time", elapsed.String()).
			WithField("code", lrr.responseCode).
			Info("Response: ", http.StatusText(lrr.responseCode))
	}
	return http.HandlerFunc(fn)
}
