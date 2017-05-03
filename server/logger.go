package server

import (
	"net/http"
	"time"

	"github.com/influxdata/chronograf"
)

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
		next.ServeHTTP(w, r)
		later := time.Now()
		elapsed := later.Sub(now)

		logger.
			WithField("component", "server").
			WithField("remote_addr", r.RemoteAddr).
			WithField("response_time", elapsed.String()).
			Info("Success")
	}
	return http.HandlerFunc(fn)
}
