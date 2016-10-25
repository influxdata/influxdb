package server

import (
	"net/http"

	"github.com/influxdata/chronograf"
)

func Logger(logger chronograf.Logger, next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		logger.
			WithField("component", "server").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL).
			Info("Request")
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}
