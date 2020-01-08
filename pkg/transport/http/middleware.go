package http

import (
	"net/http"

	"github.com/influxdata/influxdb/kit/tracing"
)

// Middleware constructor.
type Middleware func(http.Handler) http.Handler

func SkipOptions(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			return
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func Trace(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		span, ctx := tracing.StartSpanFromContext(r.Context())
		defer span.Finish()
		next.ServeHTTP(w, r.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}
