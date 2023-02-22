package http

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/influxdata/influxdb/v2"
)

// Flusher flushes data from a store to reset; used for testing.
type Flusher interface {
	Flush(ctx context.Context)
}

func Debug(ctx context.Context, next http.Handler, f Flusher, service influxdb.OnboardingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/debug/flush" {
			// DebugFlush clears all services for testing.
			f.Flush(ctx)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == "/debug/provision" {
			data := &influxdb.OnboardingRequest{
				User:     "dev_user",
				Password: "password",
				Org:      "InfluxData",
				Bucket:   "project",
			}
			res, err := service.OnboardInitialUser(ctx, data)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}
			body, err := json.Marshal(res)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			return
		}
		next.ServeHTTP(w, r)
	}
}
