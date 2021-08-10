package http

import (
	"encoding/json"
	"net/http"

	"github.com/influxdata/influxdb/v2"
)

const prefixResources = "/api/v2/resources"

// NewResourceListHandler is the HTTP handler for the GET /api/v2/resources route.
func NewResourceListHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(influxdb.AllResourceTypes)
	})
}
