package http

import (
	"fmt"
	"net/http"

	platform "github.com/influxdata/influxdb/v2"
)

// HealthHandler returns the status of the process.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	msg := fmt.Sprintf(`{"name":"influxdb", "message":"ready for queries and writes", "status":"pass", "checks":[], "version": %q, "commit": %q}`, platform.GetBuildInfo().Version, platform.GetBuildInfo().Commit)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, msg)
}
