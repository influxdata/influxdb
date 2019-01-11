package http

import (
	"fmt"
	"net/http"
)

// HealthHandler returns the status of the process.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	msg := `{"name":"influxdb", "message":"ready for queries and writes", "status":"pass", "checks":[]}`
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, msg)
}
