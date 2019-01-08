package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/influxdata/influxdb/toml"
)

var up = time.Now()

// ReadyHandler is a default readiness handler. The default behaviour is always ready.
func ReadyHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	var status = struct {
		Status string        `json:"status"`
		Start  time.Time     `json:"started"`
		Up     toml.Duration `json:"up"`
	}{
		Status: "ready",
		Start:  up,
		Up:     toml.Duration(time.Since(up)),
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "    ")
	err := enc.Encode(status)
	if err != nil {
		fmt.Fprintf(w, "Error encoding status data: %v\n", err)
	}
}
