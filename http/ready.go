package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/influxdata/influxdb/v2/toml"
)

// ReadyHandler is a default readiness handler. The default behaviour is always ready.
func ReadyHandler() http.Handler {
	up := time.Now()
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		var status = struct {
			Status string    `json:"status"`
			Start  time.Time `json:"started"`
			// TODO(jsteenb2): learn why and leave comment for this being a toml.Duration
			Up toml.Duration `json:"up"`
		}{
			Status: "ready",
			Start:  up,
			Up:     toml.Duration(time.Since(up)),
		}

		enc := json.NewEncoder(w)
		enc.SetIndent("", "    ")
		if err := enc.Encode(status); err != nil {
			fmt.Fprintf(w, "Error encoding status data: %v\n", err)
		}
	}
	return http.HandlerFunc(fn)
}
