package legacy

import (
	"net/http"

	"github.com/influxdata/httprouter"
)

type PingHandler struct {
	*httprouter.Router
	InfluxDBVersion string
}

func NewPingHandler(version string) *PingHandler {
	h := &PingHandler{
		Router:          httprouter.New(),
		InfluxDBVersion: version,
	}

	h.HandlerFunc("GET", "/ping", h.pingHandler)
	h.HandlerFunc("HEAD", "/ping", h.pingHandler)
	return h
}

// handlePostLegacyWrite is the HTTP handler for the POST /write route.
func (h *PingHandler) pingHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("X-Influxdb-Build", "oss")
	w.Header().Add("X-Influxdb-Version", h.InfluxDBVersion)
	w.WriteHeader(http.StatusNoContent)
}
