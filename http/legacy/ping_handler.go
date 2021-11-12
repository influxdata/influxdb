package legacy

import (
	"net/http"

	"github.com/influxdata/httprouter"
)

type PingHandler struct {
	*httprouter.Router
}

func NewPingHandler() *PingHandler {
	h := &PingHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("GET", "/ping", h.pingHandler)
	h.HandlerFunc("HEAD", "/ping", h.pingHandler)
	return h
}

// handlePostLegacyWrite is the HTTP handler for the POST /write route.
func (h *PingHandler) pingHandler(w http.ResponseWriter, r *http.Request) {
	// X-Influxdb-Version and X-Influxdb-Build header are sets by buildHeader in http/handler.go
	w.WriteHeader(http.StatusNoContent)
}
