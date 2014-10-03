package broker

import (
	"net/http"
	"strings"

	"github.com/influxdb/influxdb/raft"
)

// HTTPHandler represents an HTTP handler by the broker.
type HTTPHandler struct {
	*raft.HTTPHandler
	broker *Broker
}

// NewHTTPHandler returns a new instance of HTTPHandler.
func NewHTTPHandler(b *Broker) *HTTPHandler {
	return &HTTPHandler{
		HTTPHandler: raft.NewHTTPHandler(b.log),
		broker:      b,
	}
}

// ServeHTTP serves an HTTP request.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Delegate raft requests to its own handler.
	if strings.HasPrefix(r.URL.Path, "/raft") {
		h.HTTPHandler.ServeHTTP(w, r)
		return
	}

	// Route all InfluxDB broker requests.
	switch r.URL.Path {
	case "/":
	}
}
