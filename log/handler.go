package log

import (
	"net/http"
	"strings"

	"github.com/influxdb/influxdb/raft"
)

// HTTPHandler represents an HTTP handler by the distributed log.
type HTTPHandler struct {
	*raft.HTTPHandler
	log *Log
}

// NewHTTPHandler returns a new instance of HTTPHandler.
func NewHTTPHandler(l *Log) *HTTPHandler {
	return &HTTPHandler{
		HTTPHandler: raft.NewHTTPHandler(l.log),
		log:         l,
	}
}

// ServeHTTP serves an HTTP request.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Delegate raft requests to its own handler.
	if strings.HasPrefix(r.URL.Path, "/raft") {
		h.HTTPHandler.ServeHTTP(w, r)
		return
	}

	// Route all InfluxDB distributed log requests.
	switch r.URL.Path {
	case "/":
	}
}
