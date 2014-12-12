package main

import (
	"net/http"
	"strings"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/messaging"
)

// Handler represents an HTTP handler for InfluxDB node. Depending on its role, it
// will serve many different endpoints.
type Handler struct {
	brokerHandler *messaging.Handler
	serverHandler *influxdb.Handler
}

// NewHandler returns a new instance of Handler.
func NewHandler(b *messaging.Handler, s *influxdb.Handler) *Handler {
	return &Handler{
		brokerHandler: b,
		serverHandler: s,
	}
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/raft") || strings.HasPrefix(r.URL.Path, "/messages") {
		h.brokerHandler.ServeHTTP(w, r)
		return
	}

	h.serverHandler.ServeHTTP(w, r)
}
