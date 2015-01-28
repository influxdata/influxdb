package main

import (
	"net/http"
	"strings"
)

// Handler represents an HTTP handler for InfluxDB node.
// Depending on its role, it will serve many different endpoints.
type Handler struct {
	brokerHandler http.Handler
	serverHandler http.Handler
}

// NewHandler returns a new instance of Handler.
func NewHandler(bh, sh http.Handler) *Handler {
	return &Handler{
		brokerHandler: bh,
		serverHandler: sh,
	}
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Route raft and messaging paths to the broker.
	if strings.HasPrefix(r.URL.Path, "/raft") || strings.HasPrefix(r.URL.Path, "/messaging") {
		if h.brokerHandler == nil {
			http.NotFound(w, r)
			return
		}

		h.brokerHandler.ServeHTTP(w, r)
		return
	}

	// Route all other paths to the server.
	if h.serverHandler == nil {
		http.NotFound(w, r)
		return
	}
	h.serverHandler.ServeHTTP(w, r)
}
