package broker

import (
	"net/http"
	"strings"

	"github.com/influxdb/influxdb/raft"
)

// Handler represents an HTTP handler by the broker.
type Handler struct {
	raftHandler *raft.HTTPHandler
	broker      *Broker
}

// NewHandler returns a new instance of Handler.
func NewHandler(b *Broker) *Handler {
	return &Handler{
		raftHandler: raft.NewHTTPHandler(b.log),
		broker:      b,
	}
}

// ServeHTTP serves an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Delegate raft requests to its own handler.
	if strings.HasPrefix(r.URL.Path, "/raft") {
		h.raftHandler.ServeHTTP(w, r)
		return
	}

	// Route all InfluxDB broker requests.
	switch r.URL.Path {
	case "/stream":
		h.serveStream(w, r)
	}
}

// connects the requestor as the replica's writer.
func (h *Handler) serveStream(w http.ResponseWriter, r *http.Request) {
	// Retrieve the replica name.
	name := r.URL.Query().Get("name")
	if name == "" {
		w.Header().Set("X-Broker-Error", "replica name required")
		http.Error(w, "replica name required", http.StatusBadRequest)
		return
	}

	// Find the replica on the broker.
	replica := h.broker.Replica(name)
	if replica == nil {
		w.Header().Set("X-Broker-Error", ErrReplicaNotFound.Error())
		http.Error(w, ErrReplicaNotFound.Error(), http.StatusNotFound)
		return
	}

	// Connect the response writer to the replica.
	// This will block until the replica is closed or a new writer connects.
	_, _ = replica.WriteTo(w)
}
