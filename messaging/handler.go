package messaging

import (
	"io/ioutil"
	"net/http"
	"strconv"
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

// Broker returns the broker on the handler.
func (h *Handler) Broker() *Broker { return h.broker }

// ServeHTTP serves an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Delegate raft requests to its own handler.
	if strings.HasPrefix(r.URL.Path, "/raft") {
		h.raftHandler.ServeHTTP(w, r)
		return
	}

	// Route all InfluxDB broker requests.
	switch r.URL.Path {
	case "/messages":
		if r.Method == "GET" {
			h.stream(w, r)
		} else if r.Method == "POST" {
			h.publish(w, r)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

// connects the requestor as the replica's writer.
func (h *Handler) stream(w http.ResponseWriter, r *http.Request) {
	// Retrieve the replica name.
	name := r.URL.Query().Get("name")
	if name == "" {
		h.error(w, ErrReplicaNameRequired, http.StatusBadRequest)
		return
	}

	// Find the replica on the broker.
	replica := h.broker.Replica(name)
	if replica == nil {
		h.error(w, ErrReplicaNotFound, http.StatusNotFound)
		return
	}

	// Connect the response writer to the replica.
	// This will block until the replica is closed or a new writer connects.
	_, _ = replica.WriteTo(w)
}

// publishes a message to the broker.
func (h *Handler) publish(w http.ResponseWriter, r *http.Request) {
	m := &Message{}

	// Read the message type.
	if n, err := strconv.ParseUint(r.URL.Query().Get("type"), 10, 16); err != nil {
		h.error(w, ErrMessageTypeRequired, http.StatusBadRequest)
		return
	} else {
		m.Type = MessageType(n)
	}

	// Read the topic ID.
	if n, err := strconv.ParseUint(r.URL.Query().Get("topicID"), 10, 32); err != nil {
		h.error(w, ErrTopicRequired, http.StatusBadRequest)
		return
	} else {
		m.TopicID = uint64(n)
	}

	// Read the request body.
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}
	m.Data = data

	// Publish message to the broker.
	index, err := h.broker.Publish(m)
	if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}

	// Return index.
	w.Header().Set("X-Broker-Index", strconv.FormatUint(index, 10))
}

// error writes an error to the client and sets the status code.
func (h *Handler) error(w http.ResponseWriter, err error, code int) {
	s := err.Error()
	w.Header().Set("X-Broker-Error", s)
	http.Error(w, s, code)
}
