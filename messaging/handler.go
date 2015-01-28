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
	h := &Handler{}
	h.SetBroker(b)
	return h
}

// Broker returns the broker on the handler.
func (h *Handler) Broker() *Broker { return h.broker }

// SetBroker sets the broker on the handler.
func (h *Handler) SetBroker(b *Broker) {
	h.broker = b

	if b != nil {
		h.raftHandler = raft.NewHTTPHandler(b.log)
	} else {
		h.raftHandler = nil
	}
}

// ServeHTTP serves an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// h.broker.Logger.Printf("%s %s", r.Method, r.URL.String())

	// Delegate raft requests to its own handler.
	if strings.HasPrefix(r.URL.Path, "/raft") {
		h.raftHandler.ServeHTTP(w, r)
		return
	}

	// Route all InfluxDB broker requests.
	switch r.URL.Path {
	case "/messaging/messages":
		if r.Method == "GET" {
			h.stream(w, r)
		} else if r.Method == "POST" {
			h.publish(w, r)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	case "/messaging/replicas":
		if r.Method == "POST" {
			h.createReplica(w, r)
		} else if r.Method == "DELETE" {
			h.deleteReplica(w, r)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	case "/messaging/subscriptions":
		if r.Method == "POST" {
			h.subscribe(w, r)
		} else if r.Method == "DELETE" {
			h.unsubscribe(w, r)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

// connects the requestor as the replica's writer.
func (h *Handler) stream(w http.ResponseWriter, r *http.Request) {
	// Read the replica ID.
	var replicaID uint64
	if n, err := strconv.ParseUint(r.URL.Query().Get("replicaID"), 10, 64); err != nil {
		h.error(w, ErrReplicaIDRequired, http.StatusBadRequest)
		return
	} else {
		replicaID = uint64(n)
	}

	// Find the replica on the broker.
	replica := h.broker.Replica(replicaID)
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
	if n, err := strconv.ParseUint(r.URL.Query().Get("topicID"), 10, 64); err != nil {
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
	if err == raft.ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}

	// Return index.
	w.Header().Set("X-Broker-Index", strconv.FormatUint(index, 10))
}

// createReplica creates a new replica with a given ID.
func (h *Handler) createReplica(w http.ResponseWriter, r *http.Request) {
	// Read the replica ID.
	var replicaID uint64
	if n, err := strconv.ParseUint(r.URL.Query().Get("id"), 10, 64); err != nil {
		h.error(w, ErrReplicaIDRequired, http.StatusBadRequest)
		return
	} else {
		replicaID = uint64(n)
	}

	// Create a new replica on the broker.
	if err := h.broker.CreateReplica(replicaID); err == raft.ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err == ErrReplicaExists {
		h.error(w, err, http.StatusConflict)
		return
	} else if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// deleteReplica deletes an existing replica by ID.
func (h *Handler) deleteReplica(w http.ResponseWriter, r *http.Request) {
	// Read the replica ID.
	var replicaID uint64
	if n, err := strconv.ParseUint(r.URL.Query().Get("id"), 10, 64); err != nil {
		h.error(w, ErrReplicaIDRequired, http.StatusBadRequest)
		return
	} else {
		replicaID = uint64(n)
	}

	// Delete the replica on the broker.
	if err := h.broker.DeleteReplica(replicaID); err == raft.ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// subscribe creates a new subscription for a replica on a topic.
func (h *Handler) subscribe(w http.ResponseWriter, r *http.Request) {
	// Read the replica ID.
	var replicaID uint64
	if n, err := strconv.ParseUint(r.URL.Query().Get("replicaID"), 10, 64); err != nil {
		h.error(w, ErrReplicaIDRequired, http.StatusBadRequest)
		return
	} else {
		replicaID = uint64(n)
	}

	// Read the topic ID.
	var topicID uint64
	if n, err := strconv.ParseUint(r.URL.Query().Get("topicID"), 10, 64); err != nil {
		h.error(w, ErrTopicRequired, http.StatusBadRequest)
		return
	} else {
		topicID = uint64(n)
	}

	// Subscribe a replica to a topic.
	if err := h.broker.Subscribe(replicaID, topicID); err == raft.ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err == ErrReplicaNotFound {
		h.error(w, err, http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// unsubscribe removes a subscription from a replica for a topic.
func (h *Handler) unsubscribe(w http.ResponseWriter, r *http.Request) {
	// Read the replica ID.
	var replicaID uint64
	if n, err := strconv.ParseUint(r.URL.Query().Get("replicaID"), 10, 64); err != nil {
		h.error(w, ErrReplicaIDRequired, http.StatusBadRequest)
		return
	} else {
		replicaID = uint64(n)
	}

	// Read the topic ID.
	var topicID uint64
	if n, err := strconv.ParseUint(r.URL.Query().Get("topicID"), 10, 64); err != nil {
		h.error(w, ErrTopicRequired, http.StatusBadRequest)
		return
	} else {
		topicID = uint64(n)
	}

	// Unsubscribe the replica from the topic.
	if err := h.broker.Unsubscribe(replicaID, topicID); err == raft.ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err == ErrReplicaNotFound {
		h.error(w, err, http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// error writes an error to the client and sets the status code.
func (h *Handler) error(w http.ResponseWriter, err error, code int) {
	s := err.Error()
	w.Header().Set("X-Broker-Error", s)
	http.Error(w, s, code)
}

// redirects to the current known leader.
// If no leader is found then returns a 500.
func (h *Handler) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	if u := h.broker.LeaderURL(); u != nil {
		redirectURL := *r.URL
		redirectURL.Scheme = u.Scheme
		redirectURL.Host = u.Host
		http.Redirect(w, r, redirectURL.String(), http.StatusTemporaryRedirect)
		return
	}

	h.error(w, raft.ErrNotLeader, http.StatusInternalServerError)
}
