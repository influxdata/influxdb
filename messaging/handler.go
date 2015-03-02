package messaging

import (
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/influxdb/influxdb/raft"
)

// Handler represents an HTTP handler by the broker.
type Handler struct {
	raftHandler *raft.Handler
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
		h.raftHandler = &raft.Handler{Log: b.log}
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
			h.getMessages(w, r)
		} else if r.Method == "POST" {
			h.postMessages(w, r)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	case "/messaging/heartbeat":
		if r.Method == "POST" {
			h.postHeartbeat(w, r)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

// getMessages streams messages from a topic.
func (h *Handler) getMessages(w http.ResponseWriter, req *http.Request) {
	// Read the topic ID.
	topicID, err := strconv.ParseUint(req.URL.Query().Get("topicID"), 10, 64)
	if err != nil {
		h.error(w, ErrTopicRequired, http.StatusBadRequest)
		return
	}

	// Read the index to start from.
	index, err := strconv.ParseUint(req.URL.Query().Get("index"), 10, 64)
	if err != nil {
		h.error(w, ErrIndexRequired, http.StatusBadRequest)
		return
	}

	// Read the streaming flag.
	streaming := (req.URL.Query().Get("streaming") == "true")

	// Create a topic reader.
	r, err := h.broker.OpenTopicReader(topicID, index, streaming)
	if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}
	defer r.Close()

	// Ensure we close the topic reader if the connection is disconnected.
	if w, ok := w.(http.CloseNotifier); ok {
		go func() {
			select {
			case <-w.CloseNotify():
			}
		}()
	}

	// Write out all data from the topic reader.
	io.Copy(w, r)
}

// postMessages publishes a message to the broker.
func (h *Handler) postMessages(w http.ResponseWriter, r *http.Request) {
	// Read the message type.
	typ, err := strconv.ParseUint(r.URL.Query().Get("type"), 10, 16)
	if err != nil {
		h.error(w, ErrMessageTypeRequired, http.StatusBadRequest)
		return
	}

	// Read the topic ID.
	topicID, err := strconv.ParseUint(r.URL.Query().Get("topicID"), 10, 64)
	if err != nil {
		h.error(w, ErrTopicRequired, http.StatusBadRequest)
		return
	}

	// Read the request body.
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}

	// Publish message to the broker.
	index, err := h.broker.Publish(&Message{Type: MessageType(typ), TopicID: topicID, Data: data})
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

// postHearbeat receives a heartbeat from a client reporting the highest
// replicated index for a given topic.
func (h *Handler) postHeartbeat(w http.ResponseWriter, r *http.Request) {
	// Read the topic id.
	topicID, err := strconv.ParseUint(r.URL.Query().Get("topicID"), 10, 16)
	if err != nil {
		h.error(w, ErrTopicRequired, http.StatusBadRequest)
		return
	}

	// Read the index.
	index, err := strconv.ParseUint(r.URL.Query().Get("index"), 10, 16)
	if err != nil {
		h.error(w, ErrIndexRequired, http.StatusBadRequest)
		return
	}

	// Update the topic's highest replicated index.
	if err := h.broker.SetTopicMaxIndex(topicID, index); err == raft.ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}
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
