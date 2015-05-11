package messaging

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/influxdb/influxdb/raft"
)

// Handler represents an HTTP handler by the broker.
type Handler struct {
	Broker interface {
		URLs() []url.URL
		IsLeader() bool
		LeaderURL() url.URL
		TopicReader(topicID, index uint64, streaming bool) interface {
			io.ReadCloser
			io.Seeker
		}
		DataURLsForTopic(id, index uint64) []url.URL
		Publish(m *Message) (uint64, error)
		SetTopicMaxIndex(topicID, index uint64, u url.URL) error
		Diagnostics() interface{}
	}

	RaftHandler http.Handler
}

// ServeHTTP serves an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Delegate raft requests to its own handler.
	if strings.HasPrefix(r.URL.Path, "/raft") {
		h.RaftHandler.ServeHTTP(w, r)
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
	case "/messaging/diagnostics":
		if r.Method == "GET" {
			h.getDiagnostics(w, r)
		} else {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	case "/messaging/ping":
		h.servePing(w, r)
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
	r := h.Broker.TopicReader(topicID, index, streaming)
	defer r.Close()

	// Ensure we close the topic reader if the connection is disconnected.
	done := make(chan struct{}, 0)
	defer close(done)
	if w, ok := w.(http.CloseNotifier); ok {
		go func() {
			select {
			case <-w.CloseNotify():
				_ = r.Close()
			case <-done:
				return
			}
		}()
	}

	// Write out all data from the topic reader.
	// Automatically flush as reads come in.
	if _, err := CopyFlush(w, r); err == ErrTopicTruncated {
		// Broker unable to provide data for the requested index, provide another URL.
		urls := h.Broker.DataURLsForTopic(topicID, index)
		if urls == nil {
			h.error(w, ErrTopicNodesNotFound, http.StatusInternalServerError)
			return
		}

		// Send back a list of URLs where the topic data can be fetched.
		var redirects []string
		for _, u := range urls {
			redirects = append(redirects, u.String())
		}
		w.Header().Set("X-Broker-DataURLs", strings.Join(redirects, ","))
		http.Redirect(w, req, urls[0].String(), http.StatusTemporaryRedirect)
	} else if err != nil {
		log.Printf("message stream error: %s", err)
	}
}

// postMessages publishes a message to the broker.
func (h *Handler) postMessages(w http.ResponseWriter, r *http.Request) {
	// Read the message type.
	typ, err := strconv.ParseUint(r.URL.Query().Get("type"), 10, 64)
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
	// Exit if there is no message data provided.
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	} else if len(data) == 0 {
		h.error(w, ErrMessageDataRequired, http.StatusBadRequest)
		return
	}

	// Publish message to the broker.
	index, err := h.Broker.Publish(&Message{Type: MessageType(typ), TopicID: topicID, Data: data})
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
	topicID, err := strconv.ParseUint(r.URL.Query().Get("topicID"), 10, 64)
	if err != nil {
		h.error(w, ErrTopicRequired, http.StatusBadRequest)
		return
	}

	// Read the index.
	index, err := strconv.ParseUint(r.URL.Query().Get("index"), 10, 64)
	if err != nil {
		h.error(w, ErrIndexRequired, http.StatusBadRequest)
		return
	}

	u, err := url.Parse(r.URL.Query().Get("url"))
	if err != nil {
		h.error(w, ErrURLRequired, http.StatusBadRequest)
	}

	// Update the topic's highest replicated index.
	if err := h.Broker.SetTopicMaxIndex(topicID, index, *u); err == raft.ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err != nil {
		h.error(w, err, http.StatusInternalServerError)
		return
	}
}

// getDiagnostics returns Broker diagnostic information
func (h *Handler) getDiagnostics(w http.ResponseWriter, r *http.Request) {
	diagnostics := h.Broker.Diagnostics()
	if diagnostics == nil {
		http.Error(w, "unable to determine broker diagnostics", http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(diagnostics); err != nil {
		log.Printf("unable to write broker diagnostics: %s", err)
		return
	}
}

// servePing returns a status 200.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	// Redirect if not leader.
	if !h.Broker.IsLeader() {
		h.redirectToLeader(w, r)
		return
	}

	// Write out client configuration.
	var config ClientConfig
	config.URLs = h.Broker.URLs()
	if err := json.NewEncoder(w).Encode(&config); err != nil {
		log.Printf("unable to write client config: %s", err)
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
	if u := h.Broker.LeaderURL(); u.Host != "" {
		redirectURL := *r.URL
		redirectURL.Scheme = u.Scheme
		redirectURL.Host = u.Host
		http.Redirect(w, r, redirectURL.String(), http.StatusTemporaryRedirect)
		return
	}

	h.error(w, raft.ErrNotLeader, http.StatusInternalServerError)
}

// CopyFlush copies from src to dst until EOF or an error occurs.
// Each write is proceeded by a flush, if the writer implements http.Flusher.
//
// This implementation is copied from io.Copy().
func CopyFlush(dst io.Writer, src io.Reader) (written int64, err error) {
	buf := make([]byte, 32*1024)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}

			// Flush after write.
			if dst, ok := dst.(interface {
				Flush()
			}); ok {
				dst.Flush()
			}

			if ew != nil {
				err = ew
				break
			} else if nr != nw {
				err = io.ErrShortWrite
				break
			}
		} else if er == io.EOF {
			break
		} else if er != nil {
			err = er
			break
		}
	}
	return written, err
}
