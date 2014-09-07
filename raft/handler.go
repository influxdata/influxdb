package raft

import (
	"io"
	"net/http"
	"path"
	"strconv"
)

// HTTPHandler represents an HTTP endpoint for Raft to communicate over.
type HTTPHandler struct {
	log *Log
}

// NewHTTPHandler returns a new instance of HTTPHandler associated with a log.
func NewHTTPHandler(log *Log) *HTTPHandler {
	return &HTTPHandler{log: log}
}

// ServeHTTP handles all incoming HTTP requests.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch path.Base(r.URL.Path) {
	case "heartbeat":
		h.heartbeat(w, r)
	case "stream":
		h.stream(w, r)
	case "vote":
		h.vote(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *HTTPHandler) heartbeat(w http.ResponseWriter, r *http.Request) {
	// TODO(benbjohnson): Parse arguments.
	// TODO(benbjohnson): Execute heartbeat on the log.
	// TODO(benbjohnson): Return current term and index.
}

// stream provides a streaming log endpoint.
func (h *HTTPHandler) stream(w http.ResponseWriter, r *http.Request) {
	// Parse arguments.
	term, err := strconv.ParseUint(r.FormValue("term"), 10, 64)
	if err != nil {
		http.Error(w, "invalid term", http.StatusBadRequest)
		return
	}

	index, err := strconv.ParseUint(r.FormValue("index"), 10, 64)
	if err != nil {
		http.Error(w, "invalid index", http.StatusBadRequest)
		return
	}

	// Write to the response.
	if err := h.log.WriteTo(w, term, index); err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *HTTPHandler) vote(w http.ResponseWriter, r *http.Request) {
	// TODO(benbjohnson): Parse arguments.
	// TODO(benbjohnson): Request vote.
	// TODO(benbjohnson): Return vote status.
}
