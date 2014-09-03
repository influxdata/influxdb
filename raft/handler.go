package raft

import (
	"net/http"
	"path"
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
	// TODO(benbjohnson): Redirect request to appropriate handler.
	switch path.Base(r.URL.Path) {
	case "heartbeat":
		h.serveAppendEntries(w, r)
	case "entries":
		h.serveAppendEntries(w, r)
	case "vote":
		h.serveRequestVote(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *HTTPHandler) serveHeartbeat(w http.ResponseWriter, r *http.Request) {
	// TODO(benbjohnson)
}

func (h *HTTPHandler) serveAppendEntries(w http.ResponseWriter, r *http.Request) {
	// TODO(benbjohnson)
}

func (h *HTTPHandler) serveRequestVote(w http.ResponseWriter, r *http.Request) {
	// TODO(benbjohnson)
}
