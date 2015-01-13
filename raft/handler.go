package raft

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
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
	case "join":
		h.serveJoin(w, r)
	case "leave":
		h.serveLeave(w, r)
	case "heartbeat":
		h.serveHeartbeat(w, r)
	case "stream":
		h.serveStream(w, r)
	case "vote":
		h.serveRequestVote(w, r)
	case "ping":
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

// serveJoin serves a Raft membership addition to the underlying log.
func (h *HTTPHandler) serveJoin(w http.ResponseWriter, r *http.Request) {
	// TODO(benbjohnson): Redirect to leader.

	// Parse argument.
	if r.FormValue("url") == "" {
		w.Header().Set("X-Raft-Error", "url required")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Parse URL.
	u, err := url.Parse(r.FormValue("url"))
	if err != nil {
		w.Header().Set("X-Raft-Error", "invalid url")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Add peer to the log.
	id, config, err := h.log.AddPeer(u)
	if err != nil {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Return member's id in the cluster.
	w.Header().Set("X-Raft-ID", strconv.FormatUint(id, 10))
	w.WriteHeader(http.StatusOK)

	// Write config to the body.
	_ = json.NewEncoder(w).Encode(config)
}

// serveLeave removes a member from the cluster.
func (h *HTTPHandler) serveLeave(w http.ResponseWriter, r *http.Request) {
	// TODO(benbjohnson): Redirect to leader.

	// Parse arguments.
	id, err := strconv.ParseUint(r.FormValue("id"), 10, 64)
	if err != nil {
		w.Header().Set("X-Raft-ID", "invalid raft id")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Remove a peer from the log.
	if err := h.log.RemovePeer(id); err != nil {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// serveHeartbeat serves a Raft heartbeat to the underlying log.
func (h *HTTPHandler) serveHeartbeat(w http.ResponseWriter, r *http.Request) {
	var err error
	var term, commitIndex, leaderID uint64

	// Parse arguments.
	if term, err = strconv.ParseUint(r.FormValue("term"), 10, 64); err != nil {
		w.Header().Set("X-Raft-Error", "invalid term")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if commitIndex, err = strconv.ParseUint(r.FormValue("commitIndex"), 10, 64); err != nil {
		w.Header().Set("X-Raft-Error", "invalid commit index")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if leaderID, err = strconv.ParseUint(r.FormValue("leaderID"), 10, 64); err != nil {
		w.Header().Set("X-Raft-Error", "invalid leader id")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Execute heartbeat on the log.
	currentIndex, currentTerm, err := h.log.Heartbeat(term, commitIndex, leaderID)

	// Return current term and index.
	w.Header().Set("X-Raft-Index", strconv.FormatUint(currentIndex, 10))
	w.Header().Set("X-Raft-Term", strconv.FormatUint(currentTerm, 10))

	// Write error, if applicable.
	if err != nil {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// serveStream provides a streaming log endpoint.
func (h *HTTPHandler) serveStream(w http.ResponseWriter, r *http.Request) {
	var err error
	var id, index, term uint64

	// Parse client's id.
	if id, err = strconv.ParseUint(r.FormValue("id"), 10, 64); err != nil {
		w.Header().Set("X-Raft-Error", "invalid id")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Parse client's current term.
	if term, err = strconv.ParseUint(r.FormValue("term"), 10, 64); err != nil {
		w.Header().Set("X-Raft-Error", "invalid term")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Parse starting index.
	if s := r.FormValue("index"); s != "" {
		if index, err = strconv.ParseUint(r.FormValue("index"), 10, 64); err != nil {
			w.Header().Set("X-Raft-Error", "invalid index")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	// TODO(benbjohnson): Redirect to leader.

	// Write to the response.
	if err := h.log.WriteEntriesTo(w, id, term, index); err != nil && err != io.EOF {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// serveRequestVote serves a vote request to the underlying log.
func (h *HTTPHandler) serveRequestVote(w http.ResponseWriter, r *http.Request) {
	var err error
	var term, candidateID, lastLogIndex, lastLogTerm uint64

	// Parse arguments.
	if term, err = strconv.ParseUint(r.FormValue("term"), 10, 64); err != nil {
		w.Header().Set("X-Raft-Error", "invalid term")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if candidateID, err = strconv.ParseUint(r.FormValue("candidateID"), 10, 64); err != nil {
		w.Header().Set("X-Raft-Error", "invalid candidate id")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if lastLogIndex, err = strconv.ParseUint(r.FormValue("lastLogIndex"), 10, 64); err != nil {
		w.Header().Set("X-Raft-Error", "invalid last log index")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if lastLogTerm, err = strconv.ParseUint(r.FormValue("lastLogTerm"), 10, 64); err != nil {
		w.Header().Set("X-Raft-Error", "invalid last log term")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Execute heartbeat on the log.
	currentTerm, err := h.log.RequestVote(term, candidateID, lastLogIndex, lastLogTerm)

	// Return current term and index.
	w.Header().Set("X-Raft-Term", strconv.FormatUint(currentTerm, 10))

	// Write error, if applicable.
	if err != nil {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
