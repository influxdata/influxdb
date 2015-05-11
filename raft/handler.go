package raft

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
)

// Handler represents an HTTP endpoint for Raft to communicate over.
type Handler struct {
	Log interface {
		AddPeer(u url.URL) (id uint64, leaderID uint64, config *Config, err error)
		RemovePeer(id uint64) error
		Heartbeat(term, commitIndex, leaderID uint64) (currentIndex uint64, err error)
		WriteEntriesTo(w io.Writer, id, term, index uint64) error
		RequestVote(term, candidateID, lastLogIndex, lastLogTerm uint64) (peerTerm uint64, err error)
	}
}

// ServeHTTP handles all incoming HTTP requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
func (h *Handler) serveJoin(w http.ResponseWriter, r *http.Request) {
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
	id, leaderID, config, err := h.Log.AddPeer(*u)
	if err != nil {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Return member's id in the cluster.
	w.Header().Set("X-Raft-ID", strconv.FormatUint(id, 10))
	w.Header().Set("X-Raft-Leader-ID", strconv.FormatUint(leaderID, 10))
	w.WriteHeader(http.StatusOK)

	// Write config to the body.
	_ = json.NewEncoder(w).Encode(config)
}

// serveLeave removes a member from the cluster.
func (h *Handler) serveLeave(w http.ResponseWriter, r *http.Request) {
	// TODO(benbjohnson): Redirect to leader.

	// Parse arguments.
	id, err := strconv.ParseUint(r.FormValue("id"), 10, 64)
	if err != nil {
		w.Header().Set("X-Raft-Error", "invalid raft id")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Remove a peer from the log.
	if err := h.Log.RemovePeer(id); err != nil {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// serveHeartbeat serves a Raft heartbeat to the underlying log.
func (h *Handler) serveHeartbeat(w http.ResponseWriter, r *http.Request) {
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
	currentIndex, err := h.Log.Heartbeat(term, commitIndex, leaderID)

	// Return current index.
	w.Header().Set("X-Raft-Index", strconv.FormatUint(currentIndex, 10))

	// Write error, if applicable.
	if err != nil {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// serveStream provides a streaming log endpoint.
func (h *Handler) serveStream(w http.ResponseWriter, r *http.Request) {
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
	if err := h.Log.WriteEntriesTo(w, id, term, index); err != nil && err != io.EOF {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// serveRequestVote serves a vote request to the underlying log.
func (h *Handler) serveRequestVote(w http.ResponseWriter, r *http.Request) {
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

	// Request vote from log.
	peerTerm, err := h.Log.RequestVote(term, candidateID, lastLogIndex, lastLogTerm)

	// Write current term.
	w.Header().Set("X-Raft-Term", strconv.FormatUint(peerTerm, 10))

	// Write error, if applicable.
	if err != nil {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
