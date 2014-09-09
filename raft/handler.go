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
		h.HandleHeartbeat(w, r)
	case "stream":
		h.HandleStream(w, r)
	case "vote":
		h.HandleRequestVote(w, r)
	default:
		http.NotFound(w, r)
	}
}

// HandleHeartbeat serves a Raft heartbeat to the underlying log.
func (h *HTTPHandler) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
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
	}
}

// HandleStream provides a streaming log endpoint.
func (h *HTTPHandler) HandleStream(w http.ResponseWriter, r *http.Request) {
	var err error
	var index, term uint64

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

	// Write to the response.
	if err := h.log.WriteTo(w, term, index); err != nil && err != io.EOF {
		w.Header().Set("X-Raft-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// HandleRequestVote serves a vote request to the underlying log.
func (h *HTTPHandler) HandleRequestVote(w http.ResponseWriter, r *http.Request) {
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
	}
}
