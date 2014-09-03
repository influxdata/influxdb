package raft

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
)

// Initializes the default transport to support standard HTTP and TCP.
func init() {
	t := NewTransportMux()
	t.Handle("http", &HTTPTransport{})
	DefaultTransport = t
}

// Transport represents a handler for connecting the log to another node.
// It uses URLs to direct requests over different protocols.
type Transport interface {
	AppendEntries(*url.URL, *AppendEntriesRequest) (term int64, err error)
	RequestVote(*url.URL, *VoteRequest) (term int64, err error)
}

// DefaultTransport provides support for HTTP and TCP protocols.
var DefaultTransport Transport

// Transport is a transport multiplexer. It takes incoming requests and delegates
// them to the matching transport implementation based on their URL scheme.
type TransportMux struct {
	m map[string]Transport
}

// NewTransportMux returns a new instance of TransportMux.
func NewTransportMux() *TransportMux {
	return &TransportMux{m: make(map[string]Transport)}
}

// Handle registers a transport for a given scheme.
func (mux *TransportMux) Handle(scheme string, t Transport) {
	mux.m[scheme] = t
}

// AppendEntries sends a list of entries to a follower.
func (mux *TransportMux) AppendEntries(u *url.URL, r *AppendEntriesRequest) (int64, error) {
	if t, ok := mux.m[u.Scheme]; ok {
		return t.AppendEntries(u, r)
	}
	return 0, fmt.Errorf("transport scheme not supported: %s", u.Scheme)
}

// RequestVote requests a vote for a candidate in a given term.
func (mux *TransportMux) RequestVote(u *url.URL, r *VoteRequest) (int64, error) {
	if t, ok := mux.m[u.Scheme]; ok {
		return t.RequestVote(u, r)
	}
	return 0, fmt.Errorf("transport scheme not supported: %s", u.Scheme)
}

// HTTPTransport represents a transport for sending RPCs over the HTTP protocol.
type HTTPTransport struct{}

// AppendEntries sends a list of entries to a follower.
func (t *HTTPTransport) AppendEntries(uri *url.URL, r *AppendEntriesRequest) (int64, error) {
	// Copy URL and append path.
	u := uri
	u.Path = path.Join(u.Path, "append_entries")

	// Create log entry encoder.
	var buf bytes.Buffer
	enc := NewLogEntryEncoder(&buf)

	// Create an HTTP request.
	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return 0, err
	}

	// Set headers.
	req.Header = http.Header{
		"X-Raft-Term":         {strconv.FormatInt(r.Term, 10)},
		"X-Raft-LeaderID":     {strconv.FormatInt(r.LeaderID, 10)},
		"X-Raft-PrevLogIndex": {strconv.FormatInt(r.PrevLogIndex, 10)},
		"X-Raft-PrevLogTerm":  {strconv.FormatInt(r.PrevLogTerm, 10)},
	}

	// Write log entries.
	for _, e := range r.Entries {
		if err := enc.Encode(e); err != nil {
			return 0, err
		}
	}

	return 0, nil // TODO(benbjohnson)
}

// RequestVote requests a vote for a candidate in a given term.
func (t *HTTPTransport) RequestVote(u *url.URL, r *VoteRequest) (int64, error) {
	return 0, nil // TODO(benbjohnson)
}

// AppendEntriesRequest represents the arguments for an AppendEntries RPC.
type AppendEntriesRequest struct {
	Term         int64
	LeaderID     int64
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []*LogEntry
	LeaderCommit int64
}

// VoteRequest represents the arguments for a RequestVote RPC.
type VoteRequest struct {
	Term         int64
	CandidateID  int64
	LastLogIndex int64
	LastLogTerm  int64
}
