package raft

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// Parts from this transporter were heavily influenced by Peter Bougon's
// raft implementation: https://github.com/peterbourgon/raft

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// An HTTPTransporter is a default transport layer used to communicate between
// multiple servers.
type HTTPTransporter struct {
	DisableKeepAlives bool
	prefix            string
	appendEntriesPath string
	requestVotePath   string
}

type HTTPMuxer interface {
	HandleFunc(string, func(http.ResponseWriter, *http.Request))
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new HTTP transporter with the given path prefix.
func NewHTTPTransporter(prefix string) *HTTPTransporter {
	return &HTTPTransporter{
		prefix:            prefix,
		appendEntriesPath: fmt.Sprintf("%s%s", prefix, "/appendEntries"),
		requestVotePath:   fmt.Sprintf("%s%s", prefix, "/requestVote"),
	}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// Retrieves the path prefix used by the transporter.
func (t *HTTPTransporter) Prefix() string {
	return t.prefix
}

// Retrieves the AppendEntries path.
func (t *HTTPTransporter) AppendEntriesPath() string {
	return t.appendEntriesPath
}

// Retrieves the RequestVote path.
func (t *HTTPTransporter) RequestVotePath() string {
	return t.requestVotePath
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Installation
//--------------------------------------

// Applies Raft routes to an HTTP router for a given server.
func (t *HTTPTransporter) Install(server *Server, mux HTTPMuxer) {
	mux.HandleFunc(t.AppendEntriesPath(), t.appendEntriesHandler(server))
	mux.HandleFunc(t.RequestVotePath(), t.requestVoteHandler(server))
}

//--------------------------------------
// Outgoing
//--------------------------------------

// Sends an AppendEntries RPC to a peer.
func (t *HTTPTransporter) SendAppendEntriesRequest(server *Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
	var b bytes.Buffer
	if _, err := req.encode(&b); err != nil {
		return nil
	}

	url := fmt.Sprintf("http://%s%s", peer.Name(), t.AppendEntriesPath())
	traceln(server.Name(), "POST", url)

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: t.DisableKeepAlives}}
	httpResp, err := client.Post(url, "application/protobuf", &b)
	if httpResp == nil || err != nil {
		return nil
	}
	defer httpResp.Body.Close()

	resp := &AppendEntriesResponse{}
	if _, err = resp.decode(httpResp.Body); err != nil && err != io.EOF {
		return nil
	}

	return resp
}

// Sends a RequestVote RPC to a peer.
func (t *HTTPTransporter) SendVoteRequest(server *Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
	var b bytes.Buffer
	if _, err := req.encode(&b); err != nil {
		return nil
	}

	url := fmt.Sprintf("http://%s%s", peer.Name(), t.RequestVotePath())
	traceln(server.Name(), "POST", url)

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: t.DisableKeepAlives}}
	httpResp, err := client.Post(url, "application/protobuf", &b)
	if httpResp == nil || err != nil {
		return nil
	}
	defer httpResp.Body.Close()

	resp := &RequestVoteResponse{}
	if _, err = resp.decode(httpResp.Body); err != nil && err != io.EOF {
		return nil
	}

	return resp
}

// Sends a SnapshotRequest RPC to a peer.
func (t *HTTPTransporter) SendSnapshotRequest(server *Server, peer *Peer, req *SnapshotRequest) *SnapshotResponse {
	// TODO
	return nil
}

//--------------------------------------
// Incoming
//--------------------------------------

// Handles incoming AppendEntries requests.
func (t *HTTPTransporter) appendEntriesHandler(server *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		traceln(server.Name(), "RECV /appendEntries")

		defer r.Body.Close()
		req := &AppendEntriesRequest{}
		if _, err := req.decode(r.Body); err != nil {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		resp := server.AppendEntries(req)
		if _, err := resp.encode(w); err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
	}
}

// Handles incoming RequestVote requests.
func (t *HTTPTransporter) requestVoteHandler(server *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		traceln(server.Name(), "RECV /requestVote")

		defer r.Body.Close()
		req := &RequestVoteRequest{}
		if _, err := req.decode(r.Body); err != nil {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		resp := server.RequestVote(req)
		if _, err := resp.encode(w); err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
	}
}
