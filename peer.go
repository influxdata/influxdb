package raft

import (
	"errors"
	"sync"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A peer is a reference to another server involved in the consensus protocol.
type Peer struct {
	server         *Server
	name           string
	prevLogIndex   uint64
	mutex          sync.Mutex
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new peer.
func NewPeer(server *Server, name string) *Peer {
	return &Peer{
		server: server,
		name:   name,
	}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// Retrieves the name of the peer.
func (p *Peer) Name() string {
	return p.name
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

// Generates and sends an AppendEntries RPC from the server for the peer.
// This serves to replicate the log and to provide a heartbeat mechanism. It
// returns the current term from the peer, whether the flush was successful
// and any associated error message.
func (p *Peer) flush() (uint64, bool, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	req, handler := p.server.createAppendEntriesRequest(p.prevLogIndex)
	return p.sendFlushRequest(req, handler)
}

// Sends an AppendEntries RPC but does not obtain a lock on the server. This
// method should only be called from the server.
func (p *Peer) internalFlush() (uint64, bool, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	req, handler := p.server.createInternalAppendEntriesRequest(p.prevLogIndex)
	return p.sendFlushRequest(req, handler)
}

// Flushes a request through a handler.
func (p *Peer) sendFlushRequest(req *AppendEntriesRequest, handler func(*Server, *Peer, *AppendEntriesRequest) (*AppendEntriesResponse, error)) (uint64, bool, error) {
	// Ignore any null requests/handlers.
	if req == nil || handler == nil {
		return 0, false, errors.New("raft.Peer: Request or handler required")
	}

	// Generate an AppendEntries request based on the state of the server and
	// log. Send the request through the user-provided handler and process the
	// result.
	resp, err := handler(p.server, p, req)
	if resp == nil {
		return 0, false, err
	}
	
	// If successful then update the previous log index. If it was
	// unsuccessful then decrement the previous log index and we'll try again
	// next time.
	if resp.Success {
		if len(req.Entries) > 0 {
			p.prevLogIndex = req.Entries[len(req.Entries)-1].index
		}
	} else {
		if p.prevLogIndex > 0 {
			p.prevLogIndex--
		}
	}

	return resp.Term, resp.Success, err
}
