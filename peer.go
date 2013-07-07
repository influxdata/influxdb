package raft

import (
	"sync"
	"time"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A peer is a reference to another server involved in the consensus protocol.
type Peer struct {
	server           *Server
	name             string
	prevLogIndex     uint64
	mutex            sync.Mutex
	stopChan         chan bool
	heartbeatTimeout time.Duration
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new peer.
func newPeer(server *Server, name string, heartbeatTimeout time.Duration) *Peer {
	return &Peer{
		server:           server,
		name:             name,
		stopChan:         make(chan bool),
		heartbeatTimeout: heartbeatTimeout,
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

// Sets the heartbeat timeout.
func (p *Peer) setHeartbeatTimeout(duration time.Duration) {
	p.heartbeatTimeout = duration
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Heartbeat
//--------------------------------------

// Starts the peer heartbeat.
func (p *Peer) startHeartbeat() {
	c := make(chan bool)
	go p.heartbeat(c)
	<-c
}

// Stops the peer heartbeat.
func (p *Peer) stopHeartbeat() {
	p.stopChan <- true
}

//--------------------------------------
// Copying
//--------------------------------------

// Clones the state of the peer. The clone is not attached to a server and
// the heartbeat timer will not exist.
func (p *Peer) clone() *Peer {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return &Peer{
		name:         p.name,
		prevLogIndex: p.prevLogIndex,
	}
}

//--------------------------------------
// Heartbeat
//--------------------------------------

// Listens to the heartbeat timeout and flushes an AppendEntries RPC.
func (p *Peer) heartbeat(c chan bool) {
	c <- true

	for {
		select {
		case <-p.stopChan:
			return

		case <-time.After(p.heartbeatTimeout):
			p.flush()
		}
	}
}

//--------------------------------------
// Append Entries
//--------------------------------------

// Sends an AppendEntries RPC.
func (p *Peer) flush() {
	entries, prevLogTerm := p.server.log.getEntriesAfter(p.prevLogIndex)
	if entries != nil {
		p.sendAppendEntriesRequest(newAppendEntriesRequest(p.server.currentTerm, p.server.name, p.prevLogIndex, prevLogTerm, entries, p.server.log.commitIndex))
	} else {
		p.sendSnapshotRequest(newSnapshotRequest(p.server.name, p.server.lastSnapshot))
	}
}

// Sends an AppendEntries request to the peer through the transport.
func (p *Peer) sendAppendEntriesRequest(req *AppendEntriesRequest) {
	traceln("peer.flush.send: ", p.server.Name(), "->", p.Name(), " ", len(req.Entries))

	resp := p.server.transporter.SendAppendEntriesRequest(p.server, p, req)
	if resp == nil {
		debugln("peer.flush.timeout: ", p.server.Name(), "->", p.Name())
		return
	}
	traceln("peer.flush.recv: ", p.Name())

	// If successful then update the previous log index.
	if resp.Success {
		if len(req.Entries) > 0 {
			p.prevLogIndex = req.Entries[len(req.Entries)-1].Index
		}
		traceln("peer.flush.success: ", p.server.Name(), "->", p.Name(), "; idx =", p.prevLogIndex)

		// If it was unsuccessful then decrement the previous log index and
		// we'll try again next time.
	} else {
		// we may miss a response from peer
		if resp.CommitIndex >= p.prevLogIndex {
			p.prevLogIndex = resp.CommitIndex
			debugln("peer.flush.commitIndex: ", p.server.Name(), "->", p.Name(), " idx =", p.prevLogIndex)
		} else if p.prevLogIndex > 0 {
			// Decrement the previous log index down until we find a match. Don't
			// let it go below where the peer's commit index is though. That's a
			// problem.
			p.prevLogIndex--
			debugln("peer.flush.decrement: ", p.server.Name(), "->", p.Name(), " idx =", p.prevLogIndex)
		}
	}

	// Send response to server for processing.
	p.server.send(resp)
}

// Sends an Snapshot request to the peer through the transport.
func (p *Peer) sendSnapshotRequest(req *SnapshotRequest) {
	debugln("peer.snap.send: ", p.name)

	resp := p.server.transporter.SendSnapshotRequest(p.server, p, req)
	if resp == nil {
		debugln("peer.snap.timeout: ", p.name)
		return
	}

	debugln("peer.snap.recv: ", p.name)

	// If successful then update the previous log index.
	if resp.Success {
		p.prevLogIndex = req.LastIndex
	} else {
		debugln("peer.snap.failed: ", p.name)
	}

	// Send response to server for processing.
	p.server.send(resp)
}

//--------------------------------------
// Vote Requests
//--------------------------------------

// send VoteRequest Request
func (p *Peer) sendVoteRequest(req *RequestVoteRequest, c chan *RequestVoteResponse) {
	debugln("peer.vote: ", p.server.Name(), "->", p.Name())
	req.peer = p
	if resp := p.server.transporter.SendVoteRequest(p.server, p, req); resp != nil {
		resp.peer = p
		c <- resp
	}
}
