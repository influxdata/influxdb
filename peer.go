package raft

import (
	"errors"
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
	server         *Server
	name           string
	prevLogIndex   uint64
	mutex          sync.Mutex
	heartbeatTimer *Timer
}

type FlushResponse struct {
	term    uint64
	success bool
	err     error
	peer    *Peer
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new peer.
func NewPeer(server *Server, name string, heartbeatTimeout time.Duration) *Peer {
	p := &Peer{
		server:         server,
		name:           name,
		heartbeatTimer: NewTimer(heartbeatTimeout, heartbeatTimeout),
	}

	return p
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

// Retrieves the heartbeat timeout.
func (p *Peer) HeartbeatTimeout() time.Duration {
	return p.heartbeatTimer.MinDuration()
}

// Sets the heartbeat timeout.
func (p *Peer) SetHeartbeatTimeout(duration time.Duration) {
	p.heartbeatTimer.SetDuration(duration)
}

func (p *Peer) StartHeartbeat() {
	go p.heartbeat()
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// State
//--------------------------------------

// Stops the peer entirely.
func (p *Peer) stop() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.heartbeatTimer.Stop()
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
// Flush
//--------------------------------------

// Sends an AppendEntries RPC but does not obtain a lock
// on the server.
func (p *Peer) flush() (uint64, bool, error) {
	// We need to hold the log lock to create AppendEntriesRequest
	// avoid snapshot to delete the desired entries before AEQ()
	req := p.server.createAppendEntriesRequest(p.prevLogIndex)

	if req != nil {
		return p.sendFlushRequest(req)
	} else {
		req := p.server.createSnapshotRequest()
		return p.sendSnapshotRequest(req)
	}

}

// send Snapshot Request
func (p *Peer) sendSnapshotRequest(req *SnapshotRequest) (uint64, bool, error) {
	// Ignore any null requests.
	if req == nil {
		return 0, false, errors.New("raft.Peer: Request required")
	}

	// Generate an snapshot request based on the state of the server and
	// log. Send the request through the user-provided handler and process the
	// result.
	resp, err := p.server.transporter.SendSnapshotRequest(p.server, p, req)

	if resp == nil {
		return 0, false, err
	}

	// If successful then update the previous log index. If it was
	// unsuccessful then decrement the previous log index and we'll try again
	// next time.
	if resp.Success {
		p.prevLogIndex = req.LastIndex

	} else {
		panic(resp)
	}

	return resp.Term, resp.Success, err
}

// Flushes a request through the server's transport.
func (p *Peer) sendFlushRequest(req *AppendEntriesRequest) (uint64, bool, error) {
	// Ignore any null requests.
	if req == nil {
		return 0, false, errors.New("raft.Peer: Request required")
	}

	// Generate an AppendEntries request based on the state of the server and
	// log. Send the request through the user-provided handler and process the
	// result.
	//debugln("flush to ", p.Name())
	debugln("[HeartBeat] Leader ", p.server.Name(), " to ", p.Name(), " ", len(req.Entries), " ", time.Now())

	if p.server.State() != Leader {
		return 0, false, errors.New("Not leader anymore")
	}

	resp, err := p.server.transporter.SendAppendEntriesRequest(p.server, p, req)

	//debugln("receive flush response from ", p.Name())

	if resp == nil {
		return 0, false, err
	}

	// If successful then update the previous log index. If it was
	// unsuccessful then decrement the previous log index and we'll try again
	// next time.
	if resp.Success {
		if len(req.Entries) > 0 {
			p.prevLogIndex = req.Entries[len(req.Entries)-1].Index
			debugln("Peer ", p.Name(), "'s' log update to ", p.prevLogIndex)
		}
	} else {

		if p.server.State() != Leader {
			return 0, false, errors.New("Not leader anymore")
		}

		if resp.Term > p.server.currentTerm {
			return resp.Term, false, errors.New("Step down")
		}
		// Decrement the previous log index down until we find a match. Don't
		// let it go below where the peer's commit index is though. That's a
		// problem.
		if p.prevLogIndex > 0 {
			p.prevLogIndex--
		}
		if resp.CommitIndex > p.prevLogIndex {
			debugln("%v %v %v %v", resp.CommitIndex, p.prevLogIndex,
				p.server.currentTerm, resp.Term)
			panic("commitedIndex is greater than prevLogIndex")
		}
	}

	return resp.Term, resp.Success, err
}

//--------------------------------------
// Heartbeat
//--------------------------------------

// Listens to the heartbeat timeout and flushes an AppendEntries RPC.
func (p *Peer) heartbeat() {
	for {

		// (1) timeout/fire happens, flush the peer
		// (2) stopped, return

		if p.heartbeatTimer.Start() {

			var f FlushResponse

			f.peer = p

			f.term, f.success, f.err = p.flush()

			// if the peer successfully appended the log entry
			// we will tell the commit center
			if f.success {
				if p.prevLogIndex > p.server.log.CommitIndex() {
					debugln("[Heartbeat] Peer", p.Name(), "send to commit center")
					p.server.response <- f
					debugln("[Heartbeat] Peer", p.Name(), "back from commit center")
				}

			} else {
				// shutdown the heartbeat
				if f.term > p.server.currentTerm {
					debugln("[Heartbeat] SetpDown!")
					select {
					case p.server.stepDown <- f.term:
						return
					default:
						return
					}
				}
			}

		} else {
			// shutdown
			return
		}
	}
}
