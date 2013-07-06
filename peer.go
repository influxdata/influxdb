package raft

import (
	"errors"
	"fmt"
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
	heartbeatTimer *timer
}

type flushResponse struct {
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
func newPeer(server *Server, name string, heartbeatTimeout time.Duration) *Peer {
	p := &Peer{
		server:         server,
		name:           name,
		heartbeatTimer: newTimer(heartbeatTimeout, heartbeatTimeout),
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

// Sets the heartbeat timeout.
func (p *Peer) setHeartbeatTimeout(duration time.Duration) {
	p.heartbeatTimer.setDuration(duration)
}

func (p *Peer) startHeartbeat() {
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
	p.heartbeatTimer.stop()
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

// send VoteRequest Request
func (p *Peer) sendVoteRequest(req *RequestVoteRequest, c chan *RequestVoteResponse) {
	req.peer = p
	debugln(p.server.Name(), "Send Vote Request to ", p.Name())
	if resp, _ := p.server.transporter.SendVoteRequest(p.server, p, req); resp != nil {
		resp.peer = p
		c <- resp
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

	respChan := make(chan *AppendEntriesResponse, 2)

	go func() {
		tranResp, _ := p.server.transporter.SendAppendEntriesRequest(p.server, p, req)
		respChan <- tranResp
	}()

	var resp *AppendEntriesResponse

	select {
	// how to decide?
	case <-time.After(p.server.heartbeatTimeout * 2):
		resp = nil

	case resp = <-respChan:

	}

	if resp == nil {
		debugln("receive flush timeout from ", p.Name())
		return 0, false, fmt.Errorf("AppendEntries timeout: %s", p.Name())
	}
	debugln("receive flush response from ", p.Name())

	// If successful then update the previous log index. If it was
	// unsuccessful then decrement the previous log index and we'll try again
	// next time.
	if resp.Success {
		if len(req.Entries) > 0 {
			p.prevLogIndex = req.Entries[len(req.Entries)-1].Index
		}
		debugln(p.server.GetState()+": Peer ", p.Name(), "'s' log update to ", p.prevLogIndex)
	} else {

		if resp.Term > p.server.currentTerm {
			return resp.Term, false, errors.New("Step down")
		}

		// we may miss a response from peer
		if resp.CommitIndex >= p.prevLogIndex {
			debugln(p.server.GetState()+": Peer ", p.Name(), "'s' log update to ", p.prevLogIndex)
			p.prevLogIndex = resp.CommitIndex
		} else if p.prevLogIndex > 0 {
			debugln("Peer ", p.Name(), "'s' step back to ", p.prevLogIndex)
			// Decrement the previous log index down until we find a match. Don't
			// let it go below where the peer's commit index is though. That's a
			// problem.
			p.prevLogIndex--
		}

	}
	return resp.Term, resp.Success, nil
}

//--------------------------------------
// Heartbeat
//--------------------------------------

// Listens to the heartbeat timeout and flushes an AppendEntries RPC.
func (p *Peer) heartbeat() {
	for {

		// (1) timeout/fire happens, flush the peer
		// (2) stopped, return

		if p.heartbeatTimer.start() {

			var f flushResponse

			f.peer = p

			f.term, f.success, f.err = p.flush()

			// if the peer successfully appended the log entry
			// we will tell the commit center
			if f.success {
				if p.prevLogIndex > p.server.log.commitIndex {
					debugln("[Heartbeat] Peer", p.Name(), "send to commit center")
					p.server.response <- f
					debugln("[Heartbeat] Peer", p.Name(), "back from commit center")
				}

			} else {
				// shutdown the heartbeat
				if f.term > p.server.currentTerm {
					p.server.stateMutex.Lock()

					if p.server.state == Leader {
						p.server.state = Follower
						select {
						case p.server.stepDown <- f.term:
							p.server.currentTerm = f.term
						default:
							panic("heartbeat cannot step down")
						}
					}

					p.server.stateMutex.Unlock()
					return
				}
			}

		} else {
			// shutdown
			return
		}
	}
}
