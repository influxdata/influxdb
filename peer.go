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

	// Start the heartbeat timeout and wait for the goroutine to start.
	c := make(chan bool)
	go p.heartbeatTimeoutFunc(c)
	<-c

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

func (p *Peer) StartHeartbeatTimeout() {
	p.heartbeatTimer.Reset()
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// State
//--------------------------------------

// Resumes the peer heartbeating.
func (p *Peer) resume() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.heartbeatTimer.Reset()
}

// Pauses the peer to prevent heartbeating.
func (p *Peer) pause() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.heartbeatTimer.Pause()
}

// Stops the peer entirely.
func (p *Peer) stop() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.heartbeatTimer.Stop()
}

//--------------------------------------
// Flush
//--------------------------------------

// Sends an AppendEntries RPC but does not obtain a lock
// on the server.
func (p *Peer) flush() (uint64, bool, error) {

	server, prevLogIndex := p.server, p.prevLogIndex

	var req *AppendEntriesRequest
	snapShotNeeded := false

	// we need to hold the log lock to create AppendEntriesRequest
	// avoid snapshot to delete the desired entries before AEQ()

	server.log.mutex.Lock()
	if prevLogIndex >= server.log.StartIndex() {
		req = server.createInternalAppendEntriesRequest(prevLogIndex)
	} else {
		snapShotNeeded = true
	}
	server.log.mutex.Unlock()

	if snapShotNeeded {
		req := server.createSnapshotRequest()
		return p.sendSnapshotRequest(req)
	} else {
		return p.sendFlushRequest(req)
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
	p.heartbeatTimer.Reset()
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
	//fmt.Println("flush to ", p.Name())
	fmt.Println("[HeartBeat] Leader ", p.server.Name(), " to ",
		p.Name(), " ", len(req.Entries), " ", time.Now())
	resp, err := p.server.transporter.SendAppendEntriesRequest(p.server, p, req)

	//fmt.Println("receive flush response from ", p.Name())

	p.heartbeatTimer.Reset()
	if resp == nil {
		return 0, false, err
	}

	// If successful then update the previous log index. If it was
	// unsuccessful then decrement the previous log index and we'll try again
	// next time.
	if resp.Success {
		if len(req.Entries) > 0 {
			p.prevLogIndex = req.Entries[len(req.Entries)-1].Index
			fmt.Println("Peer ", p.Name(), "'s' log update to ", p.prevLogIndex)
		}
	} else {
		// Decrement the previous log index down until we find a match. Don't
		// let it go below where the peer's commit index is though. That's a
		// problem.
		if p.prevLogIndex > 0 {
			p.prevLogIndex--
		}
		if resp.CommitIndex > p.prevLogIndex {
			p.prevLogIndex = resp.CommitIndex
		}
	}

	return resp.Term, resp.Success, err
}

//--------------------------------------
// Heartbeat
//--------------------------------------

// Listens to the heartbeat timeout and flushes an AppendEntries RPC.
func (p *Peer) heartbeatTimeoutFunc(startChannel chan bool) {
	startChannel <- true

	for {
		// Grab the current timer channel.
		p.mutex.Lock()

		var c chan time.Time
		if p.heartbeatTimer != nil {
			c = p.heartbeatTimer.C()
		}
		p.mutex.Unlock()

		// If the channel or timer are gone then exit.
		if c == nil {
			break
		}

		if _, ok := <-c; ok {

			var f FlushResponse

			f.peer = p

			f.term, f.success, f.err = p.flush()

			// if the peer successfully appended the log entry
			// we will tell the commit center
			if f.success {
				if p.prevLogIndex > p.server.log.CommitIndex() {
					fmt.Println("[Heartbeat] Peer", p.Name(), "send to commit center")
					p.server.response <- f
					fmt.Println("[Heartbeat] Peer", p.Name(), "back from commit center")
				}

			} else {
				if f.term > p.server.currentTerm {
					fmt.Println("[Heartbeat] SetpDown!")
					select {
					case p.server.stepDown <- f.term:
						p.pause()
					default:
						p.pause()
					}
				}
			}

		} else {
			break
		}
	}
}
