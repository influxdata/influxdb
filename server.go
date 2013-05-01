package raft

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	Stopped   = "stopped"
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
)

const (
	DefaultHeartbeatTimeout = 50 * time.Millisecond
	DefaultElectionTimeout  = 150 * time.Millisecond
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A server is involved in the consensus protocol and can act as a follower,
// candidate or a leader.
type Server struct {
	ApplyFunc            func(*Server, Command)
	DoHandler            func(*Server, *Peer, Command) error
	RequestVoteHandler   func(*Server, *Peer, *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntriesHandler func(*Server, *Peer, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	name                 string
	path                 string
	state                string
	currentTerm          uint64
	votedFor             string
	log                  *Log
	leader               *Peer
	peers                map[string]*Peer
	mutex                sync.Mutex
	electionTimer        *ElectionTimer
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new server with a log at the given path.
func NewServer(name string, path string) (*Server, error) {
	if name == "" {
		return nil, errors.New("raft.Server: Name cannot be blank")
	}
	s := &Server{
		name:          name,
		path:          path,
		state:         Stopped,
		peers:         make(map[string]*Peer),
		log:           NewLog(),
		electionTimer: NewElectionTimer(DefaultElectionTimeout),
	}

	// Setup apply function.
	s.log.ApplyFunc = func(c Command) {
		if s.ApplyFunc == nil {
			panic("raft.Server: Apply function not set")
		}
		s.ApplyFunc(s, c)
	}

	return s, nil
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

//--------------------------------------
// General
//--------------------------------------

// Retrieves the name of the server.
func (s *Server) Name() string {
	return s.name
}

// Retrieves the storage path for the server.
func (s *Server) Path() string {
	return s.path
}

// Retrieves the log path for the server.
func (s *Server) LogPath() string {
	return fmt.Sprintf("%s/log", s.path)
}

// Retrieves the current state of the server.
func (s *Server) State() string {
	return s.state
}

//--------------------------------------
// Membership
//--------------------------------------

// Retrieves the number of member servers in the consensus.
func (s *Server) MemberCount() int {
	count := 1
	for _, _ = range s.peers {
		count++
	}
	return count
}

// Retrieves the number of servers required to make a quorum.
func (s *Server) QuorumSize() int {
	return (s.MemberCount() / 2) + 1
}

//--------------------------------------
// Election timeout
//--------------------------------------

// Retrieves the election timeout.
func (s *Server) ElectionTimeout() time.Duration {
	return s.electionTimer.Duration()
}

// Sets the election timeout.
func (s *Server) SetElectionTimeout(duration time.Duration) {
	s.electionTimer.SetDuration(duration)
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// State
//--------------------------------------

// Starts the server with a log at the given path.
func (s *Server) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Exit if the server is already running.
	if s.Running() {
		return errors.New("raft.Server: Server already running")
	}

	// Initialize the log and load it up.
	if err := s.log.Open(s.LogPath()); err != nil {
		s.unload()
		return fmt.Errorf("raft.Server: %v", err)
	}

	// Update the state.
	s.state = Follower

	return nil
}

// Shuts down the server.
func (s *Server) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.unload()
}

// Unloads the server.
func (s *Server) unload() {
	if s.log != nil {
		s.log.Close()
		s.log = nil
	}

	s.state = Stopped
}

// Checks if the server is currently running.
func (s *Server) Running() bool {
	return s.state != Stopped
}

//--------------------------------------
// Commands
//--------------------------------------

// Adds a command type to the log. The instance passed in will be copied and
// deserialized each time a new log entry is read. This function will panic
// if a command type with the same name already exists.
func (s *Server) AddCommandType(command Command) {
	s.log.AddCommandType(command)
}

// Attempts to execute a command and replicate it. The function will return
// when the command has been successfully committed or an error has occurred.
func (s *Server) Do(command Command) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.do(command)
}

// This function is the low-level interface to execute commands. This function
// does not obtain a lock so one must be obtained before executing.
func (s *Server) do(command Command) error {
	return nil
}

// Executes the handler for doing a command on a particular peer.
func (s *Server) executeDoHandler(peer *Peer, command Command) error {
	if s.DoHandler == nil {
		panic("raft.Server: DoHandler not registered")
	}
	return s.DoHandler(s, peer, command)
}

// Appends a log entry from the leader to this server.
func (s *Server) AppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// If the request is coming from an old term then reject it.
	if req.Term < s.currentTerm {
		return NewAppendEntriesResponse(s.currentTerm, false), fmt.Errorf("raft.Server: Stale request term")
	}
	s.setCurrentTerm(req.Term)
	s.state = Follower

	// Reset election timeout.
	s.electionTimer.Reset()

	// Reject if log doesn't contain a matching previous entry.
	if err := s.log.Truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		return NewAppendEntriesResponse(s.currentTerm, false), err
	}

	// Append entries to the log.
	if err := s.log.AppendEntries(req.Entries); err != nil {
		return NewAppendEntriesResponse(s.currentTerm, false), err
	}

	// Commit up to the commit index.
	if err := s.log.SetCommitIndex(req.CommitIndex); err != nil {
		return NewAppendEntriesResponse(s.currentTerm, false), err
	}

	return NewAppendEntriesResponse(s.currentTerm, true), nil
}

//--------------------------------------
// Promotion
//--------------------------------------

// Promotes the server to a candidate and then requests votes from peers. If
// enough votes are received then the server becomes the leader. If this
// server is elected then true is returned. If another server is elected then
// false is returned.
func (s *Server) promote() (bool, error) {
	for {
		// Start a new election.
		term, lastLogIndex, lastLogTerm := s.promoteToCandidate()

		// Request votes from each of our peers.
		c := make(chan *RequestVoteResponse, len(s.peers))
		for _, _peer := range s.peers {
			peer := _peer
			go func() {
				req := NewRequestVoteRequest(term, s.name, lastLogIndex, lastLogTerm)
				req.peer = peer
				resp, _ := s.executeRequestVoteHandler(peer, req)
				resp.peer = peer
				c <- resp
			}()
		}

		// Collect votes until we have a quorum.
		votes := map[string]bool{}
		elected := false
	loop:
		for {
			// Add up all our votes.
			votesGranted := 1
			for _, value := range votes {
				if value {
					votesGranted++
				}
			}
			// If we received enough votes then stop waiting for more votes.
			if votesGranted >= s.QuorumSize() {
				elected = true
				break
			}

			// Collect votes from peers.
			select {
			case resp := <-c:
				if resp != nil {
					// Step down if we discover a higher term.
					if resp.Term > term {
						s.setCurrentTerm(term)
						s.electionTimer.Reset()
						return false, fmt.Errorf("raft.Server: Higher term discovered, stepping down: (%v > %v)", resp.Term, term)
					}
					votes[resp.peer.Name()] = resp.VoteGranted
				}
			case <-time.After(s.ElectionTimeout()):
				break loop
			}
		}

		// If we received enough votes then promote to leader and stop this election.
		if elected && s.promoteToLeader(term, lastLogIndex, lastLogTerm) {
			break
		}

		// If we are no longer in the same term then another server must have been elected.
		if s.currentTerm != term {
			return false, fmt.Errorf("raft.Server: Term changed during election, stepping down: (%v > %v)", s.currentTerm, term)
		}
	}

	return true, nil
}

// Promotes the server to a candidate and increases the election term. The
// term and log state are returned for use in the RPCs.
func (s *Server) promoteToCandidate() (term uint64, lastLogIndex uint64, lastLogTerm uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Move server to become a candidate, increase our term & vote for ourself.
	s.state = Candidate
	s.currentTerm++
	s.votedFor = s.name

	// Pause the election timer while we're a candidate.
	s.electionTimer.Pause()

	// Return server state so we can check for it during leader promotion.
	lastLogIndex, lastLogTerm = s.log.CommitInfo()
	return s.currentTerm, lastLogIndex, lastLogTerm
}

// Promotes the server from a candidate to a leader. This can only occur if
// the server is in the state that it assumed when the candidate election
// began. This is because another server may have won the election and caused
// the state to change.
func (s *Server) promoteToLeader(term uint64, lastLogIndex uint64, lastLogTerm uint64) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Ignore promotion if we are not a candidate.
	if s.state != Candidate {
		return false
	}

	// Disallow promotion if the term or log does not match what we currently have.
	logIndex, logTerm := s.log.CommitInfo()
	if s.currentTerm != term || logIndex != lastLogIndex || logTerm != lastLogTerm {
		return false
	}

	// Move server to become a leader.
	s.state = Leader
	// TODO: Begin heartbeat to peers.

	return true
}

//--------------------------------------
// Request Vote
//--------------------------------------

// Requests a vote from a server. A vote can be obtained if the vote's term is
// at the server's current term and the server has not made a vote yet. A vote
// can also be obtained if the term is greater than the server's current term.
func (s *Server) RequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// If the request is coming from an old term then reject it.
	if req.Term < s.currentTerm {
		return NewRequestVoteResponse(s.currentTerm, false)
	}
	s.setCurrentTerm(req.Term)

	// If we've already voted for a different candidate then don't vote for this candidate.
	if s.votedFor != "" && s.votedFor != req.CandidateName {
		return NewRequestVoteResponse(s.currentTerm, false)
	}

	// If the candidate's log is not at least as up-to-date as our committed log then don't vote.
	lastCommitIndex, lastCommitTerm := s.log.CommitInfo()
	if lastCommitIndex > req.LastLogIndex || lastCommitTerm > req.LastLogTerm {
		return NewRequestVoteResponse(s.currentTerm, false)
	}

	// If we made it this far then cast a vote and reset our election time out.
	s.votedFor = req.CandidateName
	s.electionTimer.Reset()
	return NewRequestVoteResponse(s.currentTerm, true)
}

// Executes the handler for sending a RequestVote RPC.
func (s *Server) executeRequestVoteHandler(peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if s.RequestVoteHandler == nil {
		panic("raft.Server: RequestVoteHandler not registered")
	}
	return s.RequestVoteHandler(s, peer, req)
}

// Updates the current term on the server if the term is greater than the 
// server's current term. When the term is changed then the server's vote is
// cleared and its state is changed to be a follower.
func (s *Server) setCurrentTerm(term uint64) {
	if term > s.currentTerm {
		s.currentTerm = term
		s.votedFor = ""
		s.state = Follower
	}
}

//--------------------------------------
// Membership
//--------------------------------------

// Connects to a given server and attempts to gain membership.
func (s *Server) Join(name string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Exit if the server is not running.
	if !s.Running() {
		return errors.New("raft.Server: Cannot join while stopped")
	} else if s.MemberCount() > 1 {
		return errors.New("raft.Server: Cannot join; already in membership")
	}

	// If joining self then promote to leader.
	if s.name == name {
		s.state = Leader
		return nil
	}

	// Request membership.
	command := &JoinCommand{Name: s.name}
	return s.executeDoHandler(NewPeer(name), command)
}
