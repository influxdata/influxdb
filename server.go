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
	DefaultElectionTimeout = 150 * time.Millisecond
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A server is involved in the consensus protocol and can act as a follower,
// candidate or a leader.
type Server struct {
	name        string
	path        string
	state       string
	currentTerm uint64
	votedFor    string
	log         *Log
	leader      *Peer
	peers       map[string]*Peer
	mutex       sync.Mutex
	ElectionTimeout    int
	DoHandler    func(*Server, *Peer, Command) error
	AppendEntriesHandler    func(*Server, *AppendEntriesRequest) (*AppendEntriesResponse, error)
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
		name: name,
		path: path,
		state: Stopped,
		log: NewLog(),
	}
	return s, nil
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

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

// Retrieves the number of member servers in the consensus.
func (s *Server) MemberCount() uint64 {
	var count uint64 = 1
	for _,_ = range s.peers {
		count++
	}
	return count
}

// Retrieves the number of servers required to make a quorum.
func (s *Server) QuorumSize() uint64 {
	return (s.MemberCount() / 2) + 1
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
// Handlers
//--------------------------------------

// Executes the handler for executing a command.
func (s *Server) executeDoHandler(peer *Peer, command Command) error {
	if s.DoHandler == nil {
		return errors.New("raft.Server: DoHandler not registered")
	} else if peer == nil {
		return errors.New("raft.Server: Peer required")
	}
	
	return s.DoHandler(s, peer, command)
}

//--------------------------------------
// Commands
//--------------------------------------

// Adds a command type to the log. The instance passed in will be copied and
// deserialized each time a new log entry is read. This function will panic
// if a command type with the same name already exists.
func (s *Server) AddCommandType(command Command) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
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
	warn("[%s] do.1 (%s) %v", s.name, command.CommandName(), command)
	
	// Send the request to the leader if we're not the leader.
	if s.state != Leader {
		// TODO: If we don't have a leader then elect one.
		return s.executeDoHandler(s.leader, command)
	}
	
	warn("[%s] do.2", s.name)

	// If we are the leader then create a new log entry.
	commitIndex := s.log.CommitIndex()
	prevLogIndex, prevLogTerm := s.log.CurrentIndex(), s.log.CurrentTerm()
	entry := s.log.CreateEntry(s.currentTerm, command)
	s.log.Append(entry)

	// Send the entry to all the peers.
	c := make(chan interface{})
	for _, peer := range s.peers {
		go func() {
			// Generate request.
			request := &AppendEntriesRequest{
				peer: peer,
				Term: s.currentTerm,
				LeaderName: s.name,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				CommitIndex: commitIndex,
				Entries: []*LogEntry{entry},
			}

			// Send response through the channel that gets collected below.
			if resp, err := s.AppendEntriesHandler(s, request); err != nil {
				resp.peer = peer
				warn("raft.Server: Error in AppendEntriesHandler: %v", err)
			} else {
				c <- resp
			}
		}()
	}
	
	// Collect all the responses until consensus or timeout.
	votes := map[*Peer]bool{}
	timeoutChannel, success := time.After(DefaultElectionTimeout), false
	for {
		// If we have reached a quorum then exit.
		var voteCount uint64 = 1
		for _, _ = range votes {
			voteCount++
		}
		if voteCount >= s.QuorumSize() {
			success = true
			break
		}

		// Attempt to retrieve 'Append Entries' responses or timeout.
		select {
			case ret := <-c:
				if resp, ok := ret.(AppendEntriesResponse); ok {
					// If we're in the same term then save the vote.
					if resp.Term == s.currentTerm {
						votes[resp.peer] = resp.Success
					} else if resp.Term > s.currentTerm {
						// TODO: Reset to follower and elect leader.
					}
				}
			case <-timeoutChannel:
				success = false
				break
		}
	}
	
	// If we succeeded then commit the entry.
	if success {
		// TODO: Update commit index.
	} else {
		// TODO: Otherwise restart.
		panic("raft.Server: 'DO' did not succeed. (NOT YET IMPLEMENTED)")
	}
	
	return nil
}

// Appends a log entry from the leader to this server.
func (s *Server) AppendEntry(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	warn("raft.Sever: AppendEntry not implemented yet.")
	return nil, nil
}

//--------------------------------------
// Membership
//--------------------------------------

// Connects to a given server and attempts to gain membership.
func (s *Server) Join(name string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	warn("[%s] join.1", s.name)

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
	command := &JoinCommand{Name:s.name}
	return s.executeDoHandler(NewPeer(name), command)
}

