package raft

import (
	"errors"
	"fmt"
	"sync"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	Stopped   = iota
	Follower  = iota
	Candidate = iota
	Leader    = iota
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
	currentTerm int
	state       int
	votedFor    int
	log         *Log
	replicas    []*Replica
	mutex       sync.Mutex
}

//--------------------------------------
// Replicas
//--------------------------------------

// A replica is a reference to another server involved in the consensus protocol.
type Replica struct {
	name           string
	voteResponded  bool
	voteGranted    bool
	nextIndex      int
	lastAgreeIndex int
}

//--------------------------------------
// Request Vote RPC
//--------------------------------------

// The request sent to a server to vote for a candidate to become a leader.
type RequestVoteRequest struct {
	Term         int `json:"term"`
	CandidateId  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

// The response returned from a server after a vote for a candidate to become a leader.
type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

//--------------------------------------
// Append Entries RPC
//--------------------------------------

// The request sent to a server to append entries to the log.
type AppendEntriesRequest struct {
	Term         int         `json:"term"`
	LeaderId     int         `json:"leaderId"`
	PrevLogIndex int         `json:"prevLogIndex"`
	PrevLogTerm  int         `json:"prevLogTerm"`
	Entries      []*LogEntry `json:"entries"`
	CommitIndex  int         `json:"commitIndex"`
}

// The response returned from a server appending entries to the log.
type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
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
func (s *Server) State() int {
	return s.state
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
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.log.AddCommandType(command)
}
