package raft

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
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
	replicas    []*Replica
	currentTerm int
	state       int
	votedFor    int
}

//--------------------------------------
// Replicas
//--------------------------------------

// A replica is a reference to another server involved in the consensus protocol.
type Replica struct {
	hostname       string
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

// Creates a new server.
func NewServer() *Server {
	return &Server{
		state: Follower,
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------
