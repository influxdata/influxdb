package raft

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

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
