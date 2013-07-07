package raft

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// The request sent to a server to vote for a candidate to become a leader.
type RequestVoteRequest struct {
	peer          *Peer
	Term          uint64 `json:"term"`
	CandidateName string `json:"candidateName"`
	LastLogIndex  uint64 `json:"lastLogIndex"`
	LastLogTerm   uint64 `json:"lastLogTerm"`
}

// The response returned from a server after a vote for a candidate to become a leader.
type RequestVoteResponse struct {
	peer        *Peer
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// Creates a new RequestVote request.
func newRequestVoteRequest(term uint64, candidateName string, lastLogIndex uint64, lastLogTerm uint64) *RequestVoteRequest {
	return &RequestVoteRequest{
		Term:          term,
		CandidateName: candidateName,
		LastLogIndex:  lastLogIndex,
		LastLogTerm:   lastLogTerm,
	}
}

// Creates a new RequestVote response.
func newRequestVoteResponse(term uint64, voteGranted bool) *RequestVoteResponse {
	return &RequestVoteResponse{
		Term:        term,
		VoteGranted: voteGranted,
	}
}
