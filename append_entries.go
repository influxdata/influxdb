package raft

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// The request sent to a server to append entries to the log.
type AppendEntriesRequest struct {
	Term         uint64      `json:"term"`
	LeaderName   string      `json:"leaderName"`
	PrevLogIndex uint64      `json:"prevLogIndex"`
	PrevLogTerm  uint64      `json:"prevLogTerm"`
	Entries      []*LogEntry `json:"entries"`
	CommitIndex  uint64      `json:"commitIndex"`
}

// The response returned from a server appending entries to the log.
type AppendEntriesResponse struct {
	Term        uint64 `json:"term"`
	Success     bool   `json:"success"`
	CommitIndex uint64 `json:"commitIndex"`
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// Creates a new AppendEntries request.
func NewAppendEntriesRequest(term uint64, leaderName string, prevLogIndex uint64, prevLogTerm uint64, entries []*LogEntry, commitIndex uint64) *AppendEntriesRequest {
	return &AppendEntriesRequest{
		Term:         term,
		LeaderName:   leaderName,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		CommitIndex:  commitIndex,
	}
}

// Creates a new AppendEntries response.
func NewAppendEntriesResponse(term uint64, success bool, commitIndex uint64) *AppendEntriesResponse {
	return &AppendEntriesResponse{
		Term:        term,
		Success:     success,
		CommitIndex: commitIndex,
	}
}
