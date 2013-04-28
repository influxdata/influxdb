package raft

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// The request sent to a server to append entries to the log.
type AppendEntriesRequest struct {
	peer *Peer
	Term         uint64      `json:"term"`
	LeaderName     string      `json:"leaderName"`
	PrevLogIndex uint64      `json:"prevLogIndex"`
	PrevLogTerm  uint64      `json:"prevLogTerm"`
	Entries      []*LogEntry `json:"entries"`
	CommitIndex  uint64      `json:"commitIndex"`
}

// The response returned from a server appending entries to the log.
type AppendEntriesResponse struct {
	peer *Peer
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

