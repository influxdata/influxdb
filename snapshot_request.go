package raft

// The request sent to a server to start from the snapshot.
type SnapshotRequest struct {
	LeaderName string `json:"leaderName"`
	LastIndex  uint64 `json:"lastTerm"`
	LastTerm   uint64 `json:"lastIndex"`
}

// The response returned if the follower entered snapshot state
type SnapshotResponse struct {
	Success bool `json:"success"`
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// Creates a new Snapshot request.
func newSnapshotRequest(leaderName string, snapshot *Snapshot) *SnapshotRequest {
	return &SnapshotRequest{
		LeaderName: leaderName,
		LastIndex:  snapshot.LastIndex,
		LastTerm:   snapshot.LastTerm,
	}
}

// Creates a new Snapshot response.
func newSnapshotResponse(success bool) *SnapshotResponse {
	return &SnapshotResponse{
		Success: success,
	}
}
