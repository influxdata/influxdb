package raft

import (
	"hash/crc32"
	"fmt"
	"syscall"
	"bytes"
	"os"
	)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// the in memory SnapShot struct 
type Snapshot struct {
	lastIndex uint64
	lastTerm uint64
	// cluster configuration. 
	machineState int
	path string
}

// The request sent to a server to start from the snapshot.
type SnapshotRequest struct {
	LeaderName   string      `json:"leaderName"`
	LastIndex    uint64 	 `json:"lastTerm"`
	LastTerm	 uint64		 `json:"lastIndex"`
	MachineState int 		 `json:"machineState"`
}

// The response returned from a server appending entries to the log.
type SnapshotResponse struct {
	Term        uint64 `json:"term"`
	Success     bool   `json:"success"`
	CommitIndex uint64 `json:"commitIndex"`
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// Creates a new Snapshot request.
func NewSnapshotRequest(leaderName string, snapshot *Snapshot) *SnapshotRequest {
	return &SnapshotRequest{
		LeaderName:   leaderName,
		LastIndex:    snapshot.lastIndex,
		LastTerm:	  snapshot.lastTerm,
		MachineState: snapshot.machineState,
	}
}

// Creates a new Snapshot response.
func NewSnapshotResponse(term uint64, success bool, commitIndex uint64) *SnapshotResponse {
	return &SnapshotResponse{
		Term:        term,
		Success:     success,
		CommitIndex: commitIndex,
	}
}


func (ss *Snapshot) Save() error {
	// Write machine state to temporary buffer.
	var b bytes.Buffer

	if _, err := fmt.Fprintf(&b, "%v", 2); err != nil {
		return err
	}

	// Generate checksum.
	checksum := crc32.ChecksumIEEE(b.Bytes())

	fmt.Println(ss.path)
	// open file
	file, err := os.OpenFile(ss.path, os.O_CREATE|os.O_WRONLY, 0600)
	
	if err != nil {
		return err
	}

	defer file.Close()


	// Write log entry with checksum.
	if _, err = fmt.Fprintf(file, "%08x\n%s\n%v\n%v\n", checksum, b.String(), 
		ss.lastIndex, ss.lastTerm); err != nil {
		return err
	}

	// force the change writting to disk
	syscall.Fsync(int(file.Fd()))
	return err
}

func (ss *Snapshot) Remove() error {
	err := os.Remove(ss.path)
	return err
}
