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
// TODO add cluster configuration
type Snapshot struct {
	lastIndex uint64
	lastTerm uint64
	// cluster configuration. 
	state []byte
	path string
}

// Save the snapshot to a file
func (ss *Snapshot) Save() error {
	// Write machine state to temporary buffer.
	var b bytes.Buffer

	if _, err := fmt.Fprintf(&b, "%v", 2); err != nil {
		return err
	}

	// Generate checksum.
	checksum := crc32.ChecksumIEEE(b.Bytes())

	// open file
	file, err := os.OpenFile(ss.path, os.O_CREATE|os.O_WRONLY, 0600)

	if err != nil {
		return err
	}

	defer file.Close()


	// Write snapshot with checksum.
	if _, err = fmt.Fprintf(file, "%08x\n%v\n%v\n", checksum, ss.lastIndex, 
		ss.lastTerm); err != nil {
		return err
	}

	if  _, err = file.Write(ss.state); err != nil {
		return err
	}

	// force the change writting to disk
	syscall.Fsync(int(file.Fd()))
	return err
}

// remove the file of the snapshot
func (ss *Snapshot) Remove() error {
	err := os.Remove(ss.path)
	return err
}
