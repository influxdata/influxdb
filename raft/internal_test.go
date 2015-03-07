package raft

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestLog_followerLoop(t *testing.T) {
	l := NewInitializedLog()
	defer CloseLog(l)
}

func (l *Log) WaitUncommitted(index uint64) error { return l.waitUncommitted(index) }
func (l *Log) WaitCommitted(index uint64) error   { return l.waitCommitted(index) }
func (l *Log) WaitApplied(index uint64) error     { return l.Wait(index) }

// NewOpenedLog returns an opened Log. Panic on error.
func NewOpenedLog() *Log {
	l := NewLog()
	l.FSM = &IndexFSM{}
	if err := l.Open(tempfile()); err != nil {
		panic(err.Error())
	}
	return l
}

// NewInitializedLog returns an opened & initialized Log. Panic on error.
func NewInitializedLog() *Log {
	l := NewOpenedLog()
	if err := l.Initialize(); err != nil {
		panic(err.Error())
	}
	return l
}

// CloseLog closes a log and deletes its underlying path.
func CloseLog(l *Log) {
	defer os.RemoveAll(l.Path())
	l.Close()
}

// IndexFSM represents a state machine that only records the last applied index.
type IndexFSM struct {
	index uint64
}

// MustApply updates the index.
func (fsm *IndexFSM) MustApply(entry *LogEntry) { fsm.index = entry.Index }

// Index returns the highest applied index.
func (fsm *IndexFSM) Index() (uint64, error) { return fsm.index, nil }

// Snapshot writes the FSM's index as the snapshot.
func (fsm *IndexFSM) Snapshot(w io.Writer) (uint64, error) {
	return fsm.index, binary.Write(w, binary.BigEndian, fsm.index)
}

// Restore reads the snapshot from the reader.
func (fsm *IndexFSM) Restore(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, &fsm.index)
}

// tempfile returns the path to a non-existent file in the temp directory.
func tempfile() string {
	f, _ := ioutil.TempFile("", "raft-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}
