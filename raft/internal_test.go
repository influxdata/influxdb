package raft

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"sync"
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
	mu    sync.Mutex
	index uint64
}

// MustApply updates the index.
func (fsm *IndexFSM) Apply(entry *LogEntry) error {
	fsm.mu.Lock()
	fsm.index = entry.Index
	fsm.mu.Unlock()
	return nil
}

// Index returns the highest applied index.
func (fsm *IndexFSM) Index() uint64 {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	return fsm.index
}

// WriteTo writes a snapshot of the FSM to w.
func (fsm *IndexFSM) WriteTo(w io.Writer) (n int64, err error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	return 0, binary.Write(w, binary.BigEndian, fsm.index)
}

// ReadFrom reads an FSM snapshot from r.
func (fsm *IndexFSM) ReadFrom(r io.Reader) (n int64, err error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	return 0, binary.Read(r, binary.BigEndian, &fsm.index)
}

// tempfile returns the path to a non-existent file in the temp directory.
func tempfile() string {
	f, _ := ioutil.TempFile("", "raft-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}
