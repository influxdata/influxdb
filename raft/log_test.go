package raft_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/influxdb/influxdb/raft"
)

// Ensure that a new log can be successfully opened and closed.
func TestLog_Open(t *testing.T) {
	l := NewUnopenedTestLog()
	if err := l.Open(tempfile()); err != nil {
		t.Fatal("open: ", err)
	} else if !l.Opened() {
		t.Fatal("expected log to be open")
	}
	if err := l.Close(); err != nil {
		t.Fatal("close: ", err)
	} else if l.Opened() {
		t.Fatal("expected log to be closed")
	}
}

// TestLog wraps the raft.Log to provide helper test functions.
type TestLog struct {
	*raft.Log
}

// NewTestLog returns a new, opened instance of TestLog.
func NewTestLog() *TestLog {
	l := NewUnopenedTestLog()
	if err := l.Open(tempfile()); err != nil {
		log.Fatalf("open: %s", err)
	}
	go func() { l.Clock.Add(l.ElectionTimeout) }()
	if err := l.Initialize(); err != nil {
		log.Fatalf("initialize: %s", err)
	}
	return l
}

// NewUnopenedTestLog returns a new, unopened instance of TestLog.
// The log uses mock clock by default.
func NewUnopenedTestLog() *TestLog {
	l := &TestLog{Log: raft.NewLog()}
	l.Log.FSM = &TestFSM{}
	l.URL, _ = url.Parse("//node")
	l.Log.Clock = raft.NewMockClock()
	l.Log.Rand = seq()
	return l
}

// Close closes the log and removes the underlying data.
func (t *TestLog) Close() error {
	defer os.RemoveAll(t.Path())
	return t.Log.Close()
}

// TestFSM represents a fake state machine that simple records all commands.
type TestFSM struct {
	Log      *raft.Log `json:"-"`
	MaxIndex uint64
	Commands [][]byte
}

func (fsm *TestFSM) Apply(entry *raft.LogEntry) error {
	fsm.MaxIndex = entry.Index
	if entry.Type == raft.LogEntryCommand {
		fsm.Commands = append(fsm.Commands, entry.Data)
	}
	return nil
}

func (fsm *TestFSM) Index() (uint64, error) { return fsm.MaxIndex, nil }
func (fsm *TestFSM) Snapshot(w io.Writer) (uint64, error) {
	b, _ := json.Marshal(fsm)
	binary.Write(w, binary.BigEndian, uint64(len(b)))
	_, err := w.Write(b)
	return fsm.MaxIndex, err
}
func (fsm *TestFSM) Restore(r io.Reader) error {
	var sz uint64
	if err := binary.Read(r, binary.BigEndian, &sz); err != nil {
		return err
	}
	buf := make([]byte, sz)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return json.Unmarshal(buf, &fsm)
}

// BufferCloser represents a bytes.Buffer that provides a no-op close.
type BufferCloser struct {
	*bytes.Buffer
}

func (b *BufferCloser) Close() error { return nil }

// seq implements the raft.Log#Rand interface and returns incrementing ints.
func seq() func() int64 {
	var i int64
	var mu sync.Mutex

	return func() int64 {
		mu.Lock()
		defer mu.Unlock()

		i++
		return i
	}
}

// tempfile returns the path to a non-existent file in the temp directory.
func tempfile() string {
	f, _ := ioutil.TempFile("", "raft-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
