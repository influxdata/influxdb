package raft_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"testing"
	"testing/quick"

	"github.com/benbjohnson/clock"
	"github.com/influxdb/influxdb/raft"
)

func init() {
	log.SetFlags(0)
}

// Ensure that a single node can process commands applied to the log.
func Test_Simulate_SingleNode(t *testing.T) {
	f := func(commands [][]byte) bool {
		var fsm TestFSM
		l := &raft.Log{
			FSM:   &fsm,
			Clock: clock.NewMockClock(),
			Rand:  nopRand,
		}
		l.URL, _ = url.Parse("//node")
		if err := l.Open(tempfile()); err != nil {
			log.Fatal("open: ", err)
		}
		defer os.RemoveAll(l.Path())
		defer l.Close()

		// HACK(benbjohnson): Initialize instead.
		if err := l.Initialize(); err != nil {
			t.Fatalf("initialize: %s", err)
		}

		// Execute a series of commands.
		for _, command := range commands {
			if err := l.Apply(command); err != nil {
				t.Fatalf("apply: %s", err)
			}
		}

		// Verify the configuration is set.
		if fsm.config != `{"clusterID":"00000000","peers":["//node"]}` {
			t.Fatalf("unexpected config: %s", fsm.config)
		}

		// Verify the commands were executed against the FSM, in order.
		for i, command := range commands {
			if b := fsm.commands[i]; !bytes.Equal(command, b) {
				t.Fatalf("%d. command:\n\nexp: %x\n\n got:%x\n\n", i, command, b)
			}
		}

		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// TestFSM represents a fake state machine that simple records all commands.
type TestFSM struct {
	config   string
	commands [][]byte
}

func (fsm *TestFSM) Apply(entry *raft.LogEntry) error {
	switch entry.Type {
	case raft.LogEntryCommand:
		fsm.commands = append(fsm.commands, entry.Data)
	case raft.LogEntryConfig:
		fsm.config = string(entry.Data)
	default:
		panic("unknown entry type")
	}
	return nil
}

func (fsm *TestFSM) Snapshot(w io.Writer) error { return nil }
func (fsm *TestFSM) Restore(r io.Reader) error  { return nil }

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
