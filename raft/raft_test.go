package raft_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
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
			Rand:  seq(),
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
		if fsm.config != `{"clusterID":2,"nodes":[{"id":1,"url":"//node"}]}` {
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

// Ensure that a cluster of multiple nodes can maintain consensus.
func Test_Simulate_MultiNode(t *testing.T) {
	f := func(s *Simulation) bool {
		defer s.Close()
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

// Simulation represents a collection of nodes for simulating a raft cluster.
type Simulation struct {
	Nodes    []*SimulationNode
	Clock    clock.Clock
	Commands [][]byte
}

// Generate implements the testing/quick Generator interface.
func (s *Simulation) Generate(rand *rand.Rand, size int) reflect.Value {
	s = &Simulation{}

	// Create between 1 and 9 nodes.
	nodeN := rand.Intn(8) + 1
	for i := 0; i < nodeN; i++ {
		var n SimulationNode
		n.Generate(rand, size)
		s.Nodes = append(s.Nodes, &n)
	}
	return reflect.ValueOf(s)
}

// Initialize initializes the first node's log and joins other nodes to the first.
func (s *Simulation) Initialize() error {
	// Initialize the log of the first node.
	if err := s.Nodes[0].Log.Initialize(); err != nil {
		return fmt.Errorf("node(0): %s", err)
	}

	// All other nodes should join the first node.
	for i := 1; i < len(s.Nodes); i++ {
		// TODO(benbjohnson): Join first node.
	}

	return nil
}

// Close closes all the logs and servers.
func (s *Simulation) Close() error {
	for _, n := range s.Nodes {
		_ = n.Close()
	}
	return nil
}

// SimulationNode represents a single node in the simulation.
type SimulationNode struct {
	FSM        *TestFSM
	Log        *raft.Log
	HTTPServer *httptest.Server
}

// Generate implements the testing/quick Generator interface.
func (n *SimulationNode) Generate(rand *rand.Rand, size int) reflect.Value {
	// Create raft log.
	n.Log = &raft.Log{
		FSM:  &TestFSM{},
		Rand: seq(),
	}

	// Start HTTP server and set log URL.
	n.HTTPServer = httptest.NewServer(raft.NewHTTPHandler(n.Log))
	n.Log.URL, _ = url.Parse(n.HTTPServer.URL)

	// Open log.
	if err := n.Log.Open(tempfile()); err != nil {
		log.Fatalf("open: %s", err)
	}

	return reflect.ValueOf(n)
}

// Close closes the log and HTTP server.
func (n *SimulationNode) Close() error {
	defer func() { _ = os.RemoveAll(n.Log.Path()) }()
	_ = n.Log.Close()
	n.HTTPServer.Close()
	return nil
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
