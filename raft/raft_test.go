package raft_test

import (
	"bytes"
	"encoding/json"
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
	"time"

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
			Clock: raft.NewMockClock(),
			Rand:  seq(),
		}
		fsm.Log = l
		l.URL, _ = url.Parse("//node")
		if err := l.Open(tempfile()); err != nil {
			log.Fatal("open: ", err)
		}
		defer os.RemoveAll(l.Path())
		defer l.Close()

		// Initialize log.
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
		if b, _ := json.Marshal(l.Config()); string(b) != `{"clusterID":1,"nodes":[{"id":1,"url":"//node"}],"index":1,"maxNodeID":1}` {
			t.Fatalf("unexpected config: %s", b)
		}

		// Verify the commands were executed against the FSM, in order.
		for i, command := range commands {
			if e := fsm.entries[i]; !bytes.Equal(command, e.Data) {
				t.Fatalf("%d. command:\n\nexp: %x\n\ngot: %x\n\n", i, command, e.Data)
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
		fmt.Print(".")
		defer s.Close()

		// Retrieve leader.
		leader := s.Nodes[0]

		// Initialize the cluster.
		if err := s.Initialize(); err != nil {
			t.Fatalf("initialize: %s", err)
		}

		// Validate log identifiers.
		for i, n := range s.Nodes {
			if id := n.Log.ID(); uint64(i+1) != id {
				t.Fatalf("unexpected log id: exp=%d, got=%d", i+1, id)
			}
		}

		// Apply commands to the leader.
		for i, command := range s.Commands {
			go func() { s.Clock.Add(150 * time.Millisecond) }()
			if err := leader.Log.Apply(command); err != nil {
				t.Fatalf("%d. apply: %s", i, err)
			}
		}

		// Wait for commands to apply.
		s.Clock.Add(500 * time.Millisecond)
		time.Sleep(1 * time.Second)

		// Validate logs of all nodes.
		for i, n := range s.Nodes {
			if id := n.Log.ID(); uint64(i+1) != id {
				t.Fatalf("unexpected log id: exp=%d, got=%d", i+1, id)
			}
		}

		// Verify that all entries are present on all nodes.
		for _, n := range s.Nodes {
			if entryN, commandN := len(n.FSM.entries), len(s.Commands); commandN != entryN {
				t.Fatalf("unexpected entry count: node %d: exp=%d, got=%d", n.Log.ID(), commandN, entryN)
			}

			for i, command := range s.Commands {
				if !bytes.Equal(command, n.FSM.entries[i].Data) {
					t.Fatalf("log mismatch: node %d, i=%d\n\nexp=%x\n\ngot=%x\n\n", n.Log.ID(), i, command, n.FSM.entries[i].Data)
				}
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
	Log     *raft.Log
	entries []*raft.LogEntry
}

func (fsm *TestFSM) Apply(entry *raft.LogEntry) error {
	fsm.entries = append(fsm.entries, entry)
	return nil
}

func (fsm *TestFSM) Snapshot(w io.Writer) error { return nil }
func (fsm *TestFSM) Restore(r io.Reader) error  { return nil }

// Simulation represents a collection of nodes for simulating a raft cluster.
type Simulation struct {
	Nodes    []*SimulationNode
	Clock    raft.Clock
	Commands [][]byte
}

// Generate implements the testing/quick Generator interface.
func (s *Simulation) Generate(rand *rand.Rand, size int) reflect.Value {
	s = &Simulation{
		Clock: raft.NewMockClock(),
	}

	// Generate commands.
	s.Commands = GenerateValue(reflect.TypeOf(s.Commands), rand).([][]byte)

	// Create between 1 and 9 nodes.
	nodeN := 2 // rand.Intn(8) + 1
	for i := 0; i < nodeN; i++ {
		var n SimulationNode
		n.Clock = s.Clock
		n.Generate(rand, size)
		s.Nodes = append(s.Nodes, &n)
	}
	return reflect.ValueOf(s)
}

// Initialize initializes the first node's log and joins other nodes to the first.
func (s *Simulation) Initialize() error {
	// Initialize the log of the first node.
	go func() { s.Clock.Add(100 * time.Millisecond) }()
	if err := s.Nodes[0].Log.Initialize(); err != nil {
		return fmt.Errorf("node(0): initialize: %s", err)
	}

	// All other nodes should join the first node.
	for i, n := range s.Nodes[1:] {
		go func() { s.Clock.Add(100 * time.Millisecond) }()
		if err := n.Log.Join(s.Nodes[0].Log.URL); err != nil {
			return fmt.Errorf("node(%d): join: %s", i, err)
		}
	}

	return nil
}

// Close closes all the logs and servers.
func (s *Simulation) Close() error {
	// Close nodes in reverse order.
	for i := len(s.Nodes) - 1; i >= 0; i-- {
		_ = s.Nodes[i].Close()
	}
	return nil
}

// SimulationNode represents a single node in the simulation.
type SimulationNode struct {
	FSM        *TestFSM
	Log        *raft.Log
	Clock      raft.Clock
	HTTPServer *httptest.Server
}

// Generate implements the testing/quick Generator interface.
func (n *SimulationNode) Generate(rand *rand.Rand, size int) reflect.Value {
	n.FSM = &TestFSM{}

	// Create raft log.
	n.Log = &raft.Log{
		FSM:   n.FSM,
		Clock: n.Clock,
		Rand:  seq(),
	}
	n.FSM.Log = n.Log

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
	n.HTTPServer.CloseClientConnections()
	n.HTTPServer.Close()
	return nil
}

func GenerateValue(t reflect.Type, rand *rand.Rand) interface{} {
	v, ok := quick.Value(t, rand)
	if !ok {
		panic("testing/quick value error")
	}
	return v.Interface()
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
