package raft_test

import (
	"bytes"
	"encoding/binary"
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
	check(t, func(commands [][]byte) bool {
		fmt.Print(".")

		var fsm TestFSM
		l := raft.NewLog()
		l.FSM = &fsm
		l.URL, _ = url.Parse("//node")
		l.Clock = raft.NewMockClock()
		l.Rand = seq()

		fsm.Log = l
		if err := l.Open(tempfile()); err != nil {
			log.Fatal("open: ", err)
		}
		defer os.RemoveAll(l.Path())
		defer l.Close()

		// Initialize log.
		go func() { l.Clock.Add(150 * time.Millisecond) }()
		if err := l.Initialize(); err != nil {
			t.Fatalf("initialize: %s", err)
		}
		if err := l.Wait(1); err != nil {
			t.Fatalf("wait: %s", err)
		}

		// Verify the configuration is set.
		if b, _ := json.Marshal(l.Config()); string(b) != `{"clusterID":1,"nodes":[{"id":1,"url":"//node"}],"index":1,"maxNodeID":1}` {
			t.Fatalf("unexpected config: %s", b)
		}

		// Execute a series of commands.
		var index uint64
		for _, command := range commands {
			var err error
			index, err = l.Apply(command)
			if err != nil {
				t.Fatalf("apply: %s", err)
			}
		}

		go func() { l.Clock.Add(2 * l.HeartbeatTimeout) }()
		l.Wait(index)

		// Verify the commands were executed against the FSM, in order.
		for i, command := range commands {
			if data := fsm.Commands[i]; !bytes.Equal(command, data) {
				t.Fatalf("%d. command:\n\nexp: %x\n\ngot: %x\n\n", i, command, data)
			}
		}

		go func() { l.Clock.Add(l.HeartbeatTimeout) }()
		return true
	})
}

// Ensure that a cluster of multiple nodes can maintain consensus.
func Test_Simulate_MultiNode(t *testing.T) {
	check(t, func(s *Simulation) bool {
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
			if _, err := leader.Log.Apply(command); err != nil {
				t.Fatalf("%d. apply: %s", i, err)
			}
		}

		// Allow entries to be sent to the followers.
		time.Sleep(10 * time.Millisecond)

		// Wait for one heartbeat to retrieve current index.
		// Wait for another heartbeat to send commit index.
		s.Clock.Add(raft.DefaultHeartbeatTimeout)
		s.Clock.Add(raft.DefaultHeartbeatTimeout)

		// Validate logs of all nodes.
		for i, n := range s.Nodes {
			if id := n.Log.ID(); uint64(i+1) != id {
				t.Fatalf("unexpected log id: exp=%d, got=%d", i+1, id)
			}
		}

		// Verify that all commands are present on all nodes.
		for _, n := range s.Nodes {
			n.Log.Wait(s.Nodes[0].FSM.MaxIndex)

			if entryN, commandN := len(n.FSM.Commands), len(s.Commands); commandN != entryN {
				t.Fatalf("unexpected entry count: node %d: exp=%d, got=%d", n.Log.ID(), commandN, entryN)
			}

			for i, command := range s.Commands {
				if !bytes.Equal(command, n.FSM.Commands[i]) {
					t.Fatalf("log mismatch: node %d, i=%d\n\nexp=%x\n\ngot=%x\n\n", n.Log.ID(), i, command, n.FSM.Commands[i])
				}
			}
		}

		return true
	})
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
	go func() { s.Clock.Add(raft.DefaultHeartbeatTimeout) }()
	if err := s.Nodes[0].Log.Initialize(); err != nil {
		return fmt.Errorf("node(0): initialize: %s", err)
	}

	// All other nodes should join the first node.
	for i, n := range s.Nodes[1:] {
		go func() { s.Clock.Add(raft.DefaultHeartbeatTimeout) }()
		if err := n.Log.Join(s.Nodes[0].Log.URL); err != nil {
			return fmt.Errorf("node(%d): join: %s", i, err)
		}
		n.Log.Wait(uint64(i))
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
	n.Log = raft.NewLog()
	n.Log.FSM = n.FSM
	n.Log.Clock = n.Clock
	n.Log.Rand = seq()
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

func check(t *testing.T, fn interface{}) {
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
	fmt.Println("")
}
