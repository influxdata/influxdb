package raft_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/influxdb/influxdb/raft"
)

// Ensure that opening an already open log returns an error.
func TestLog_Open_ErrOpen(t *testing.T) {
	n := NewInitNode()
	defer n.Close()
	if err := n.Log.Open(tempfile()); err != raft.ErrOpen {
		t.Fatal("expected error")
	}
}

// Ensure that a log can be checked for being open.
func TestLog_Opened(t *testing.T) {
	n := NewInitNode()
	if n.Log.Opened() != true {
		t.Fatalf("expected open")
	}
	n.Close()
	if n.Log.Opened() != false {
		t.Fatalf("expected closed")
	}
}

// Ensure that reopening an existing log will restore its ID.
func TestLog_Reopen(t *testing.T) {
	n := NewInitNode()
	if n.Log.ID() != 1 {
		t.Fatalf("expected id == 1")
	}
	path := n.Log.Path()

	// Close log and make sure id is cleared.
	n.Close()
	if n.Log.ID() != 0 {
		t.Fatalf("expected id == 0")
	}

	// Re-open and ensure id is restored.
	if err := n.Log.Open(path); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if n.Log.ID() != 1 {
		t.Fatalf("expected id == 1")
	}
	n.Close()
}

// Ensure that a single node-cluster can apply a log entry.
func TestLog_Apply(t *testing.T) {
	n := NewInitNode()
	defer n.Close()

	// Apply a command.
	index, err := n.Log.Apply([]byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if index != 2 {
		t.Fatalf("unexpected index: %d", index)
	}

	// Single node clusters should apply to FSM immediately.
	n.Log.Wait(index)
	if n := len(n.FSM().Commands); n != 1 {
		t.Fatalf("unexpected command count: %d", n)
	}
}

// Ensure that log ids are set sequentially.
func TestLog_ID_Sequential(t *testing.T) {
	c := NewCluster(3)
	defer c.Close()
	for i, n := range c.Nodes {
		if n.Log.ID() != uint64(i+1) {
			t.Fatalf("expected id: %d, got: %d", i+1, n.Log.ID())
		}
	}
}

// Ensure that cluster starts with one leader and multiple followers.
func TestLog_State(t *testing.T) {
	c := NewCluster(3)
	defer c.Close()
	if state := c.Nodes[0].Log.State(); state != raft.Leader {
		t.Fatalf("unexpected state(0): %s", state)
	}
	if state := c.Nodes[1].Log.State(); state != raft.Follower {
		t.Fatalf("unexpected state(1): %s", state)
	}
	if state := c.Nodes[2].Log.State(); state != raft.Follower {
		t.Fatalf("unexpected state(2): %s", state)
	}
}

// Ensure that a node has no configuration after it's closed.
func TestLog_Config_Closed(t *testing.T) {
	n := NewInitNode()
	n.Close()
	if n.Log.Config() != nil {
		t.Fatal("expected nil config")
	}
}

// Ensure that each node's configuration matches in the cluster.
func TestLog_Config(t *testing.T) {
	c := NewCluster(3)
	defer c.Close()
	config := jsonify(c.Nodes[0].Log.Config())
	for _, n := range c.Nodes[1:] {
		if b := jsonify(n.Log.Config()); config != b {
			t.Fatalf("config mismatch(%d):\n\nexp=%s\n\ngot:%s\n\n", n.Log.ID(), config, b)
		}
	}
}

// Ensure that a new log can be successfully opened and closed.
func TestLog_Apply_Cluster(t *testing.T) {
	c := NewCluster(3)
	defer c.Close()

	// Apply a command.
	leader := c.Nodes[0]
	index, err := leader.Log.Apply([]byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if index != 4 {
		t.Fatalf("unexpected index: %d", index)
	}
	leader.Log.Flush()

	// Should not apply immediately.
	if n := len(leader.FSM().Commands); n != 0 {
		t.Fatalf("unexpected pre-heartbeat command count: %d", n)
	}

	// Wait for a heartbeat and let the log apply the changes.
	// Only the leader should have the changes applied.
	c.Clock().Add(leader.Log.HeartbeatInterval)
	if n := len(c.Nodes[0].FSM().Commands); n != 1 {
		t.Fatalf("unexpected command count(0): %d", n)
	}
	if n := len(c.Nodes[1].FSM().Commands); n != 0 {
		t.Fatalf("unexpected command count(1): %d", n)
	}
	if n := len(c.Nodes[2].FSM().Commands); n != 0 {
		t.Fatalf("unexpected command count(2): %d", n)
	}

	// Wait for another heartbeat and all nodes should be in sync.
	c.Clock().Add(leader.Log.HeartbeatInterval)
	if n := len(c.Nodes[1].FSM().Commands); n != 1 {
		t.Fatalf("unexpected command count(1): %d", n)
	}
	if n := len(c.Nodes[2].FSM().Commands); n != 1 {
		t.Fatalf("unexpected command count(2): %d", n)
	}
}

// Ensure that a new leader can be elected.
func TestLog_Elect(t *testing.T) {
	c := NewCluster(3)
	defer c.Close()
	n0, n1, n2 := c.Nodes[0], c.Nodes[1], c.Nodes[2]

	// Stop leader.
	path := n0.Log.Path()
	n0.Log.Close()

	// Wait for election timeout.
	c.Clock().Add(2 * n0.Log.ElectionTimeout)

	// Ensure one node is elected in the next term.
	if s1, s2 := n1.Log.State(), n2.Log.State(); s1 != raft.Leader && s2 != raft.Leader {
		t.Fatalf("expected leader: n1=%s, n2=%s", s1, s2)
	}
	leader := c.Leader()
	if term := leader.Log.Term(); term != 2 {
		t.Fatalf("unexpected new term: %d", term)
	}

	// Restart leader and make sure it rejoins as a follower.
	if err := n0.Log.Open(path); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}

	// Wait for a heartbeat and verify the new leader is still the leader.
	c.Clock().Add(leader.Log.HeartbeatInterval)
	if state := leader.Log.State(); state != raft.Leader {
		t.Fatalf("new leader deposed: %s", state)
	}
	if term := n0.Log.Term(); term != 2 {
		t.Fatalf("invalid term: %d", term)
	}

	// Apply a command and ensure it's replicated.
	index, err := leader.Log.Apply([]byte("abc"))
	if err != nil {
		t.Fatalf("unexpected apply error: %s", err)
	}
	leader.Log.Flush()
	go func() { c.Clock().Add(2 * leader.Log.HeartbeatInterval) }()
	if err := leader.Log.Wait(index); err != nil {
		t.Fatalf("unexpected wait error: %s", err)
	}
}

// Ensure that state can be stringified.
func TestState_String(t *testing.T) {
	var tests = []struct {
		state raft.State
		s     string
	}{
		{raft.Stopped, "stopped"},
		{raft.Follower, "follower"},
		{raft.Candidate, "candidate"},
		{raft.Leader, "leader"},
		{raft.State(50), "unknown"},
	}
	for i, tt := range tests {
		if tt.state.String() != tt.s {
			t.Errorf("%d. mismatch: %s != %s", i, tt.state.String(), tt.s)
		}
	}
}

// Cluster represents a collection of nodes that share the same mock clock.
type Cluster struct {
	Nodes []*Node
}

// NewCluster creates a new cluster with an initial set of nodes.
func NewCluster(nodeN int) *Cluster {
	c := &Cluster{}
	for i := 0; i < nodeN; i++ {
		n := c.NewNode()
		n.Open()

		// Initialize the first node.
		// Join remaining nodes to the first node.
		if i == 0 {
			go func() { n.Clock().Add(2 * n.Log.ApplyInterval) }()
			if err := n.Log.Initialize(); err != nil {
				panic("initialize: " + err.Error())
			}
		} else {
			go func() { n.Clock().Add(n.Log.HeartbeatInterval) }()
			if err := n.Log.Join(c.Nodes[0].Log.URL); err != nil {
				panic("join: " + err.Error())
			}
		}
	}

	// Make sure everything is replicated to all followers.
	c.Nodes[0].Log.Flush()
	c.Clock().Add(c.Nodes[0].Log.HeartbeatInterval)

	return c
}

// Close closes all nodes in the cluster.
func (c *Cluster) Close() {
	for _, n := range c.Nodes {
		n.Close()
	}
}

// NewNode creates a new node on the cluster with the same clock.
func (c *Cluster) NewNode() *Node {
	n := NewNode()
	if len(c.Nodes) > 0 {
		n.Log.Clock = c.Nodes[0].Clock()
	}
	c.Nodes = append(c.Nodes, n)
	return n
}

// Clock returns the a clock that will slightly delay clock movement.
func (c *Cluster) Clock() raft.Clock { return &delayClock{c.Nodes[0].Log.Clock} }

// Leader returns the leader node with the highest term.
func (c *Cluster) Leader() *Node {
	var leader *Node
	for _, n := range c.Nodes {
		if n.Log.State() == raft.Leader && (leader == nil || leader.Log.Term() < n.Log.Term()) {
			leader = n
		}
	}
	return leader
}

// Node represents a log, FSM and associated HTTP server.
type Node struct {
	Log    *raft.Log
	Server *httptest.Server
}

// NewNode returns a new instance of Node.
func NewNode() *Node {
	n := &Node{Log: raft.NewLog()}
	n.Log.FSM = &FSM{}
	n.Log.Clock = raft.NewMockClock()
	n.Log.Rand = seq()
	if !testing.Verbose() {
		n.Log.Logger = log.New(ioutil.Discard, "", 0)
	}
	return n
}

// NewInitNode returns a new initialized Node.
func NewInitNode() *Node {
	n := NewNode()
	n.Open()
	go func() { n.Clock().Add(2 * n.Log.ApplyInterval) }()
	if err := n.Log.Initialize(); err != nil {
		panic("initialize: " + err.Error())
	}
	return n
}

// Open opens the log and HTTP server.
func (n *Node) Open() {
	// Start the HTTP server.
	n.Server = httptest.NewServer(raft.NewHTTPHandler(n.Log))
	n.Log.URL, _ = url.Parse(n.Server.URL)

	// Open the log.
	if err := n.Log.Open(tempfile()); err != nil {
		panic("open: " + err.Error())
	}
}

// Close closes the log and HTTP server.
func (n *Node) Close() error {
	defer func() { _ = os.RemoveAll(n.Log.Path()) }()
	_ = n.Log.Close()
	if n.Server != nil {
		n.Server.CloseClientConnections()
		n.Server.Close()
		n.Server = nil
	}
	return nil
}

// Clock returns the a clock that will slightly delay clock movement.
func (n *Node) Clock() raft.Clock { return &delayClock{n.Log.Clock} }

// FSM returns the state machine.
func (n *Node) FSM() *FSM { return n.Log.FSM.(*FSM) }

// delayClock represents a clock that adds a slight delay on clock movement.
// This ensures that clock movement doesn't occur too quickly.
type delayClock struct {
	raft.Clock
}

func (c *delayClock) Add(d time.Duration) {
	time.Sleep(10 * time.Millisecond)
	c.Clock.Add(d)
}

// FSM represents a simple state machine that records all commands.
type FSM struct {
	MaxIndex uint64
	Commands [][]byte
}

// Apply updates the max index and appends the command.
func (fsm *FSM) Apply(entry *raft.LogEntry) error {
	fsm.MaxIndex = entry.Index
	if entry.Type == raft.LogEntryCommand {
		fsm.Commands = append(fsm.Commands, entry.Data)
	}
	return nil
}

// Index returns the highest applied index.
func (fsm *FSM) Index() (uint64, error) { return fsm.MaxIndex, nil }

// Snapshot begins writing the FSM to a writer.
func (fsm *FSM) Snapshot(w io.Writer) (uint64, error) {
	b, _ := json.Marshal(fsm)
	binary.Write(w, binary.BigEndian, uint64(len(b)))
	_, err := w.Write(b)
	return fsm.MaxIndex, err
}

// Restore reads the snapshot from the reader.
func (fsm *FSM) Restore(r io.Reader) error {
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

// MockFSM represents a state machine that can be mocked out.
type MockFSM struct {
	ApplyFunc    func(*raft.LogEntry) error
	IndexFunc    func() (uint64, error)
	SnapshotFunc func(w io.Writer) (index uint64, err error)
	RestoreFunc  func(r io.Reader) error
}

func (fsm *MockFSM) Apply(e *raft.LogEntry) error         { return fsm.ApplyFunc(e) }
func (fsm *MockFSM) Index() (uint64, error)               { return fsm.IndexFunc() }
func (fsm *MockFSM) Snapshot(w io.Writer) (uint64, error) { return fsm.SnapshotFunc(w) }
func (fsm *MockFSM) Restore(r io.Reader) error            { return fsm.RestoreFunc(r) }

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

func jsonify(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
