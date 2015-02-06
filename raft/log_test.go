package raft_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/influxdb/influxdb/raft"
)

// Ensure that opening an already open log returns an error.
func TestLog_Open_ErrOpen(t *testing.T) {
	l := NewInitializedLog(&url.URL{Host: "log0"})
	defer l.Close()
	if err := l.Open(tempfile()); err != raft.ErrOpen {
		t.Fatal("expected error")
	}
}

// Ensure that a log can be checked for being open.
func TestLog_Opened(t *testing.T) {
	l := NewInitializedLog(&url.URL{Host: "log0"})
	if l.Opened() != true {
		t.Fatalf("expected open")
	}
	l.Close()
	if l.Opened() != false {
		t.Fatalf("expected closed")
	}
}

// Ensure that reopening an existing log will restore its ID.
func TestLog_Reopen(t *testing.T) {
	l := NewInitializedLog(&url.URL{Host: "log0"})
	if l.ID() != 1 {
		t.Fatalf("expected id == 1")
	}
	path := l.Path()

	// Close log and make sure id is cleared.
	l.Log.Close()
	if l.ID() != 0 {
		t.Fatalf("expected id == 0")
	}

	// Re-open and ensure id is restored.
	if err := l.Open(path); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if id := l.ID(); id != 1 {
		t.Fatalf("unexpected id: %d", id)
	}
	l.Close()
}

// Ensure that a single node-cluster can apply a log entry.
func TestLog_Apply(t *testing.T) {
	l := NewInitializedLog(&url.URL{Host: "log0"})
	defer l.Close()

	// Apply a command.
	index, err := l.Apply([]byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if index != 2 {
		t.Fatalf("unexpected index: %d", index)
	}

	// Force apply cycle and then signal wait.
	go func() { l.Clock.apply() }()

	// Single node clusters should apply to FSM immediately.
	l.Wait(index)
	if n := len(l.FSM.Commands); n != 1 {
		t.Fatalf("unexpected command count: %d", n)
	}
}

// Ensure that a node has no configuration after it's closed.
func TestLog_Config_Closed(t *testing.T) {
	l := NewInitializedLog(&url.URL{Host: "log0"})
	defer l.Close()
	l.Log.Close()
	if l.Config() != nil {
		t.Fatal("expected nil config")
	}
}

// Ensure that log ids in a cluster are set sequentially.
func TestCluster_ID_Sequential(t *testing.T) {
	c := NewCluster()
	defer c.Close()
	for i, l := range c.Logs {
		if l.ID() != uint64(i+1) {
			t.Fatalf("expected id: %d, got: %d", i+1, l.ID())
		}
	}
}

// Ensure that cluster starts with one leader and multiple followers.
func TestCluster_State(t *testing.T) {
	c := NewCluster()
	defer c.Close()
	if state := c.Logs[0].State(); state != raft.Leader {
		t.Fatalf("unexpected state(0): %s", state)
	}
	if state := c.Logs[1].State(); state != raft.Follower {
		t.Fatalf("unexpected state(1): %s", state)
	}
	if state := c.Logs[2].State(); state != raft.Follower {
		t.Fatalf("unexpected state(2): %s", state)
	}
}

// Ensure that each node's configuration matches in the cluster.
func TestCluster_Config(t *testing.T) {
	c := NewCluster()
	defer c.Close()
	config := jsonify(c.Logs[0].Config())
	for _, l := range c.Logs[1:] {
		if b := jsonify(l.Config()); config != b {
			t.Fatalf("config mismatch(%d):\n\nexp=%s\n\ngot:%s\n\n", l.ID(), config, b)
		}
	}
}

// Ensure that a command can be applied to a cluster and distributed appropriately.
func TestCluster_Apply(t *testing.T) {
	c := NewCluster()
	defer c.Close()

	// Apply a command.
	leader := c.Logs[0]
	index, err := leader.Apply([]byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if index != 4 {
		t.Fatalf("unexpected index: %d", index)
	}
	c.Logs[1].MustWaitUncommitted(4)
	c.Logs[2].MustWaitUncommitted(4)

	// Should not apply immediately.
	if n := len(leader.FSM.Commands); n != 0 {
		t.Fatalf("unexpected pre-heartbeat command count: %d", n)
	}

	// Run the heartbeat on the leader and have all logs apply.
	// Only the leader should have the changes applied.
	c.Logs[0].Clock.heartbeat()
	c.Logs[0].Clock.apply()
	c.Logs[1].Clock.apply()
	c.Logs[2].Clock.apply()
	if n := len(c.Logs[0].FSM.Commands); n != 1 {
		t.Fatalf("unexpected command count(0): %d", n)
	}
	if n := len(c.Logs[1].FSM.Commands); n != 0 {
		t.Fatalf("unexpected command count(1): %d", n)
	}
	if n := len(c.Logs[2].FSM.Commands); n != 0 {
		t.Fatalf("unexpected command count(2): %d", n)
	}

	// Wait for another heartbeat and all logs should be in sync.
	c.Logs[0].Clock.heartbeat()
	c.Logs[1].Clock.apply()
	c.Logs[2].Clock.apply()
	if n := len(c.Logs[1].FSM.Commands); n != 1 {
		t.Fatalf("unexpected command count(1): %d", n)
	}
	if n := len(c.Logs[2].FSM.Commands); n != 1 {
		t.Fatalf("unexpected command count(2): %d", n)
	}
}

// Ensure that a new leader can be elected.
func TestLog_Elect(t *testing.T) {
	c := NewCluster()
	defer c.Close()

	// Stop leader.
	path := c.Logs[0].Path()
	c.Logs[0].Log.Close()

	// Signal election on node 1. Then heartbeat to establish leadership.
	c.Logs[1].Clock.now = c.Logs[1].Clock.now.Add(raft.DefaultElectionTimeout)
	c.Logs[1].Clock.election()
	c.Logs[1].Clock.heartbeat()

	// Ensure node 1 is elected in the next term.
	if state := c.Logs[1].State(); state != raft.Leader {
		t.Fatalf("expected node 1 to move to leader: %s", state)
	} else if term := c.Logs[1].Term(); term != 2 {
		t.Fatalf("expected term 2: got %d", term)
	}

	// Restart leader and make sure it rejoins as a follower.
	if err := c.Logs[0].Open(path); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}

	// Wait for a heartbeat and verify the node 1 is still the leader.
	c.Logs[1].Clock.heartbeat()
	if state := c.Logs[1].State(); state != raft.Leader {
		t.Fatalf("node 1 unexpectedly deposed: %s", state)
	} else if term := c.Logs[1].Term(); term != 2 {
		t.Fatalf("expected node 0 to go to term 2: got term %d", term)
	}

	// Apply a command and ensure it's replicated.
	index, err := c.Logs[1].Log.Apply([]byte("abc"))
	if err != nil {
		t.Fatalf("unexpected apply error: %s", err)
	}

	c.MustWaitUncommitted(index)
	c.Logs[1].Clock.heartbeat()
	c.Logs[1].Clock.heartbeat()
	c.Logs[0].Clock.apply()
	c.Logs[1].Clock.apply()
	c.Logs[2].Clock.apply()
	if err := c.Logs[0].Wait(index); err != nil {
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

func BenchmarkLogApply1(b *testing.B) { benchmarkLogApply(b, 1) }
func BenchmarkLogApply2(b *testing.B) { benchmarkLogApply(b, 2) }
func BenchmarkLogApply3(b *testing.B) { benchmarkLogApply(b, 3) }

// Benchmarks an n-node cluster connected through an in-memory transport.
func benchmarkLogApply(b *testing.B, logN int) {
	warnf("== BenchmarkLogApply (%d) ====================================", b.N)

	logs := make([]*raft.Log, logN)
	t := NewTransport()
	var ptrs []string
	for i := 0; i < logN; i++ {
		// Create log.
		l := raft.NewLog()
		l.URL = &url.URL{Host: fmt.Sprintf("log%d", i)}
		l.FSM = &BenchmarkFSM{}
		l.DebugEnabled = true
		l.Transport = t
		t.register(l)

		// Open log.
		if err := l.Open(tempfile()); err != nil {
			b.Fatalf("open: %s", err)
		}

		// Initialize or join.
		if i == 0 {
			if err := l.Initialize(); err != nil {
				b.Fatalf("initialize: %s", err)
			}
		} else {
			if err := l.Join(logs[0].URL); err != nil {
				b.Fatalf("initialize: %s", err)
			}
		}

		ptrs = append(ptrs, fmt.Sprintf("%d/%p", i, l))
		logs[i] = l
	}
	warn("LOGS:", strings.Join(ptrs, " "))
	b.ResetTimer()

	// Apply commands to leader.
	var index uint64
	var err error
	for i := 0; i < b.N; i++ {
		index, err = logs[0].Apply(make([]byte, 50))
		if err != nil {
			b.Fatalf("apply: %s", err)
		}
	}

	// Wait for all logs to catch up.
	for i, l := range logs {
		if err := l.Wait(index); err != nil {
			b.Fatalf("wait(%d): %s", i, err)
		}
	}
	b.StopTimer()

	// Verify FSM indicies match.
	for i, l := range logs {
		if fsm := l.FSM.(*BenchmarkFSM); index != fsm.index {
			b.Errorf("fsm index mismatch(%d): exp=%d, got=%d", i, index, fsm.index)
		}
	}
}

// BenchmarkFSM represents a state machine that records the command count.
type BenchmarkFSM struct {
	index uint64
}

// MustApply updates the index.
func (fsm *BenchmarkFSM) MustApply(entry *raft.LogEntry) { fsm.index = entry.Index }

// Index returns the highest applied index.
func (fsm *BenchmarkFSM) Index() (uint64, error) { return fsm.index, nil }

// Snapshot writes the FSM's index as the snapshot.
func (fsm *BenchmarkFSM) Snapshot(w io.Writer) (uint64, error) {
	return fsm.index, binary.Write(w, binary.BigEndian, fsm.index)
}

// Restore reads the snapshot from the reader.
func (fsm *BenchmarkFSM) Restore(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, &fsm.index)
}

// Cluster represents a collection of nodes that share the same mock clock.
type Cluster struct {
	Logs []*Log
}

// NewCluster creates a new 3 log cluster.
func NewCluster() *Cluster {
	c := &Cluster{}
	t := NewTransport()

	logN := 3
	for i := 0; i < logN; i++ {
		l := NewLog(&url.URL{Host: fmt.Sprintf("log%d", i)})
		l.Transport = t
		c.Logs = append(c.Logs, l)
		t.register(l.Log)
		warnf("Log %s: %p", l.URL.String(), l.Log)
	}
	warn("")

	// Initialize leader.
	c.Logs[0].MustOpen()
	c.Logs[0].MustInitialize()

	// Join second node.
	c.Logs[1].MustOpen()
	go func() {
		c.Logs[0].MustWaitUncommitted(2)
		c.Logs[0].Clock.apply()
	}()
	if err := c.Logs[1].Join(c.Logs[0].URL); err != nil {
		panic("join: " + err.Error())
	}
	c.Logs[0].Clock.heartbeat()
	c.Logs[1].MustWaitUncommitted(2)
	c.Logs[1].Clock.apply()
	c.Logs[0].Clock.heartbeat()

	// Join third node.
	c.Logs[2].MustOpen()
	go func() {
		c.Logs[0].MustWaitUncommitted(3)
		c.Logs[1].MustWaitUncommitted(3)
		c.Logs[0].Clock.heartbeat()
		c.Logs[0].Clock.apply()
		c.Logs[1].Clock.apply()
		c.Logs[2].Clock.apply()
	}()
	if err := c.Logs[2].Log.Join(c.Logs[0].Log.URL); err != nil {
		panic("join: " + err.Error())
	}

	// Heartbeart final commit index to all nodes and reapply.
	c.Logs[0].Clock.heartbeat()
	c.Logs[1].Clock.apply()
	c.Logs[2].Clock.apply()

	return c
}

// Close closes all logs in the cluster.
func (c *Cluster) Close() {
	for _, l := range c.Logs {
		l.Close()
	}
}

// Leader returns the leader log with the highest term.
func (c *Cluster) Leader() *Log {
	var leader *Log
	for _, l := range c.Logs {
		if l.State() == raft.Leader && (leader == nil || leader.Log.Term() < l.Term()) {
			leader = l
		}
	}
	return leader
}

// WaitUncommitted waits until all logs in the cluster have reached a given uncomiitted index.
func (c *Cluster) MustWaitUncommitted(index uint64) {
	for _, l := range c.Logs {
		l.MustWaitUncommitted(index)
	}
}

// flush issues messages to cycle all logs.
func (c *Cluster) flush() {
	for _, l := range c.Logs {
		l.Clock.heartbeat()
		l.Clock.apply()
	}
}

// Log represents a test log.
type Log struct {
	*raft.Log
	Clock *Clock
	FSM   *FSM
}

// NewLog returns a new instance of Log.
func NewLog(u *url.URL) *Log {
	l := &Log{Log: raft.NewLog(), Clock: NewClock(), FSM: &FSM{}}
	l.URL = u
	l.Log.FSM = l.FSM
	l.Log.Clock = l.Clock
	l.Rand = seq()
	l.DebugEnabled = true
	if !testing.Verbose() {
		l.Logger = log.New(ioutil.Discard, "", 0)
	}
	return l
}

// NewInitializedLog returns a new initialized Node.
func NewInitializedLog(u *url.URL) *Log {
	l := NewLog(u)
	l.MustOpen()
	l.MustInitialize()
	return l
}

// MustOpen opens the log. Panic on error.
func (l *Log) MustOpen() {
	if err := l.Open(tempfile()); err != nil {
		panic("open: " + err.Error())
	}
}

// MustInitialize initializes the log. Panic on error.
func (l *Log) MustInitialize() {
	go func() {
		l.MustWaitUncommitted(1)
		l.Clock.apply()
	}()
	if err := l.Initialize(); err != nil {
		panic("initialize: " + err.Error())
	}
}

// Close closes the log and HTTP server.
func (l *Log) Close() error {
	defer os.RemoveAll(l.Log.Path())
	_ = l.Log.Close()
	return nil
}

// MustWaitUncommitted waits for at least a given uncommitted index. Panic on error.
func (l *Log) MustWaitUncommitted(index uint64) {
	if err := l.Log.WaitUncommitted(index); err != nil {
		panic(l.URL.String() + " wait uncommitted: " + err.Error())
	}
}

// FSM represents a simple state machine that records all commands.
type FSM struct {
	MaxIndex uint64
	Commands [][]byte
}

// MustApply updates the max index and appends the command.
func (fsm *FSM) MustApply(entry *raft.LogEntry) {
	fsm.MaxIndex = entry.Index
	if entry.Type == raft.LogEntryCommand {
		fsm.Commands = append(fsm.Commands, entry.Data)
	}
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
	if err := json.Unmarshal(buf, &fsm); err != nil {
		return err
	}
	return nil
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

func warn(v ...interface{}) {
	if testing.Verbose() {
		fmt.Fprintln(os.Stderr, v...)
	}
}

func warnf(msg string, v ...interface{}) {
	if testing.Verbose() {
		fmt.Fprintf(os.Stderr, msg+"\n", v...)
	}
}
