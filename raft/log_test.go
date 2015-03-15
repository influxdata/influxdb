package raft_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/influxdb/influxdb/raft"
)

// Ensure that opening an already open log returns an error.
func TestLog_Open_ErrOpen(t *testing.T) {
	l := NewInitializedLog(url.URL{Host: "log0"})
	defer l.Close()
	if err := l.Open(tempfile()); err != raft.ErrOpen {
		t.Fatal("expected error")
	}
}

// Ensure that a log can be checked for being open.
func TestLog_Opened(t *testing.T) {
	l := NewInitializedLog(url.URL{Host: "log0"})
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
	l := NewInitializedLog(url.URL{Host: "log0"})
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
	l := NewInitializedLog(url.URL{Host: "log0"})
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
	if n := len(l.FSM.(*FSM).Commands); n != 1 {
		t.Fatalf("unexpected command count: %d", n)
	}
}

// Ensure that a node has no configuration after it's closed.
func TestLog_Config_Closed(t *testing.T) {
	l := NewInitializedLog(url.URL{Host: "log0"})
	defer l.Close()
	l.Log.Close()
	if l.Config() != nil {
		t.Fatal("expected nil config")
	}
}

// Ensure that log ids in a cluster are set sequentially.
func TestCluster_ID_Sequential(t *testing.T) {
	c := NewCluster(fsmFunc)
	defer c.Close()
	for i, l := range c.Logs {
		if l.ID() != uint64(i+1) {
			t.Fatalf("expected id: %d, got: %d", i+1, l.ID())
		}
	}
}

// Ensure that cluster starts with one leader and multiple followers.
func TestCluster_State(t *testing.T) {
	c := NewCluster(fsmFunc)
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
	c := NewCluster(fsmFunc)
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
	c := NewCluster(fsmFunc)
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
	if n := len(leader.FSM.(*FSM).Commands); n != 0 {
		t.Fatalf("unexpected pre-heartbeat command count: %d", n)
	}

	// Run the heartbeat on the leader and have all logs apply.
	// Only the leader should have the changes applied.
	c.Logs[0].HeartbeatUntil(4)
	c.Logs[0].Clock.apply()
	c.Logs[1].Clock.apply()
	c.Logs[2].Clock.apply()
	if n := len(c.Logs[0].FSM.(*FSM).Commands); n != 1 {
		t.Fatalf("unexpected command count(0): %d", n)
	}
	if n := len(c.Logs[1].FSM.(*FSM).Commands); n != 1 {
		t.Fatalf("unexpected command count(1): %d", n)
	}
	if n := len(c.Logs[2].FSM.(*FSM).Commands); n != 1 {
		t.Fatalf("unexpected command count(2): %d", n)
	}
}

// Ensure that a new leader can be elected.
func TestLog_Elect(t *testing.T) {
	c := NewCluster(fsmFunc)
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
	c.Logs[1].HeartbeatUntil(index)
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

// Ensure a cluster of nodes can successfully re-elect while applying commands.
func TestCluster_Elect_RealTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skip: short mode")
	}

	// Create a cluster with a real-time clock.
	c := NewRealTimeCluster(3, indexFSMFunc)
	minIndex := c.Logs[0].AppliedIndex()
	commandN := uint64(1000) - minIndex

	// Run a loop to continually apply commands.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); i < commandN; i++ {
			index, err := c.Apply(make([]byte, 50))
			if err != nil {
				t.Fatalf("apply: index=%d, err=%s", index, err)
			}
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		}
	}()

	// Run a loop to periodically kill off nodes.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for nodes to get going.
		time.Sleep(500 * time.Millisecond)

		// Choose random log.
		// i := rand.Intn(len(c.Logs))
		i := 0
		l := c.Logs[i]

		// Restart the log.
		path := l.Path()
		l.Log.Close()
		if err := l.Log.Open(path); err != nil {
			t.Fatalf("reopen(%d): %s", i, err)
		}
	}()

	// Wait for all logs to catch up.
	wg.Wait()
	for i, l := range c.Logs {
		if err := l.Wait(commandN + minIndex); err != nil {
			t.Errorf("wait(%d): %s", i, err)
		}
	}

	// Verify FSM indicies match.
	for i, l := range c.Logs {
		fsmIndex, _ := l.FSM.(*raft.IndexFSM).Index()
		if exp := commandN + minIndex; exp != fsmIndex {
			t.Errorf("fsm index mismatch(%d): exp=%d, got=%d", i, exp, fsmIndex)
		}
	}
}

func BenchmarkClusterApply1(b *testing.B) { benchmarkClusterApply(b, 1) }
func BenchmarkClusterApply2(b *testing.B) { benchmarkClusterApply(b, 2) }
func BenchmarkClusterApply3(b *testing.B) { benchmarkClusterApply(b, 3) }

// Benchmarks an n-node cluster connected through an in-memory transport.
func benchmarkClusterApply(b *testing.B, logN int) {
	warnf("== BenchmarkClusterApply (%d) ====================================", b.N)

	c := NewRealTimeCluster(logN, indexFSMFunc)
	defer c.Close()
	b.ResetTimer()

	// Apply commands to leader.
	var index uint64
	var err error
	for i := 0; i < b.N; i++ {
		index, err = c.Apply(make([]byte, 50))
		if err != nil {
			b.Fatalf("apply: %s", err)
		}
	}

	// Wait for all logs to catch up.
	for _, l := range c.Logs {
		l.MustWait(index)
	}
	b.StopTimer()

	// Verify FSM indicies match.
	for i, l := range c.Logs {
		fsmIndex, _ := l.FSM.(*raft.IndexFSM).Index()
		if index != fsmIndex {
			b.Errorf("fsm index mismatch(%d): exp=%d, got=%d", i, index, fsmIndex)
		}
	}
}

// Cluster represents a collection of nodes that share the same mock clock.
type Cluster struct {
	Logs []*Log
}

// NewCluster creates a new 3 log cluster.
func NewCluster(fsmFn func() raft.FSM) *Cluster {
	c := &Cluster{}
	t := NewTransport()

	logN := 3
	for i := 0; i < logN; i++ {
		l := NewLog(url.URL{Host: fmt.Sprintf("log%d", i)})
		l.Log.FSM = fsmFn()
		l.Transport = t
		c.Logs = append(c.Logs, l)
		t.register(l.Log)
		u := l.URL()
		warnf("Log %s: %p", u.String(), l.Log)
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
	if err := c.Logs[1].Join(c.Logs[0].URL()); err != nil {
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
		c.Logs[0].HeartbeatUntil(3)
		c.Logs[0].Clock.apply()
		c.Logs[1].Clock.apply()
		c.Logs[2].Clock.apply()
	}()
	if err := c.Logs[2].Log.Join(c.Logs[0].Log.URL()); err != nil {
		panic("join: " + err.Error())
	}

	// Heartbeart final commit index to all nodes and reapply.
	c.Logs[0].Clock.heartbeat()
	c.Logs[1].Clock.apply()
	c.Logs[2].Clock.apply()

	return c
}

// NewRealTimeCluster a new cluster with n logs.
// All logs use a real-time clock instead of a test clock.
func NewRealTimeCluster(logN int, fsmFn func() raft.FSM) *Cluster {
	c := &Cluster{}
	t := NewTransport()

	for i := 0; i < logN; i++ {
		l := NewLog(url.URL{Host: fmt.Sprintf("log%d", i)})
		l.Log.FSM = fsmFn()
		l.Clock = nil
		l.Log.Clock = raft.NewClock()
		l.Transport = t
		c.Logs = append(c.Logs, l)
		t.register(l.Log)
		u := l.URL()
		warnf("Log %s: %p", u.String(), l.Log)
	}
	warn("")

	// Initialize leader.
	c.Logs[0].MustOpen()
	c.Logs[0].MustInitialize()

	// Join remaining nodes.
	for i := 1; i < logN; i++ {
		c.Logs[i].MustOpen()
		c.Logs[i].MustJoin(c.Logs[0].URL())
	}

	// Ensure nodes are ready.
	index, _ := c.Logs[0].LastLogIndexTerm()
	for i := 0; i < logN; i++ {
		c.Logs[i].MustWait(index)
	}

	return c
}

// Close closes all logs in the cluster.
func (c *Cluster) Close() {
	for _, l := range c.Logs {
		l.Close()
	}
}

// Apply continually tries to apply data to the current leader.
// If the leader cannot be found then it will retry until it finds the leader.
func (c *Cluster) Apply(data []byte) (uint64, error) {
	for {
		for _, l := range c.Logs {
			index, err := l.Apply(make([]byte, 50))
			if err == raft.ErrNotLeader {
				continue
			}
			return index, err
		}
		time.Sleep(1 * time.Millisecond)
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

// WaitCommitted waits until all logs in the cluster have reached a given uncommitted index.
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
}

// NewLog returns a new instance of Log.
func NewLog(u url.URL) *Log {
	l := &Log{Log: raft.NewLog(), Clock: NewClock()}
	l.SetURL(u)
	l.Log.Clock = l.Clock
	l.Rand = seq()
	l.DebugEnabled = true
	if !testing.Verbose() {
		l.Logger = log.New(ioutil.Discard, "", 0)
	}
	return l
}

// NewInitializedLog returns a new initialized Node.
func NewInitializedLog(u url.URL) *Log {
	l := NewLog(u)
	l.Log.FSM = &FSM{}
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
		if l.Clock != nil {
			l.Clock.apply()
		}
	}()
	if err := l.Initialize(); err != nil {
		panic("initialize: " + err.Error())
	}
}

// MustJoin joins the log to another log. Panic on error.
func (l *Log) MustJoin(u url.URL) {
	if err := l.Join(u); err != nil {
		panic("join: " + err.Error())
	}
}

// Close closes the log and HTTP server.
func (l *Log) Close() error {
	defer os.RemoveAll(l.Log.Path())
	_ = l.Log.Close()
	return nil
}

// MustWaits waits for at least a given applied index. Panic on error.
func (l *Log) MustWait(index uint64) {
	if err := l.Log.Wait(index); err != nil {
		u := l.URL()
		panic(u.String() + " wait: " + err.Error())
	}
}

// MustCommitted waits for at least a given committed index. Panic on error.
func (l *Log) MustWaitCommitted(index uint64) {
	if err := l.Log.WaitCommitted(index); err != nil {
		u := l.URL()
		panic(u.String() + " wait committed: " + err.Error())
	}
}

// MustWaitUncommitted waits for at least a given uncommitted index. Panic on error.
func (l *Log) MustWaitUncommitted(index uint64) {
	if err := l.Log.WaitUncommitted(index); err != nil {
		u := l.URL()
		panic(u.String() + " wait uncommitted: " + err.Error())
	}
}

// HeartbeatUtil continues to heartbeat until an index is committed.
func (l *Log) HeartbeatUntil(index uint64) {
	for {
		time.Sleep(1 * time.Millisecond)
		l.Clock.heartbeat()
		if l.CommitIndex() >= index {
			return
		}
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

func fsmFunc() raft.FSM      { return &FSM{} }
func indexFSMFunc() raft.FSM { return &raft.IndexFSM{} }

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

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 converts an 8-byte slice into an uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
