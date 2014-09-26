package raft_test

import (
	"bytes"
	"fmt"
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

const (
	// MinSimulationInterval is the minimum time an AddTimeEvent can move the clock.
	MinSimulationInterval = 1 * time.Millisecond

	// MaxSimulationInterval is the maximum time an AddTimeEvent can move the clock.
	MaxSimulationInterval = 1 * time.Second
)

// Ensure that a cluster of multiple nodes can maintain consensus.
func Test_Simulate(t *testing.T) {
	var checkN int
	check(t, func(s *Simulation) bool {
		fmt.Printf("%04d |", checkN)

		// Initialize the cluster.
		if err := s.Initialize(); err != nil {
			t.Fatalf("initialize: %s", err)
		}

		// Execute events against the leader.
		for _, e := range s.Events {
			// Apply the event to the simulation.
			if err := e.Apply(s); err != nil {
				t.Fatal(err)
			}
			time.Sleep(1 * time.Millisecond)

			// Verify the simulation is in a correct state.
			if err := s.Verify(); err != nil {
				t.Fatal(err)
			}
		}
		fmt.Print("| ")

		// Move clock forward to resolve partitions.
		s.Clock.Add(1 * time.Second)

		// Validate logs of all nodes.
		for i, n := range s.Nodes {
			if n.Status != Running {
				continue
			}
			if id := n.Log.ID(); uint64(i+1) != id {
				t.Fatalf("unexpected log id: exp=%d, got=%d", i+1, id)
			}
		}

		// Verify that all commands are present on all nodes.
		for _, n := range s.Nodes {
			if n.Status != Running {
				continue
			}
			n.Log.Wait(s.Nodes[0].FSM.MaxIndex)
			fmt.Print(n.Log.ID())

			if entryN, commandN := len(n.FSM.Commands), len(s.Commands); commandN != entryN {
				t.Fatalf("unexpected entry count: node %d: exp=%d, got=%d", n.Log.ID(), commandN, entryN)
			}

			for i, command := range s.Commands {
				if !bytes.Equal(command, n.FSM.Commands[i]) {
					t.Fatalf("log mismatch: node %d, i=%d\n\nexp=%x\n\ngot=%x\n\n", n.Log.ID(), i, command, n.FSM.Commands[i])
				}
			}
		}

		fmt.Printf(" n=%d, cmd=%d\n", len(s.Nodes), len(s.Commands))
		checkN++
		s.Close()
		return true
	})
}

// Simulation represents a collection of nodes for simulating a raft cluster.
type Simulation struct {
	Nodes    []*SimulationNode
	Events   []SimulationEvent
	Clock    raft.Clock
	Commands [][]byte

	LeaderStopTime time.Time // last time the leader disconnected
	Error          error     // out-of-band error
}

// SetError sets an out-of-band error on the simulation.
// This can occur when an event will cause an error after time has elasped.
// Only the first error is stored.
func (s *Simulation) SetError(err error) {
	if s.Error == nil {
		s.Error = err
	}
}

// Verify checks if the simulation is in the correct state at the given time.
func (s *Simulation) Verify() error {
	// Check for out-of-band errors.
	if s.Error != nil {
		return s.Error
	}

	// TODO(simulation): Ensure one leader per term.

	// Ensure that we have a leader after an election timeout.
	if s.LeaderStopTime.IsZero() || s.Clock.Now().Sub(s.LeaderStopTime) > (raft.DefaultElectionTimeout*2) {
		if s.Leader() == nil {
			return fmt.Errorf("expected leadership after election timeout")
		}
	}

	return nil
}

// Leader returns the node which is the leader of the latest term.
func (s *Simulation) Leader() *SimulationNode {
	var node *SimulationNode
	for _, n := range s.Nodes {
		// Ignore joining nodes since they'll be locked.
		if n.Status == Joining {
			continue
		}

		if n.Log.State() == raft.Leader && (node == nil || node.Log.Term() < n.Log.Term()) {
			node = n
		}
	}
	return node
}

// Generate implements the testing/quick Generator interface.
func (s *Simulation) Generate(rand *rand.Rand, size int) reflect.Value {
	s = &Simulation{
		Clock:  raft.NewMockClock(),
		Events: make([]SimulationEvent, size),
		Nodes: []*SimulationNode{
			MustGenerateValue(reflect.TypeOf((*SimulationNode)(nil)), rand).Interface().(*SimulationNode),
		},
	}

	// Generate events.
	for i := range s.Events {
		n := rand.Intn(100)
		if n >= 0 && n < 70 {
			s.Events[i] = (*AddTimeEvent)(nil).Generate(rand, size).Interface().(SimulationEvent)
		} else if n >= 70 && n < 90 {
			s.Events[i] = (*ApplyEvent)(nil).Generate(rand, size).Interface().(SimulationEvent)
		} else if n >= 90 && n < 95 {
			s.Events[i] = (*AddPeerEvent)(nil).Generate(rand, size).Interface().(SimulationEvent)
		} else if n >= 95 && n < 100 {
			s.Events[i] = (*RestartEvent)(nil).Generate(rand, size).Interface().(SimulationEvent)
		} else {
			panic("unreachable")
		}
	}

	return reflect.ValueOf(s)
}

// Initialize initializes the first node's log and joins other nodes to the first.
func (s *Simulation) Initialize() error {
	n := s.Nodes[0]

	// Open initial log.
	n.Log.Clock = s.Clock
	if err := n.Log.Open(tempfile()); err != nil {
		return fmt.Errorf("open: %s", err)
	}

	// Start HTTP server and set log URL.
	n.HTTPServer = httptest.NewServer(raft.NewHTTPHandler(n.Log))
	n.Log.URL, _ = url.Parse(n.HTTPServer.URL)

	// Initialize the log of the first node.
	go func() {
		time.Sleep(10 * time.Millisecond)
		s.Clock.Add(raft.DefaultHeartbeatTimeout)
	}()
	if err := n.Log.Initialize(); err != nil {
		return fmt.Errorf("initialize: %s", err)
	}
	n.Status = Running

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

type NodeStatus int

const (
	Stopped NodeStatus = iota
	Joining
	Running
	Leaving
)

// SimulationNode represents a single node in the simulation.
type SimulationNode struct {
	Status     NodeStatus
	FSM        *TestFSM
	Log        *raft.Log
	Clock      raft.Clock
	HTTPServer *httptest.Server
}

// Generate implements the testing/quick Generator interface.
func (n *SimulationNode) Generate(rand *rand.Rand, size int) reflect.Value {
	n = &SimulationNode{Status: Stopped}
	n.FSM = &TestFSM{}

	// Create raft log.
	n.Log = raft.NewLog()
	n.Log.FSM = n.FSM
	n.Log.Clock = n.Clock
	n.Log.Rand = seq()
	n.FSM.Log = n.Log

	// Quiet log if we're not verbosely logging.
	if !testing.Verbose() {
		n.Log.Logger = log.New(ioutil.Discard, "", 0)
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

func MustGenerateValue(t reflect.Type, rand *rand.Rand) reflect.Value {
	v, ok := quick.Value(t, rand)
	if !ok {
		panic("testing/quick value error")
	}
	return v
}

// SimulationEvent represents an action that occurs during the simulation
type SimulationEvent interface {
	simulationEvent()
	Apply(*Simulation) error
}

func (_ *AddTimeEvent) simulationEvent() {}
func (_ *ApplyEvent) simulationEvent()   {}
func (_ *AddPeerEvent) simulationEvent() {}
func (_ *RestartEvent) simulationEvent() {}

// AddTimeEvent represents a simulation event where time is added to the clock.
type AddTimeEvent struct {
	Duration time.Duration
}

func (e *AddTimeEvent) Apply(s *Simulation) error {
	fmt.Print("➔")
	s.Clock.Add(e.Duration)
	return nil
}

// Generate implements the testing/quick Generator interface.
func (e *AddTimeEvent) Generate(rand *rand.Rand, size int) reflect.Value {
	//e.Duration = time.Duration(rand.Int63n(int64(MaxSimulationInterval-MinSimulationInterval))) + MinSimulationInterval
	return reflect.ValueOf(&AddTimeEvent{Duration: 10 * time.Millisecond})
}

// ApplyEvent represents a simulation event where a command is applied.
type ApplyEvent struct {
	Data []byte
}

func (e *ApplyEvent) Apply(s *Simulation) error {
	fmt.Print("•")

	// TODO(election): If there's no leader then drop command?

	// Write to leader.
	leader := s.Leader()
	if leader == nil {
		return nil
	}

	if _, err := leader.Log.Apply(e.Data); err != nil {
		return fmt.Errorf("apply: %s", err)
	}
	leader.Log.Flush()

	// Add to simulation's list of commands.
	s.Commands = append(s.Commands, e.Data)
	return nil
}

// Generate implements the testing/quick Generator interface.
func (e *ApplyEvent) Generate(rand *rand.Rand, size int) reflect.Value {
	e = &ApplyEvent{}
	e.Data = MustGenerateValue(reflect.TypeOf(e.Data), rand).Interface().([]byte)
	return reflect.ValueOf(e)
}

// AddPeerEvent represents a simulation event where a peer is created.
type AddPeerEvent struct {
	Node *SimulationNode
}

func (e *AddPeerEvent) Apply(s *Simulation) error {
	// Ignore if there is no leader.
	leader := s.Leader()
	if leader == nil {
		return nil
	}

	// Indicate event.
	fmt.Print("+")

	// Open log.
	e.Node.Log.Clock = s.Clock
	if err := e.Node.Log.Open(tempfile()); err != nil {
		return fmt.Errorf("open: %s", err)
	}

	// Start HTTP server and set log URL.
	e.Node.HTTPServer = httptest.NewServer(raft.NewHTTPHandler(e.Node.Log))
	e.Node.Log.URL, _ = url.Parse(e.Node.HTTPServer.URL)

	// Join to the leader.
	leaderURL := leader.Log.URL
	e.Node.Status = Joining
	go func() {
		fmt.Printf("X%d", len(s.Nodes))
		if err := e.Node.Log.Join(leaderURL); err != nil {
			s.SetError(fmt.Errorf("node: join: %s", err))
		}
		fmt.Printf("Y%d", len(s.Nodes))
		e.Node.Status = Running
	}()
	time.Sleep(10 * time.Millisecond)

	// Add to list of simulation nodes.
	s.Nodes = append(s.Nodes, e.Node)
	return nil
}

// Generate implements the testing/quick Generator interface.
func (e *AddPeerEvent) Generate(rand *rand.Rand, size int) reflect.Value {
	e = &AddPeerEvent{}
	e.Node = MustGenerateValue(reflect.TypeOf(e.Node), rand).Interface().(*SimulationNode)
	return reflect.ValueOf(e)
}

// RestartEvent represents a simulation event where a peer is stopped
// for a period of time and then restarted.
type RestartEvent struct {
	Index    int
	Duration time.Duration
	Node     *SimulationNode
}

func (e *RestartEvent) Apply(s *Simulation) error {
	// Select node to disconnect.
	index := e.Index % len(s.Nodes)
	e.Node = s.Nodes[index]

	// Ignore if the node is not actively running.
	if e.Node.Status != Running {
		return nil
	}

	// Set the leader stop time if this is the leader.
	if s.Leader() == e.Node {
		s.LeaderStopTime = s.Clock.Now()
	}

	// Close log on node.
	path := e.Node.Log.Path()
	if err := e.Node.Log.Close(); err != nil {
		return err
	}
	e.Node.Status = Stopped
	fmt.Printf("(▼%d %v)", index, e.Duration)

	// Re-open after the given duration.
	s.Clock.AfterFunc(e.Duration, func() {
		if err := e.Node.Log.Open(path); err != nil {
			s.SetError(err)
		}
		e.Node.Status = Running
		fmt.Printf("(▲%d)", index)
	})

	return nil
}

// Generate implements the testing/quick Generator interface.
func (e *RestartEvent) Generate(rand *rand.Rand, size int) reflect.Value {
	e = &RestartEvent{}
	e.Index = rand.Int()
	e.Duration = time.Duration(rand.Int()) % (500 * time.Millisecond)
	e.Duration = (e.Duration / time.Millisecond) * time.Millisecond
	return reflect.ValueOf(e)
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
