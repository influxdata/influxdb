package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FSM represents the state machine that the log is applied to.
// The FSM must maintain the highest index that it has seen.
type FSM interface {
	// Executes a log entry against the state machine.
	// Non-repeatable errors such as system and disk errors must panic.
	MustApply(*LogEntry)

	// Returns the highest index saved to the state machine.
	Index() (uint64, error)

	// Writes a snapshot of the entire state machine to a writer.
	// Returns the index at the point in time of the snapshot.
	Snapshot(w io.Writer) (index uint64, err error)

	// Reads a snapshot of the entire state machine.
	Restore(r io.Reader) error
}

const logEntryHeaderSize = 8 + 8 + 8 // sz+index+term

// WaitInterval represents the amount of time between checks to the applied index.
// This is used by clients wanting to wait until a given index is processed.
const WaitInterval = 1 * time.Millisecond

// State represents whether the log is a follower, candidate, or leader.
type State int

// String returns the string representation of the state.
func (s State) String() string {
	switch s {
	case Stopped:
		return "stopped"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

const (
	Stopped State = iota
	Follower
	Candidate
	Leader
)

// Log represents a replicated log of commands.
type Log struct {
	mu sync.Mutex

	id     uint64  // log identifier
	path   string  // data directory
	config *Config // cluster configuration

	state      State          // current node state
	heartbeats chan heartbeat // incoming heartbeat channel
	terms      chan uint64    // incoming channel of newer terms

	lastLogTerm  uint64 // highest term in the log
	lastLogIndex uint64 // highest index in the log

	term        uint64    // current election term
	leaderID    uint64    // the current leader
	votedFor    uint64    // candidate voted for in current election term
	lastContact time.Time // last contact from the leader

	commitIndex  uint64 // highest entry to be committed
	appliedIndex uint64 // highest entry to applied to state machine

	reader  io.ReadCloser // incoming stream from leader
	writers []*logWriter  // outgoing streams to followers

	entries []*LogEntry

	wg      sync.WaitGroup // pending goroutines
	closing chan struct{}  // close notification

	// Network address to the reach the log.
	url url.URL

	// The state machine that log entries will be applied to.
	FSM FSM

	// The transport used to communicate with other nodes in the cluster.
	Transport interface {
		Join(u url.URL, nodeURL url.URL) (id uint64, leaderID uint64, config *Config, err error)
		Leave(u url.URL, id uint64) error
		Heartbeat(u url.URL, term, commitIndex, leaderID uint64) (lastIndex uint64, err error)
		ReadFrom(u url.URL, id, term, index uint64) (io.ReadCloser, error)
		RequestVote(u url.URL, term, candidateID, lastLogIndex, lastLogTerm uint64) error
	}

	// Clock is an abstraction of time.
	Clock interface {
		Now() time.Time
		AfterApplyInterval() <-chan chan struct{}
		AfterElectionTimeout() <-chan chan struct{}
		AfterHeartbeatInterval() <-chan chan struct{}
		AfterReconnectTimeout() <-chan chan struct{}
	}

	// Rand returns a random number.
	Rand func() int64

	// Sets whether trace messages are logged.
	DebugEnabled bool

	// This logs some asynchronous errors that occur within the log.
	Logger *log.Logger
}

// NewLog creates a new instance of Log with reasonable defaults.
func NewLog() *Log {
	l := &Log{
		Clock:      NewClock(),
		Transport:  &HTTPTransport{},
		Rand:       rand.NewSource(time.Now().UnixNano()).Int63,
		heartbeats: make(chan heartbeat, 1),
		terms:      make(chan uint64, 1),
	}
	l.SetLogOutput(os.Stderr)
	return l
}

// Path returns the data path of the Raft log.
// Returns an empty string if the log is closed.
func (l *Log) Path() string { return l.path }

// URL returns the URL for the log.
func (l *Log) URL() url.URL {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.url
}

// SetURL sets the URL for the log. This must be set before opening.
func (l *Log) SetURL(u url.URL) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.opened() {
		panic("url cannot be set while log is open")
	}

	l.url = u
}

// URLs returns a list of all URLs in the cluster.
func (l *Log) URLs() []url.URL {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.config == nil {
		return nil
	}

	var a []url.URL
	for _, n := range l.config.Nodes {
		a = append(a, n.URL)
	}

	return a
}

func (l *Log) idPath() string     { return filepath.Join(l.path, "id") }
func (l *Log) termPath() string   { return filepath.Join(l.path, "term") }
func (l *Log) configPath() string { return filepath.Join(l.path, "config") }

// Opened returns true if the log is currently open.
func (l *Log) Opened() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.opened()
}

func (l *Log) opened() bool { return l.path != "" }

// ID returns the log's identifier.
func (l *Log) ID() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.id
}

// State returns the current state.
func (l *Log) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// LastLogIndexTerm returns the last index & term from the log.
func (l *Log) LastLogIndexTerm() (index, term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastLogIndex, l.lastLogTerm
}

// CommtIndex returns the highest committed index.
func (l *Log) CommitIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.commitIndex
}

// AppliedIndex returns the highest applied index.
func (l *Log) AppliedIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appliedIndex
}

// Term returns the current term.
func (l *Log) Term() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.term
}

// Config returns a the log's current configuration.
func (l *Log) Config() *Config {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.config != nil {
		return l.config.Clone()
	}
	return nil
}

// Open initializes the log from a path.
// If the path does not exist then it is created.
func (l *Log) Open(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Validate initial log state.
	if l.opened() {
		return ErrOpen
	}

	// Create directory, if not exists.
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	l.path = path

	// Initialize log identifier.
	id, err := l.readID()
	if err != nil {
		_ = l.close()
		return err
	}
	l.setID(id)

	// Initialize log term.
	term, err := l.readTerm()
	if err != nil {
		_ = l.close()
		return err
	}
	l.term = term
	l.votedFor = 0
	l.lastLogTerm = term

	// Read config.
	c, err := l.readConfig()
	if err != nil {
		_ = l.close()
		return err
	}
	l.config = c

	// Determine last applied index from FSM.
	index, err := l.FSM.Index()
	if err != nil {
		return err
	}
	l.tracef("Open: fsm: index=%d", index)
	l.lastLogIndex = index
	l.appliedIndex = index
	l.commitIndex = index

	// Start goroutine to apply logs.
	l.wg.Add(1)
	l.closing = make(chan struct{})
	go l.applier(l.closing)

	// If a log exists then start the state loop.
	if c != nil {
		l.Logger.Printf("log open: created at %s, with ID %d, term %d, last applied index of %d",
			path, l.id, l.term, l.lastLogIndex)

		// If the config only has one node then start it as the leader.
		// Otherwise start as a follower.
		if len(c.Nodes) == 1 && c.Nodes[0].ID == l.id {
			l.Logger.Println("log open: promoting to leader immediately")
			l.startStateLoop(l.closing, Leader)
		} else {
			l.startStateLoop(l.closing, Follower)
		}
	} else {
		l.Logger.Printf("log pending: waiting for initialization or join")
	}

	return nil
}

// Close closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.close()
}

// close should be called under lock.
func (l *Log) close() error {
	l.tracef("closing...")

	// Close the reader, if open.
	l.closeReader()

	// Notify goroutines of closing and wait outside of lock.
	if l.closing != nil {
		close(l.closing)
		l.closing = nil
		l.mu.Unlock()
		l.wg.Wait()
		l.mu.Lock()
	}

	// Close the writers.
	for _, w := range l.writers {
		_ = w.Close()
	}
	l.writers = nil

	// Clear log info.
	l.setID(0)
	l.path = ""
	l.lastLogIndex, l.lastLogTerm = 0, 0
	l.term, l.votedFor = 0, 0
	l.config = nil

	l.tracef("closed")

	return nil
}

func (l *Log) closeReader() {
	if l.reader != nil {
		_ = l.reader.Close()
		l.reader = nil
	}
}

func (l *Log) setID(id uint64) {
	l.id = id
	l.updateLogPrefix()
}

// readID reads the log identifier from file.
func (l *Log) readID() (uint64, error) {
	// Read identifier from disk.
	b, err := ioutil.ReadFile(l.idPath())
	if os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	// Parse identifier.
	id, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// writeID writes the log identifier to file.
func (l *Log) writeID(id uint64) error {
	b := []byte(strconv.FormatUint(id, 10))
	return ioutil.WriteFile(l.idPath(), b, 0666)
}

// readTerm reads the log term from file.
func (l *Log) readTerm() (uint64, error) {
	// Read term from disk.
	b, err := ioutil.ReadFile(l.termPath())
	if os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	// Parse term.
	id, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// writeTerm writes the current log term to file.
func (l *Log) writeTerm(term uint64) error {
	b := []byte(strconv.FormatUint(term, 10))
	return ioutil.WriteFile(l.termPath(), b, 0666)
}

// readConfig reads the configuration from disk.
func (l *Log) readConfig() (*Config, error) {
	// Read config from disk.
	f, err := os.Open(l.configPath())
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	// Marshal file to a config type.
	var config *Config
	if f != nil {
		config = &Config{}
		if err := NewConfigDecoder(f).Decode(config); err != nil {
			return nil, err
		}
	}
	return config, nil
}

// writeConfig writes the configuration to disk.
func (l *Log) writeConfig(config *Config) error {
	// FIX(benbjohnson): Atomic write.

	// Open file.
	f, err := os.Create(l.configPath())
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Marshal config into file.
	if err := NewConfigEncoder(f).Encode(config); err != nil {
		return err
	}

	return nil
}

// Initialize a new log.
// Returns an error if log data already exists.
func (l *Log) Initialize() error {
	var config *Config
	err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()

		// Return error if log is not open or is already a member of a cluster.
		if !l.opened() {
			return ErrClosed
		} else if l.id != 0 {
			return ErrInitialized
		}

		// Start first node at id 1.
		id := uint64(1)

		// Generate a new configuration with one node.
		config = &Config{MaxNodeID: id}
		config.AddNode(id, l.url)

		// Generate new 8-hex digit cluster identifier.
		config.ClusterID = uint64(l.Rand())

		// Generate log id.
		if err := l.writeID(id); err != nil {
			return err
		}
		l.setID(id)

		// Automatically promote to leader.
		term := uint64(1)
		if err := l.writeTerm(term); err != nil {
			return fmt.Errorf("write term: %s", err)
		}
		l.term = term
		l.votedFor = 0
		l.lastLogTerm = term
		l.leaderID = l.id

		// Begin state loop as leader.
		l.startStateLoop(l.closing, Leader)

		l.Logger.Printf("log initialize: promoted to 'leader' with cluster ID %d, log ID %d, term %d",
			config.ClusterID, l.id, l.term)

		return nil
	}()
	if err != nil {
		return err
	}

	// Set initial configuration.
	var buf bytes.Buffer
	_ = NewConfigEncoder(&buf).Encode(config)
	index, err := l.internalApply(LogEntryInitialize, buf.Bytes())
	if err != nil {
		return err
	}

	// Wait until entry is applied.
	return l.Wait(index)
}

// SetLogOutput sets writer for all Raft output.
func (l *Log) SetLogOutput(w io.Writer) {
	l.Logger = log.New(w, "", log.LstdFlags)
	l.updateLogPrefix()
}

func (l *Log) updateLogPrefix() {
	var host string
	if l.url.Host != "" {
		host = l.url.Host
	}
	l.Logger.SetPrefix(fmt.Sprintf("[raft] %s ", host))
}

// trace writes a log message if DebugEnabled is true.
func (l *Log) trace(v ...interface{}) {
	if l.DebugEnabled {
		l.Logger.Print(v...)
	}
}

// trace writes a formatted log message if DebugEnabled is true.
func (l *Log) tracef(msg string, v ...interface{}) {
	if l.DebugEnabled {
		l.Logger.Printf(msg+"\n", v...)
	}
}

// IsLeader returns true if the log is the current leader.
func (l *Log) IsLeader() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.id != 0 && l.id == l.leaderID
}

// Leader returns the id and URL associated with the current leader.
// Returns zero if there is no current leader.
func (l *Log) Leader() (id uint64, u url.URL) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leader()
}

func (l *Log) leader() (id uint64, u url.URL) {
	// Ignore if there's no configuration set.
	if l.config == nil {
		return
	}

	// Find node by identifier.
	n := l.config.NodeByID(l.leaderID)
	if n == nil {
		return
	}

	return n.ID, n.URL
}

// ClusterID returns the identifier for the cluster.
// Returns zero if the cluster has not been initialized yet.
func (l *Log) ClusterID() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.config == nil {
		return 0
	}
	return l.config.ClusterID
}

// Join contacts a node in the cluster to request membership.
// A log cannot join a cluster if it has already been initialized.
func (l *Log) Join(u url.URL) error {
	// Validate under lock.
	var nodeURL url.URL
	if err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()

		if !l.opened() {
			return ErrClosed
		} else if l.id != 0 {
			return ErrInitialized
		} else if l.url.Host == "" {
			return ErrURLRequired
		}

		nodeURL = l.url
		return nil
	}(); err != nil {
		return err
	}

	l.tracef("Join: %s", u)

	// Send join request.
	id, leaderID, config, err := l.Transport.Join(u, nodeURL)
	if err != nil {
		return err
	}
	l.leaderID = leaderID

	l.tracef("Join: confirmed")

	// Lock once the join request is returned.
	l.mu.Lock()
	defer l.mu.Unlock()

	// Write identifier.
	if err := l.writeID(id); err != nil {
		return err
	}
	l.setID(id)

	// Write config.
	if err := l.writeConfig(config); err != nil {
		return err
	}
	l.config = config

	// Begin state loop as follower.
	l.startStateLoop(l.closing, Follower)

	// Change to a follower state.
	l.Logger.Println("log join: entered 'follower' state for cluster at", u, " with log ID", l.id)

	return nil
}

// Leave removes the log from cluster membership and removes the log data.
func (l *Log) Leave() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// TODO(benbjohnson): Check if open.
	// TODO(benbjohnson): Apply remove peer command.
	// TODO(benbjohnson): Remove underlying data.

	return nil
}

// startStateLoop begins the state loop in a separate goroutine.
// Returns once the state has transitioned to the initial state passed in.
func (l *Log) startStateLoop(closing <-chan struct{}, state State) {
	l.wg.Add(1)
	stateChanged := make(chan struct{})
	go l.stateLoop(closing, state, stateChanged)

	// Wait until state change.
	<-stateChanged
}

// stateLoop runs in a separate goroutine and runs the appropriate state loop.
func (l *Log) stateLoop(closing <-chan struct{}, state State, stateChanged chan struct{}) {
	defer l.wg.Done()

	l.Logger.Printf("log state change: %s => %s", l.state, state)
	l.state = state
	close(stateChanged)

	for {
		// Transition to new state.
		l.Logger.Printf("log state change: %s => %s", l.state, state)
		l.state = state

		// Remove previous reader, if one exists.
		if l.reader != nil {
			_ = l.reader.Close()
		}

		// Execute the appropriate state loop.
		// Each loop returns the next state to transition to.
		switch state {
		case Stopped:
			return
		case Follower:
			state = l.followerLoop(closing)
		case Candidate:
			state = l.candidateLoop(closing)
		case Leader:
			state = l.leaderLoop(closing)
		}
	}
}

func (l *Log) followerLoop(closing <-chan struct{}) State {
	l.tracef("followerLoop")
	defer l.tracef("followerLoop: exit")

	// Ensure all follower goroutines complete before transitioning to another state.
	var wg sync.WaitGroup
	defer wg.Wait()
	var transitioning = make(chan struct{})
	defer close(transitioning)

	// Read log from leader in a separate goroutine.
	wg.Add(1)
	go l.readFromLeader(&wg, transitioning)

	for {
		select {
		case <-closing:
			return Stopped
		case ch := <-l.Clock.AfterElectionTimeout():
			l.closeReader()
			close(ch)
			return Candidate
		case hb := <-l.heartbeats:
			l.tracef("followerLoop: heartbeat: term=%d, idx=%d", hb.term, hb.commitIndex)

			// Update term, commit index & leader.
			l.mu.Lock()
			if hb.term > l.term {
				l.term = hb.term
				l.votedFor = 0
			}
			if hb.commitIndex > l.commitIndex {
				l.commitIndex = hb.commitIndex
			}
			l.leaderID = hb.leaderID
			l.mu.Unlock()
		}
	}
}

func (l *Log) readFromLeader(wg *sync.WaitGroup, transitioning <-chan struct{}) {
	defer wg.Done()
	l.tracef("readFromLeader:")

	for {
		select {
		case <-transitioning:
			l.tracef("readFromLeader: exiting")
			return
		default:
		}

		// Retrieve the term, last commit index, & leader URL.
		l.mu.Lock()
		id, commitIndex, term := l.id, l.commitIndex, l.term
		_, u := l.leader()
		l.mu.Unlock()

		// If no leader exists then wait momentarily and retry.
		if u.Host == "" {
			l.tracef("readFromLeader: no leader")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Connect to leader.
		l.tracef("readFromLeader: read from: %s, id=%d, term=%d, index=%d", u.String(), id, term, commitIndex)
		r, err := l.Transport.ReadFrom(u, id, term, commitIndex)
		if err != nil {
			l.Logger.Printf("connect stream: %s", err)
		}

		// Attach the stream to the log.
		if err := l.ReadFrom(r); err != nil {
			l.tracef("readFromLeader: read from: disconnect: %s", err)
		}
	}
}

// truncate removes all uncommitted entries.
func (l *Log) truncate() {
	if len(l.entries) == 0 {
		return
	}

	entmin := l.entries[0].Index
	assert(l.commitIndex >= entmin, "commit index before lowest entry: commit=%d, entmin=%d", l.commitIndex, entmin)
	l.entries = l.entries[:l.commitIndex-entmin]
	l.lastLogIndex = l.commitIndex
}

// candidateLoop requests vote from other nodes in an attempt to become leader.
func (l *Log) candidateLoop(closing <-chan struct{}) State {
	l.tracef("candidateLoop")
	defer l.tracef("candidateLoop: exit")

	// TODO: prevote

	// Increment term and request votes.
	l.mu.Lock()
	l.term++
	l.votedFor = l.id
	term := l.term
	l.mu.Unlock()

	// Ensure all candidate goroutines complete before transitioning to another state.
	var wg sync.WaitGroup
	defer wg.Wait()
	var transitioning = make(chan struct{})
	defer close(transitioning)

	// Read log from leader in a separate goroutine.
	wg.Add(1)
	elected := make(chan struct{}, 1)
	go l.elect(term, elected, &wg, transitioning)

	for {
		select {
		case <-closing:
			return Stopped
		case hb := <-l.heartbeats:
			l.mu.Lock()
			if hb.term >= l.term {
				l.term = hb.term
				l.votedFor = 0
				l.leaderID = hb.leaderID
				l.mu.Unlock()
				return Follower
			}
			l.mu.Unlock()
		case <-elected:
			return Leader
		case ch := <-l.Clock.AfterElectionTimeout():
			close(ch)
			return Follower
		}
	}
}

func (l *Log) elect(term uint64, elected chan struct{}, wg *sync.WaitGroup, transitioning <-chan struct{}) {
	defer wg.Done()

	// Ensure we are in the same term and copy properties.
	l.mu.Lock()
	if term != l.term {
		l.mu.Unlock()
		return
	}
	id, config := l.id, l.config
	lastLogIndex, lastLogTerm := l.lastLogIndex, l.lastLogTerm
	l.mu.Unlock()

	// Request votes from peers.
	votes := make(chan struct{}, len(config.Nodes))
	for _, n := range config.Nodes {
		if n.ID == id {
			continue
		}
		go func(n *ConfigNode) {
			if err := l.Transport.RequestVote(n.URL, term, id, lastLogIndex, lastLogTerm); err != nil {
				l.tracef("sendVoteRequests: %s: %s", n.URL.String(), err)
				return
			}
			votes <- struct{}{}
		}(n)
	}

	// Wait until we have a quorum before responding.
	voteN := 1
	for {
		// Signal channel that the log has been elected.
		if voteN >= (len(config.Nodes)/2)+1 {
			elected <- struct{}{}
			return
		}

		// Wait until log transitions to another state or we receive a vote.
		select {
		case <-transitioning:
			return
		case <-votes:
			voteN++
		}
	}
}

// leaderLoop periodically sends heartbeats to all followers to maintain dominance.
func (l *Log) leaderLoop(closing <-chan struct{}) State {
	l.tracef("leaderLoop")
	defer l.tracef("leaderLoop: exit")

	// Ensure all leader goroutines complete before transitioning to another state.
	var wg sync.WaitGroup
	defer wg.Wait()
	var transitioning = make(chan struct{})
	defer close(transitioning)

	// Retrieve leader's term.
	l.mu.Lock()
	term := l.term
	l.mu.Unlock()

	// Read log from leader in a separate goroutine.
	for {
		// Send hearbeat to followers.
		wg.Add(1)
		committed := make(chan uint64, 1)
		go l.heartbeater(term, committed, &wg, transitioning)

		// Wait for close, new leader, or new heartbeat response.
		select {
		case <-closing: // wait for state change.
			return Stopped

		case newTerm := <-l.terms: // step down on higher term
			if newTerm > term {
				l.mu.Lock()
				l.term = newTerm
				l.truncate()
				l.mu.Unlock()
				return Follower
			}
			continue

		case hb := <-l.heartbeats: // step down on higher term
			if hb.term > term {
				l.mu.Lock()
				l.term = hb.term
				l.truncate()
				l.mu.Unlock()
				return Follower
			}
			continue

		case commitIndex, ok := <-committed:
			// Quorum not reached, try again.
			if !ok {
				continue
			}

			// Quorum reached, set new commit index.
			l.mu.Lock()
			if commitIndex > l.commitIndex {
				l.tracef("leaderLoop: committed: idx=%d", commitIndex)
				l.commitIndex = commitIndex
			}
			l.mu.Unlock()
			continue
		}
	}
}

// heartbeater continuoally sends heartbeats to all peers.
func (l *Log) heartbeater(term uint64, committed chan uint64, wg *sync.WaitGroup, transitioning <-chan struct{}) {
	defer wg.Done()

	// Ensure term is correct and retrieve current state.
	l.mu.Lock()
	if l.term != term {
		l.mu.Unlock()
		return
	}
	commitIndex, localIndex, leaderID, config := l.commitIndex, l.lastLogIndex, l.id, l.config
	l.mu.Unlock()

	// Commit latest index if there are no peers.
	if config == nil || len(config.Nodes) <= 1 {
		time.Sleep(10 * time.Millisecond)
		committed <- localIndex
		return
	}

	l.tracef("heartbeater: start: n=%d", len(config.Nodes))

	// Send heartbeats to all peers.
	peerIndices := make(chan uint64, len(config.Nodes))
	for _, n := range config.Nodes {
		if n.ID == leaderID {
			continue
		}
		go func(n *ConfigNode) {
			//l.tracef("heartbeater: send: url=%s, term=%d, commit=%d, leaderID=%d", n.URL, term, commitIndex, leaderID)
			peerIndex, err := l.Transport.Heartbeat(n.URL, term, commitIndex, leaderID)
			//l.tracef("heartbeater: recv: url=%s, peerIndex=%d, err=%s", n.URL, peerIndex, err)
			if err != nil {
				l.Logger.Printf("heartbeater: error: %s", err)
				return
			}
			peerIndices <- peerIndex
		}(n)
	}

	// Wait for heartbeat responses or timeout.
	after := l.Clock.AfterHeartbeatInterval()
	indexes := make([]uint64, 1, len(config.Nodes))
	indexes[0] = localIndex
	for {
		select {
		case <-transitioning:
			l.tracef("heartbeater: transitioning")
			return
		case peerIndex := <-peerIndices:
			l.tracef("heartbeater: index: idx=%d, idxs=%+v", peerIndex, indexes)
			indexes = append(indexes, peerIndex) // collect responses
		case ch := <-after:
			// Once we have enough indices then return the lowest index
			// among the highest quorum of nodes.
			quorumN := (len(config.Nodes) / 2) + 1
			if len(indexes) >= quorumN {
				// Return highest index reported by quorum.
				sort.Sort(sort.Reverse(uint64Slice(indexes)))
				committed <- indexes[quorumN-1]
				l.tracef("heartbeater: commit: idx=%d, idxs=%+v", commitIndex, indexes)
			} else {
				l.tracef("heartbeater: no quorum: idxs=%+v", indexes)
				close(committed)
			}
			close(ch)
			return
		}
	}
}

type heartbeatResponse struct {
	peerTerm  uint64
	peerIndex uint64
}

// check looks if the channel has any messages.
// If it does then errDone is returned, otherwise nil is returned.
func check(done chan struct{}) error {
	select {
	case <-done:
		return errDone
	default:
		return nil
	}
}

// Apply executes a command against the log.
// This function returns once the command has been committed to the log.
func (l *Log) Apply(command []byte) (uint64, error) {
	return l.internalApply(LogEntryCommand, command)
}

func (l *Log) internalApply(typ LogEntryType, command []byte) (index uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Do not apply if this node is closed.
	// Do not apply if this node is not the leader.
	if l.state == Stopped {
		return 0, ErrClosed
	} else if l.state != Leader {
		return 0, ErrNotLeader
	}

	// Create log entry.
	e := LogEntry{
		Type:  typ,
		Index: l.lastLogIndex + 1,
		Term:  l.term,
		Data:  command,
	}
	index = e.Index

	// Append to the log.
	l.append(&e)

	// If there is no config or only one node then move commit index forward.
	if l.config == nil || len(l.config.Nodes) <= 1 {
		l.commitIndex = l.lastLogIndex
	}

	return
}

// Wait blocks until a given index is applied.
func (l *Log) Wait(index uint64) error {
	// TODO(benbjohnson): Check for leadership change (?).
	// TODO(benbjohnson): Add timeout.

	for {
		l.mu.Lock()
		state, appliedIndex := l.state, l.appliedIndex
		l.mu.Unlock()

		if state == Stopped {
			return ErrClosed
		} else if appliedIndex >= index {
			return nil
		}
		time.Sleep(WaitInterval)
	}
}

// waitCommitted blocks until a given committed index is reached.
func (l *Log) waitCommitted(index uint64) error {
	for {
		l.mu.Lock()
		state, committedIndex := l.state, l.commitIndex
		l.mu.Unlock()

		if state == Stopped {
			return ErrClosed
		} else if committedIndex >= index {
			return nil
		}
		time.Sleep(WaitInterval)
	}
}

// waitUncommitted blocks until a given uncommitted index is reached.
func (l *Log) waitUncommitted(index uint64) error {
	for {
		l.mu.Lock()
		lastLogIndex := l.lastLogIndex
		//l.tracef("waitUncommitted: %s / %d", l.state, l.lastLogIndex)
		l.mu.Unlock()

		if lastLogIndex >= index {
			return nil
		}
		time.Sleep(WaitInterval)
	}
}

// append adds a log entry to the list of entries.
func (l *Log) append(e *LogEntry) {
	//l.tracef("append: idx=%d, prev=%d", e.Index, l.lastLogIndex)
	assert(e.Index == l.lastLogIndex+1, "non-contiguous log index(%d): idx=%d, prev=%d", l.id, e.Index, l.lastLogIndex)

	// Encode entry to a byte slice.
	buf := make([]byte, logEntryHeaderSize+len(e.Data))
	copy(buf, e.encodedHeader())
	copy(buf[logEntryHeaderSize:], e.Data)

	// Add to pending entries list to wait to be applied.
	l.entries = append(l.entries, e)
	l.lastLogIndex = e.Index
	l.lastLogTerm = e.Term

	// Write to tailing writers.
	for i := 0; i < len(l.writers); i++ {
		w := l.writers[i]

		// If an error occurs then remove the writer and close it.
		if _, err := w.Write(buf); err != nil {
			l.removeWriter(w)
			i--
			continue
		}

		// Flush, if possible.
		flushWriter(w.Writer)
	}
}

// applier runs in a separate goroutine and applies all entries between the
// previously applied index and the current commit index.
func (l *Log) applier(closing <-chan struct{}) {
	defer l.wg.Done()

	for {
		// Wait for a close signal or timeout.
		var confirm chan struct{}
		select {
		case <-closing:
			return

		case confirm = <-l.Clock.AfterApplyInterval():
		}

		//l.tracef("applier")

		// Apply all entries committed since the previous apply.
		err := func() error {
			l.mu.Lock()
			defer l.mu.Unlock()

			// Verify, under lock, that we're not closing.
			select {
			case <-closing:
				return nil
			default:
			}

			// Ignore if there are no pending entries.
			// Ignore if all entries are applied.
			if len(l.entries) == 0 {
				//l.tracef("applier: no entries")
				return nil
			} else if l.appliedIndex == l.commitIndex {
				//l.tracef("applier: up to date")
				return nil
			}

			// Determine the available range of indices on the log.
			entmin, entmax := l.entries[0].Index, l.entries[len(l.entries)-1].Index
			assert(entmin <= entmax, "apply: log out of order: min=%d, max=%d", entmin, entmax)
			assert(uint64(len(l.entries)) == (entmax-entmin+1), "apply: missing entries: min=%d, max=%d, len=%d", entmin, entmax, len(l.entries))

			// Determine the range of indices that should be processed.
			// This should be the entry after the last applied index through to
			// the committed index.
			nextUnappliedIndex, commitIndex := l.appliedIndex+1, l.commitIndex
			l.tracef("applier: entries: available=%d-%d, [next,commit]=%d-%d", entmin, entmax, nextUnappliedIndex, commitIndex)
			assert(nextUnappliedIndex <= commitIndex, "next unapplied index after commit index: next=%d, commit=%d", nextUnappliedIndex, commitIndex)

			// Determine the lowest index to start from.
			// This should be the next entry after the last applied entry.
			// Ignore if we don't have any entries after the last applied yet.
			assert(entmin <= nextUnappliedIndex, "apply: missing entries: min=%d, next=%d", entmin, nextUnappliedIndex)
			if nextUnappliedIndex > entmax {
				return nil
			}
			imin := nextUnappliedIndex

			// Determine the highest index to go to.
			// This should be the committed index.
			// If we haven't yet received the committed index then go to the last available.
			var imax uint64
			if commitIndex <= entmax {
				imax = commitIndex
			} else {
				imax = entmax
			}

			// Determine entries to apply.
			l.tracef("applier: entries: available=%d-%d, applying=%d-%d", entmin, entmax, imin, imax)
			entries := l.entries[imin-entmin : imax-entmin+1]

			// Determine low water mark for entries to cut off.
			for _, w := range l.writers {
				if w.snapshotIndex > 0 && w.snapshotIndex < imax {
					imax = w.snapshotIndex
				}
			}
			l.entries = l.entries[imax-entmin:]

			// Iterate over each entry and apply it.
			for _, e := range entries {
				// l.tracef("applier: entry: idx=%d", e.Index)

				switch e.Type {
				case LogEntryCommand, LogEntryNop:
				case LogEntryInitialize:
					l.mustApplyInitialize(e)
				case LogEntryAddPeer:
					l.mustApplyAddPeer(e)
				case LogEntryRemovePeer:
					l.mustApplyRemovePeer(e)
				default:
					panic("unsupported command type: " + strconv.Itoa(int(e.Type)))
				}

				// Apply to FSM.
				if e.Index > 0 {
					l.FSM.MustApply(e)
				}

				// Increment applied index.
				l.appliedIndex++
			}

			return nil
		}()

		// If error occurred then log it.
		// The log will retry after a given timeout.
		if err != nil {
			l.Logger.Printf("apply error: %s", err)
			// TODO(benbjohnson): Longer timeout before retry?
		}

		// Signal clock that apply is done.
		close(confirm)
	}
}

// mustApplyInitialize a log initialization command by parsing and setting the configuration.
func (l *Log) mustApplyInitialize(e *LogEntry) {
	// Parse the configuration from the log entry.
	config := &Config{}
	if err := NewConfigDecoder(bytes.NewReader(e.Data)).Decode(config); err != nil {
		panic("decode: " + err.Error())
	}

	// Set the last update index on the configuration.
	config.Index = e.Index

	// TODO(benbjohnson): Lock the log while we update the configuration.

	// Perist the configuration to disk.
	if err := l.writeConfig(config); err != nil {
		panic("write config: " + err.Error())
	}
	l.config = config
}

// mustApplyAddPeer adds a node to the cluster configuration.
func (l *Log) mustApplyAddPeer(e *LogEntry) {
	// Unmarshal node from entry data.
	var n *ConfigNode
	if err := json.Unmarshal(e.Data, &n); err != nil {
		panic("unmarshal: " + err.Error())
	}

	// Clone configuration.
	config := l.config.Clone()

	// Increment the node identifier.
	config.MaxNodeID++
	n.ID = config.MaxNodeID

	// Add node to configuration.
	if err := config.AddNode(n.ID, n.URL); err != nil {
		l.Logger.Panicf("apply: add node: %s", err)
	}

	// Set configuration index.
	config.Index = e.Index

	// Write configuration.
	if err := l.writeConfig(config); err != nil {
		panic("write config: " + err.Error())
	}
	l.config = config
}

// mustApplyRemovePeer removes a node from the cluster configuration.
func (l *Log) mustApplyRemovePeer(e *LogEntry) error {
	// TODO(benbjohnson): Clone configuration.
	// TODO(benbjohnson): Remove node from configuration.
	// TODO(benbjohnson): Set configuration index.
	// TODO(benbjohnson): Write configuration.
	return nil
}

// AddPeer creates a new peer in the cluster.
// Returns the new peer's identifier and the current configuration.
func (l *Log) AddPeer(u url.URL) (uint64, uint64, *Config, error) {
	// Validate URL.
	if u.Host == "" {
		return 0, 0, nil, fmt.Errorf("peer url required")
	}

	// Apply command.
	b, _ := json.Marshal(&ConfigNode{URL: u})
	index, err := l.internalApply(LogEntryAddPeer, b)
	if err != nil {
		return 0, 0, nil, err
	}
	if err := l.Wait(index); err != nil {
		return 0, 0, nil, err
	}

	// Lock while we look up the node.
	l.mu.Lock()
	defer l.mu.Unlock()

	// Look up node.
	n := l.config.NodeByURL(u)
	if n == nil {
		return 0, 0, nil, fmt.Errorf("node not found")
	}

	return n.ID, l.leaderID, l.config.Clone(), nil
}

// RemovePeer removes an existing peer from the cluster by id.
func (l *Log) RemovePeer(id uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// TODO(benbjohnson): Apply removePeerCommand.

	return nil
}

// Heartbeat establishes dominance by the current leader.
// Returns the current term and highest written log entry index.
func (l *Log) Heartbeat(term, commitIndex, leaderID uint64) (currentIndex uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() || l.state == Stopped {
		l.tracef("Heartbeat: closed")
		return 0, ErrClosed
	}

	// Ignore if the incoming term is less than the log's term.
	if term < l.term {
		l.tracef("HB: stale term, ignore: %d < %d", term, l.term)
		return l.lastLogIndex, ErrStaleTerm
	}

	// Send heartbeat to channel for the state loop to process.
	select {
	case l.heartbeats <- heartbeat{term: term, commitIndex: commitIndex, leaderID: leaderID}:
	default:
	}

	l.tracef("HB: (term=%d, commit=%d, leaderID: %d) (index=%d, term=%d)", term, commitIndex, leaderID, l.lastLogIndex, l.term)
	return l.lastLogIndex, nil
}

// RequestVote requests a vote from the log.
func (l *Log) RequestVote(term, candidateID, lastLogIndex, lastLogTerm uint64) (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return ErrClosed
	}

	defer func() {
		l.tracef("RV(term=%d, candidateID=%d, lastLogIndex=%d, lastLogTerm=%d) (err=%v)", term, candidateID, lastLogIndex, lastLogTerm, err)
	}()

	// Deny vote if:
	//   1. Candidate is requesting a vote from an earlier term. (§5.1)
	//   2. Already voted for a different candidate in this term. (§5.2)
	//   3. Candidate log is less up-to-date than local log. (§5.4)
	if term < l.term {
		return ErrStaleTerm
	} else if term == l.term && l.votedFor != 0 && l.votedFor != candidateID {
		return ErrAlreadyVoted
	} else if lastLogTerm < l.lastLogTerm {
		return ErrOutOfDateLog
	} else if lastLogTerm == l.lastLogTerm && lastLogIndex < l.lastLogIndex {
		return ErrOutOfDateLog
	}

	// Vote for candidate.
	l.votedFor = candidateID

	return nil
}

// WriteEntriesTo attaches a writer to the log from a given index.
// The index specified must be a committed index.
func (l *Log) WriteEntriesTo(w io.Writer, id, term, index uint64) error {
	// Validate and initialize the writer.
	writer, err := l.initWriter(w, id, term, index)
	if err != nil {
		return err
	}

	// Write the snapshot and advance the writer through the log.
	// If an error occurs then remove the writer.
	if err := l.writeTo(writer, id, term, index); err != nil {
		l.mu.Lock()
		l.removeWriter(writer)
		l.mu.Unlock()
		return err
	}

	// Wait for writer to finish.
	<-writer.done
	return nil
}

func (l *Log) writeTo(writer *logWriter, id, term, index uint64) error {
	// Extract the underlying writer.
	w := writer.Writer

	// Write snapshot marker byte.
	if _, err := w.Write([]byte{logEntrySnapshot}); err != nil {
		return err
	}

	// Begin streaming the snapshot.
	snapshotIndex, err := l.FSM.Snapshot(w)
	if err != nil {
		return err
	}

	// Write snapshot index at the end and flush.
	if err := binary.Write(w, binary.BigEndian, snapshotIndex); err != nil {
		return fmt.Errorf("write snapshot index: %s", err)
	}
	flushWriter(w)

	// Write entries since the snapshot occurred and begin tailing writer.
	if err := l.advanceWriter(writer, snapshotIndex); err != nil {
		return err
	}

	return nil
}

// validates writer and adds it to the list of writers.
func (l *Log) initWriter(w io.Writer, id, term, index uint64) (*logWriter, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return nil, ErrClosed
	}

	// Do not begin streaming if:
	//   1. Node is not the leader.
	//   2. Term is after current term.
	//   3. Index is after the commit index.
	if l.state != Leader {
		return nil, ErrNotLeader
	} else if term > l.term {
		select {
		case l.terms <- term:
		default:
		}
		return nil, ErrNotLeader
	} else if index > l.lastLogIndex {
		return nil, ErrUncommittedIndex
	}

	// OPTIMIZE(benbjohnson): Create buffered output to prevent blocking.

	// Write configuration.
	var buf bytes.Buffer
	err := NewConfigEncoder(&buf).Encode(l.config)
	assert(err == nil, "marshal config error: %s", err)
	enc := NewLogEntryEncoder(w)
	if err := enc.Encode(&LogEntry{Type: logEntryConfig, Data: buf.Bytes()}); err != nil {
		return nil, err
	}
	flushWriter(w)

	// Wrap writer and append to log to tail.
	writer := &logWriter{
		Writer:        w,
		id:            id,
		snapshotIndex: l.appliedIndex,
		done:          make(chan struct{}),
	}
	l.writers = append(l.writers, writer)

	return writer, nil
}

// replays entries since the snapshot's index and begins tailing the log.
func (l *Log) advanceWriter(writer *logWriter, snapshotIndex uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if writer has been closed during snapshot.
	select {
	case <-writer.done:
		return errors.New("writer closed during snapshot")
	default:
	}

	// Write pending entries.
	if len(l.entries) > 0 {
		startIndex := l.entries[0].Index
		enc := NewLogEntryEncoder(writer.Writer)
		for _, e := range l.entries[snapshotIndex-startIndex+1:] {
			if err := enc.Encode(e); err != nil {
				return err
			}
		}
	}

	// Flush data.
	flushWriter(writer.Writer)

	// Clear snapshot index on writer.
	writer.snapshotIndex = 0

	return nil
}

// removeWriter removes a writer from the list of log writers.
func (l *Log) removeWriter(writer *logWriter) {
	l.tracef("removeWriter")
	for i, w := range l.writers {
		if w == writer {
			copy(l.writers[i:], l.writers[i+1:])
			l.writers[len(l.writers)-1] = nil
			l.writers = l.writers[:len(l.writers)-1]
			_ = w.Close()
			break
		}
	}
	return
}

// Flush pushes out buffered data for all open writers.
func (l *Log) Flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, w := range l.writers {
		flushWriter(w.Writer)
	}
}

// ReadFrom continually reads log entries from a reader.
func (l *Log) ReadFrom(r io.ReadCloser) error {
	l.tracef("ReadFrom")
	if err := l.initReadFrom(r); err != nil {
		return err
	}

	// If a nil reader is passed in then exit.
	if r == nil {
		return nil
	}

	// Continually decode entries.
	dec := NewLogEntryDecoder(r)
	for {
		// Decode single entry.
		var e LogEntry
		if err := dec.Decode(&e); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// If this is a config entry then update the config.
		if e.Type == logEntryConfig {
			l.tracef("ReadFrom: config")

			config := &Config{}
			if err := NewConfigDecoder(bytes.NewReader(e.Data)).Decode(config); err != nil {
				return err
			}

			l.mu.Lock()
			if err := l.writeConfig(config); err != nil {
				l.mu.Unlock()
				return err
			}
			l.config = config
			l.mu.Unlock()
			continue
		}

		// If this is a snapshot then load it.
		if e.Type == logEntrySnapshot {
			l.tracef("ReadFrom: snapshot")

			if err := l.FSM.Restore(r); err != nil {
				return err
			}
			l.tracef("ReadFrom: snapshot: restored")

			// Read the snapshot index off the end of the snapshot.
			var index uint64
			if err := binary.Read(r, binary.BigEndian, &index); err != nil {
				return fmt.Errorf("read snapshot index: %s", err)
			}
			l.tracef("ReadFrom: snapshot: index=%d", index)

			// Update the indicies & clear the entries.
			l.mu.Lock()
			l.lastLogIndex = index
			l.commitIndex = index
			l.appliedIndex = index
			l.entries = nil
			l.mu.Unlock()

			continue
		}

		// Append entry to the log.
		l.mu.Lock()
		if l.state == Stopped {
			l.mu.Unlock()
			return nil
		}
		//l.tracef("ReadFrom: entry: index=%d / prev=%d / commit=%d", e.Index, l.lastLogIndex, l.commitIndex)
		l.append(&e)
		l.mu.Unlock()
	}
}

// Initializes the ReadFrom() call under a lock and swaps out the readers.
func (l *Log) initReadFrom(r io.ReadCloser) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return ErrClosed
	}

	// Remove previous reader, if one exists.
	l.closeReader()

	// Set new reader.
	l.reader = r
	return nil
}

// heartbeat represents an incoming heartbeat.
type heartbeat struct {
	term        uint64
	commitIndex uint64
	leaderID    uint64
}

// logWriter wraps writers to provide a channel for close notification.
type logWriter struct {
	io.Writer
	id            uint64        // target's log id
	snapshotIndex uint64        // snapshot index, if zero then ignored.
	done          chan struct{} // close notification
}

// Write writes bytes to the underlying writer.
// The write is ignored if the writer is currently snapshotting.
func (w *logWriter) Write(p []byte) (int, error) {
	if w.snapshotIndex != 0 {
		return 0, nil
	}
	return w.Writer.Write(p)
}

func (w *logWriter) Close() error {
	w.snapshotIndex = 0
	close(w.done)
	return nil
}

// flushes data for writers that implement HTTP.Flusher.
func flushWriter(w io.Writer) {
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}
}

// LogEntryType serves as an internal marker for log entries.
// Non-command entry types are handled by the library itself.
type LogEntryType uint8

const (
	LogEntryCommand LogEntryType = iota
	LogEntryNop
	LogEntryInitialize
	LogEntryAddPeer
	LogEntryRemovePeer

	// Internal entry types.
	logEntryConfig   = 254
	logEntrySnapshot = 255
)

// LogEntry represents a single command within the log.
type LogEntry struct {
	Type  LogEntryType
	Index uint64
	Term  uint64
	Data  []byte
}

// encodedHeader returns the encoded header for the entry.
func (e *LogEntry) encodedHeader() []byte {
	var b [logEntryHeaderSize]byte
	binary.BigEndian.PutUint64(b[0:8], (uint64(e.Type)<<56)|uint64(len(e.Data)))
	binary.BigEndian.PutUint64(b[8:16], e.Index)
	binary.BigEndian.PutUint64(b[16:24], e.Term)
	return b[:]
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

var errDone = errors.New("done")

func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func printstack() {
	stack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	fmt.Fprintln(os.Stderr, stack)
}
