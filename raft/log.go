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
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FSM represents the state machine that the log is applied to.
// The FSM must maintain the highest index that it has seen.
type FSM interface {
	// These implement the snapshot and restore.
	io.WriterTo
	io.ReaderFrom

	// Executes a log entry against the state machine.
	// Non-repeatable errors such as system and disk errors must panic.
	Apply(*LogEntry) error

	// Returns the applied index saved to the state machine.
	Index() uint64
}

const logEntryHeaderSize = 8 + 8 + 8 // sz+index+term

// heartbeartErrorLogThreshold is the number of seconds to wait before logging
// another heartbeat error
const heartbeartErrorLogThreshold = 15

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

const (
	// DefaultLogEntryCacheSize is the default number of entries to keep before trimming.
	DefaultLogEntryCacheSize = 1000
)

// Log represents a replicated log of commands based on the Raft protocol.
//
// The log can exist in one of four states that transition based on the following rules:
//
//            ┌───────────┐
//         ┌─▶│  Stopped  │
//         │  └───────────┘
//         │        │
//         │        ▼
//         │  ┌───────────┐
//         ├──│ Follower  │◀─┐
//         │  └───────────┘  │
//   close │        │        │
//    log  │        ▼        │
//         │  ┌───────────┐  │
//         ├──│ Candidate │──┤ higher
//         │  └───────────┘  │  term
//         │        │        │
//         │        ▼        │
//         │  ┌───────────┐  │
//         └──│  Leader   │──┘
//            └───────────┘
//
//   - Stopped moves to Follower when initialized or joined.
//   - Follower moves to Candidate after election timeout.
//   - Candidate moves to Leader after a quorum of votes.
//   - Leader or Candidate moves to Follower if higher term seen.
//   - Any state moves to Stopped if log is closed.
type Log struct {
	mu sync.Mutex

	// The directory where the id, term and config are written to.
	path string

	// The log identifier. This is set when the log initializes
	// or when the log joins to another cluster.
	id uint64

	// Config stores all nodes in the cluster.
	config *Config

	// The ID of the current leader.
	leaderID uint64

	// Current state of the log.
	// The transitioning channel is closed whenever state is changed.
	state         State
	transitioning chan struct{}

	// An atomic flag stating if a snapshot is currently being loaded.
	snapshotting uint32

	// In-memory log entries.
	// Followers replicate these entries from the Leader.
	// Leader appends to the end of these entries.
	// Truncated and trimmed as needed.
	entries []*LogEntry

	// Highest term & index in the log.
	// These are initialially read from the id/term files but otherwise
	// should always match the index/term of the last 'entries' element.
	lastLogTerm  uint64
	lastLogIndex uint64

	// Highest entry to be committed.
	// An entry can be committed once a quorum of nodes have received the entry.
	// Because streaming raft asyncronously replicates entries, the lastLogIndex
	// may be lower than the commitIndex. The commitIndex is always higher than
	// or equal to the FSM.Index().
	commitIndex uint64

	// The current term the log is in. This increases when the log starts a
	// new election term or when the log sees a higher election term.
	term uint64

	// The node this log voted for in the current term.
	votedFor uint64

	// Incoming stream from the leader.
	// This is disconnected when the leader is deposed or the log changes state.
	reader io.ReadCloser

	// Outgoing streams to the followers to replicate the log.
	// These are closed when the leader is deposed.
	writers []*logWriter // outgoing streams to followers

	// Incoming heartbeats and term changes go to these channels
	// and are picked up by the current state.
	heartbeats chan heartbeat
	terms      chan struct{}

	// Close notification and wait.
	wg      sync.WaitGroup
	closing chan struct{}

	// Network address to the reach the log.
	url url.URL

	// The state machine that log entries will be applied to.
	FSM FSM

	// LogEntryCacheSize is the minimum number of log entries to keep before
	// trimming the log. These entries are kept in case a node disconnects
	// momentarily. Otherwise a reconnecting node would have to resnapshot.
	LogEntryCacheSize int

	// The transport used to communicate with other nodes in the cluster.
	Transport interface {
		Join(u url.URL, nodeURL url.URL) (id uint64, leaderID uint64, config *Config, err error)
		Leave(u url.URL, id uint64) error
		Heartbeat(u url.URL, term, commitIndex, leaderID uint64) (lastIndex uint64, err error)
		ReadFrom(u url.URL, id, term, index uint64) (io.ReadCloser, error)
		RequestVote(u url.URL, term, candidateID, lastLogIndex, lastLogTerm uint64) (peerTerm uint64, err error)
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
		heartbeats: make(chan heartbeat, 10),
		terms:      make(chan struct{}, 1),
		Logger:     log.New(os.Stderr, "[raft] ", log.LstdFlags),

		LogEntryCacheSize: DefaultLogEntryCacheSize,
	}
	l.Logger.SetPrefix("[raft] ")
	return l
}

func (l *Log) lock()   { l.mu.Lock() }
func (l *Log) unlock() { l.mu.Unlock() }

func (l *Log) printCaller(label string) {
	_, file, line, _ := runtime.Caller(2)
	l.Logger.Printf("%s: %s:%d", label, filepath.Base(file), line)
}

// Path returns the data path of the Raft log.
// Returns an empty string if the log is closed.
func (l *Log) Path() string {
	l.lock()
	defer l.unlock()
	return l.path
}

// URL returns the URL for the log.
func (l *Log) URL() url.URL {
	l.lock()
	defer l.unlock()
	return l.url
}

// SetURL sets the URL for the log. This must be set before opening.
func (l *Log) SetURL(u url.URL) {
	l.lock()
	defer l.unlock()
	assert(!l.opened(), "url cannot be set while log is open")
	l.url = u
}

// URLs returns a list of all URLs in the cluster.
func (l *Log) URLs() []url.URL {
	l.lock()
	defer l.unlock()

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
	l.lock()
	defer l.unlock()
	return l.opened()
}

func (l *Log) opened() bool { return l.path != "" }

// ID returns the log's identifier.
func (l *Log) ID() uint64 {
	l.lock()
	defer l.unlock()
	return l.id
}

// State returns the current state.
func (l *Log) State() State {
	l.lock()
	defer l.unlock()
	return l.state
}

// isSnapshotting returns true if the log is currently restoring from snapshot.
func (l *Log) isSnapshotting() bool { return atomic.LoadUint32(&l.snapshotting) != 0 }

// LastLogIndexTerm returns the last index & term from the log.
func (l *Log) LastLogIndexTerm() (index, term uint64) {
	l.lock()
	defer l.unlock()
	return l.lastLogIndex, l.lastLogTerm
}

// CommtIndex returns the highest committed index.
func (l *Log) CommitIndex() uint64 {
	l.lock()
	defer l.unlock()
	return l.commitIndex
}

// Term returns the current term.
func (l *Log) Term() uint64 {
	l.lock()
	defer l.unlock()
	return l.term
}

// Config returns a the log's current configuration.
func (l *Log) Config() *Config {
	l.lock()
	defer l.unlock()
	if l.config != nil {
		return l.config.Clone()
	}
	return nil
}

// Open initializes the log from a path.
// If the path does not exist then it is created.
func (l *Log) Open(path string) error {
	var closing chan struct{}
	var config *Config
	if err := func() error {
		l.lock()
		defer l.unlock()

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
			return fmt.Errorf("read id: %s", err)
		}
		l.setID(id)

		// Initialize log term.
		term, err := l.readTerm()
		if err != nil {
			return fmt.Errorf("read term: %s", err)
		}
		l.term = term
		l.votedFor = 0
		l.lastLogTerm = term

		// Read config.
		c, err := l.readConfig()
		if err != nil {
			return fmt.Errorf("read config: %s", err)
		}
		l.config = c

		// Determine last applied index from FSM.
		index := l.FSM.Index()
		l.tracef("Open: fsm: index=%d", index)
		l.lastLogIndex = index
		l.commitIndex = index
		l.entries = nil

		// Start goroutine to apply logs.
		l.wg.Add(1)
		l.closing = make(chan struct{})
		go l.applier(l.closing)

		if l.config != nil {
			l.printf("log open: created at %s, with ID %d, term %d, last applied index of %d", path, l.id, l.term, l.lastLogIndex)
		}

		// Retrieve variables to use while starting state loop.
		config = l.config
		closing = l.closing

		return nil
	}(); err == ErrOpen {
		return err
	} else if err != nil {
		_ = l.close()
		return err
	}

	// If a log exists then start the state loop.
	if config != nil {
		// If the config only has one node then start it as the leader.
		// Otherwise start as a follower.
		if len(config.Nodes) == 1 && config.Nodes[0].ID == l.ID() {
			l.Logger.Println("log open: promoting to leader immediately")
			l.startStateLoop(closing, Leader)
		} else {
			l.startStateLoop(closing, Follower)
		}
	} else {
		l.printf("log pending: waiting for initialization or join")
	}

	return nil
}

// Close closes the log.
func (l *Log) Close() error {
	l.lock()
	defer l.unlock()
	return l.close()
}

// close should be called under lock.
func (l *Log) close() error {
	l.tracef("closing...")

	// Remove the reader.
	_ = l.setReader(nil)

	// Notify goroutines of closing and wait outside of lock.
	if l.closing != nil {
		close(l.closing)
		l.closing = nil
		l.unlock()
		l.wg.Wait()
		l.lock()
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
	l.entries = nil
	l.term, l.votedFor = 0, 0
	l.config = nil

	l.tracef("closed")

	return nil
}

func (l *Log) setReaderWithLock(r io.ReadCloser) error {
	l.lock()
	defer l.unlock()
	return l.setReader(r)
}

func (l *Log) setReader(r io.ReadCloser) error {
	if l.reader != nil {
		_ = l.reader.Close()
		l.reader = nil
	}

	// Ignore if there is no new reader.
	if r == nil {
		return nil
	}

	// Close reader immediately and ignore if log is closed.
	if !l.opened() {
		_ = r.Close()
		return ErrClosed
	}

	// Ignore if setting while transitioning state.
	select {
	case <-l.transitioning:
		return errTransitioning
	default:
	}

	// Set new reader.
	l.reader = r
	return nil
}

func (l *Log) setID(id uint64) { l.id = id }

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

// setTerm sets the current term and clears the vote.
func (l *Log) setTerm(term uint64) error {
	l.printf("changing term: %d => %d", l.term, term)

	if err := l.writeTerm(term); err != nil {
		return err
	}

	l.term = term
	l.votedFor = 0
	return nil
}

// mustSetTerm sets the current term and clears the vote. Panic on error.
func (l *Log) mustSetTermIfHigher(term uint64) {
	if term <= l.term {
		return
	}

	if err := l.setTerm(term); err != nil {
		panic("unable to set term: " + err.Error())
	}

	// Signal term change.
	select {
	case l.terms <- struct{}{}:
	default:
	}
}

// readConfig reads the configuration from disk.
func (l *Log) readConfig() (*Config, error) {
	// Read config from disk.
	f, err := os.Open(l.configPath())
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	// Marshal file to a config type.
	config := &Config{}
	if err := NewConfigDecoder(f).Decode(config); err != nil {
		return nil, err
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
	if err := func() error {
		l.lock()
		defer l.unlock()

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
		if err := l.setTerm(term); err != nil {
			return fmt.Errorf("set term: %s", err)
		}
		l.lastLogTerm = term
		l.leaderID = l.id

		return nil
	}(); err != nil {
		return err
	}

	// Begin state loop as leader.
	l.startStateLoop(l.closing, Leader)

	l.printf("log initialize: promoted to 'leader' with cluster ID %d, log ID %d, term %d",
		config.ClusterID, l.id, l.term)

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

// trace writes a log message if DebugEnabled is true.
func (l *Log) trace(v ...interface{}) {
	if l.DebugEnabled {
		l.Logger.Print(v...)
	}
}

// trace writes a formatted log message if DebugEnabled is true.
func (l *Log) tracef(msg string, v ...interface{}) {
	if l.DebugEnabled {
		l.printf(msg, v...)
	}
}

func (l *Log) printf(msg string, v ...interface{}) {
	l.Logger.Printf(fmt.Sprintf("%s[%d]: ", l.state, l.id)+msg+"\n", v...)
}

// IsLeader returns true if the log is the current leader.
func (l *Log) IsLeader() bool {
	l.lock()
	defer l.unlock()
	return l.id != 0 && l.id == l.leaderID
}

// Leader returns the id and URL associated with the current leader.
// Returns zero if there is no current leader.
func (l *Log) Leader() (id uint64, u url.URL) {
	l.lock()
	defer l.unlock()
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
	l.lock()
	defer l.unlock()
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
		l.lock()
		defer l.unlock()

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
	if err := func() error {
		l.lock()
		defer l.unlock()

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

		return nil
	}(); err != nil {
		return err
	}

	// Begin state loop as follower.
	l.startStateLoop(l.closing, Follower)

	// Change to a follower state.
	l.Logger.Println("log join: entered 'follower' state for cluster at", u, " with log ID", l.id)

	// Wait for anything to be applied.
	return l.Wait(1)
}

// Leave removes the log from cluster membership and removes the log data.
func (l *Log) Leave() error {
	l.lock()
	defer l.unlock()

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

	for {
		// Transition to new state.
		func() {
			l.lock()
			defer l.unlock()

			l.tracef("log state change: %s => %s (term=%d)", l.state, state, l.term)
			l.state = state
			l.transitioning = make(chan struct{}, 0)

			// Remove previous reader, if one exists.
			_ = l.setReader(nil)
		}()

		// Notify caller on first state changes.
		if stateChanged != nil {
			close(stateChanged)
			stateChanged = nil
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
	defer l.setReaderWithLock(nil)
	defer close(l.transitioning)

	// Read log from leader in a separate goroutine.
	wg.Add(1)
	go l.readFromLeader(&wg)

	for {
		select {
		case <-closing:
			return Stopped
		case ch := <-l.Clock.AfterElectionTimeout():
			close(ch)

			// Ignore timeout if we are snapshotting.
			// Or if we haven't received confirmation of join.
			if l.isSnapshotting() {
				continue
			} else if l.FSM.Index() == 0 {
				continue
			}

			// TODO: Prevote before becoming candidate.

			return Candidate
		case hb := <-l.heartbeats:
			l.tracef("followerLoop: recv heartbeat: term=%d, idx=%d", hb.term, hb.commitIndex)

			// Update term, commit index & leader.
			l.lock()
			l.mustSetTermIfHigher(hb.term)
			if hb.commitIndex > l.commitIndex {
				l.commitIndex = hb.commitIndex
			}
			l.leaderID = hb.leaderID
			l.unlock()
		}
	}
}

func (l *Log) readFromLeader(wg *sync.WaitGroup) {
	defer wg.Done()
	l.tracef("readFromLeader")

	for {
		select {
		case <-l.transitioning:
			l.tracef("readFromLeader: exiting")
			return
		default:
		}

		// Retrieve the term, last log index, & leader URL.
		l.lock()
		id, lastLogIndex, term := l.id, l.lastLogIndex, l.term
		_, u := l.leader()
		l.unlock()

		// If no leader exists then wait momentarily and retry.
		if u.Host == "" {
			l.tracef("readFromLeader: no leader")
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Connect to leader.
		l.tracef("readFromLeader: read from: %s, id=%d, term=%d, index=%d", u.String(), id, term, lastLogIndex)
		r, err := l.Transport.ReadFrom(u, id, term, lastLogIndex)
		if err != nil {
			l.printf("readFromLeader: connect stream: %s", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Attach the stream to the log.
		if err := l.ReadFrom(r); err != nil {
			l.tracef("readFromLeader: read from: disconnect: %s", err)
		}
	}
}

// truncateTo removes all uncommitted entries up to index.
func (l *Log) truncateTo(index uint64) {
	assert(index >= l.commitIndex, "cannot truncate to before the commit index: id=%d index=%d, commit=%d", l.id, index, l.commitIndex)

	// Ignore if there are no entries.
	// Ignore if all entries are before the index.
	if len(l.entries) == 0 {
		return
	} else if l.entries[len(l.entries)-1].Index < index {
		return
	}

	// If all entries are after the index, remove all.
	if l.entries[0].Index > index {
		l.entries = nil
		l.lastLogIndex, l.lastLogTerm = index, l.term
		return
	}

	// Otherwise slice entries starting from index.
	emin, emax := l.entries[0].Index, l.entries[len(l.entries)-1].Index
	l.tracef("trunc: entries=[%d,%d], index=%d", emin, emax, index)
	l.entries = l.entries[:index-emin+1]
	l.lastLogIndex = index

	assert(l.entries[len(l.entries)-1].Index == index, "last entry in truncation not index: id=%d emax=%d, index=%d",
		l.id, l.entries[len(l.entries)-1].Index, index)
}

// candidateLoop requests vote from other nodes in an attempt to become leader.
func (l *Log) candidateLoop(closing <-chan struct{}) State {
	l.tracef("candidateLoop")
	defer l.tracef("candidateLoop: exit")

	// TODO: prevote

	// Increment term and request votes.
	l.lock()
	l.mustSetTermIfHigher(l.term + 1)
	l.votedFor = l.id
	term := l.term

	select {
	case <-l.terms:
	default:
	}
	l.unlock()

	// Ensure all candidate goroutines complete before transitioning to another state.
	var wg sync.WaitGroup
	defer wg.Wait()
	defer close(l.transitioning)

	// Read log from leader in a separate goroutine.
	wg.Add(1)
	elected := make(chan struct{}, 1)
	go l.elect(term, elected, &wg)

	for {
		select {
		case <-closing:
			return Stopped
		case hb := <-l.heartbeats:
			l.lock()
			l.mustSetTermIfHigher(hb.term)
			if hb.term >= l.term {
				l.tracef("candidateLoop: new leader: old=%d new=%d", l.leaderID, hb.leaderID)
				l.leaderID = hb.leaderID
				l.unlock()
				return Follower
			}
			l.unlock()
		case <-l.terms:
			return Follower
		case <-elected:
			l.lock()
			l.leaderID = l.id
			l.unlock()
			return Leader
		case ch := <-l.Clock.AfterElectionTimeout():
			close(ch)
			return Follower
		}
	}
}

func (l *Log) elect(term uint64, elected chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	// Ensure we are in the same term and copy properties.
	l.lock()
	if term != l.term {
		l.unlock()
		return
	}
	id, config := l.id, l.config
	lastLogIndex, lastLogTerm := l.lastLogIndex, l.lastLogTerm
	l.unlock()

	// Request votes from peers.
	votes := make(chan struct{}, len(config.Nodes))
	for _, n := range config.Nodes {
		if n.ID == id {
			continue
		}
		go func(n *ConfigNode) {
			peerTerm, err := l.Transport.RequestVote(n.URL, term, id, lastLogIndex, lastLogTerm)
			l.tracef("send req vote(term=%d, candidateID=%d, lastLogIndex=%d, lastLogTerm=%d) (term=%d, err=%v)", term, id, lastLogIndex, lastLogTerm, peerTerm, err)

			// If an error occured then update term.
			if err != nil {
				l.lock()
				l.mustSetTermIfHigher(peerTerm)
				l.unlock()
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
		case <-l.transitioning:
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
	defer close(l.transitioning)

	// Retrieve leader's term.
	l.lock()
	term := l.term

	select {
	case <-l.terms:
	default:
	}
	l.unlock()

	// Read log from leader in a separate goroutine.
	for {
		// Send hearbeat to followers.
		wg.Add(1)
		committed := make(chan uint64, 1)
		go l.heartbeater(term, committed, &wg)

		// Wait for close, new leader, or new heartbeat response.
		select {
		case <-closing: // wait for state change.
			return Stopped

		case <-l.terms: // step down on higher term
			l.lock()
			l.truncateTo(l.commitIndex)
			l.unlock()
			return Follower

		case hb := <-l.heartbeats: // update term, if necessary
			l.lock()
			l.mustSetTermIfHigher(hb.term)
			l.unlock()

		case commitIndex, ok := <-committed:
			// Quorum not reached, try again.
			if !ok {
				continue
			}

			// Quorum reached, set new commit index.
			l.lock()
			if commitIndex > l.commitIndex {
				l.tracef("leaderLoop: committed: idx=%d", commitIndex)
				l.commitIndex = commitIndex
			}
			l.unlock()
			continue
		}
	}
}

// heartbeater continually sends heartbeats to all peers.
func (l *Log) heartbeater(term uint64, committed chan uint64, wg *sync.WaitGroup) {
	defer wg.Done()

	// Ensure term is correct and retrieve current state.
	l.lock()
	if l.term != term {
		l.unlock()
		return
	}
	commitIndex, localIndex, leaderID, config := l.commitIndex, l.lastLogIndex, l.id, l.config
	l.unlock()

	// Commit latest index if there are no peers.
	if config == nil || len(config.Nodes) <= 1 {
		time.Sleep(10 * time.Millisecond)
		committed <- localIndex
		return
	}

	l.tracef("leaderLoop: send heartbeat: start: n=%d", len(config.Nodes))

	// Send heartbeats to all peers.
	peerIndices := make(chan uint64, len(config.Nodes))
	for _, n := range config.Nodes {
		if n.ID == leaderID {
			continue
		}
		go func(n *ConfigNode) {
			peerIndex, err := l.Transport.Heartbeat(n.URL, term, commitIndex, leaderID)
			if err != nil {
				c := atomic.AddInt64(&n.HeartbeatErrorCount, 1)
				// log heartbeat error once every 15 seconds to avoid flooding the logs
				if time.Now().Unix()-atomic.LoadInt64(&n.LastHeartbeatError) > heartbeartErrorLogThreshold {
					l.printf("leaderLoop: send heartbeat: error: cnt=%d %s", c, err)
					atomic.StoreInt64(&n.LastHeartbeatError, time.Now().Unix())
				}
				return
			}
			if atomic.LoadInt64(&n.LastHeartbeatError) != 0 {
				l.printf("leaderLoop: send heartbeat: success url=%s", n.URL.String())
				atomic.StoreInt64(&n.LastHeartbeatError, 0)
				atomic.StoreInt64(&n.HeartbeatErrorCount, 0)
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
		case <-l.transitioning:
			l.tracef("leaderLoop: send heartbeat: transitioning")
			return
		case peerIndex := <-peerIndices:
			l.tracef("leaderLoop: send heartbeat: index: idx=%d, idxs=%+v", peerIndex, indexes)
			indexes = append(indexes, peerIndex) // collect responses
		case ch := <-after:
			// Once we have enough indices then return the lowest index
			// among the highest quorum of nodes.
			quorumN := (len(config.Nodes) / 2) + 1
			if len(indexes) >= quorumN {
				// Return highest index reported by quorum.
				sort.Sort(sort.Reverse(uint64Slice(indexes)))
				committed <- indexes[quorumN-1]
				l.tracef("leaderLoop: send heartbeat: commit: idx=%d, idxs=%+v", commitIndex, indexes)
			} else {
				l.tracef("leaderLoop: send heartbeat: no quorum: idxs=%+v", indexes)
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

// Apply executes a command against the log.
// This function returns once the command has been committed to the log.
func (l *Log) Apply(command []byte) (uint64, error) {
	return l.internalApply(LogEntryCommand, command)
}

func (l *Log) internalApply(typ LogEntryType, command []byte) (index uint64, err error) {
	l.lock()
	defer l.unlock()

	// Do not apply if this node is not the leader.
	if l.state != Leader {
		return 0, ErrNotLeader
	}

	// Create log entry.
	e := &LogEntry{
		Type:  typ,
		Index: l.lastLogIndex + 1,
		Term:  l.term,
		Data:  command,
	}
	index = e.Index

	// Append to the log.
	if err := l.append(e); err != nil {
		return 0, fmt.Errorf("append: %s", err)
	}

	// If there is no config or only one node then move commit index forward.
	if l.config == nil || len(l.config.Nodes) <= 1 {
		l.commitIndex = l.lastLogIndex
	}

	return
}

// Wait blocks until a given index is applied.
func (l *Log) Wait(idx uint64) error {
	// TODO(benbjohnson): Check for leadership change (?).
	// TODO(benbjohnson): Add timeout.

	for {
		l.lock()
		state, index := l.state, l.FSM.Index()
		l.unlock()

		if state == Stopped {
			return ErrClosed
		} else if index >= idx {
			return nil
		}
		time.Sleep(WaitInterval)
	}
}

// waitCommitted blocks until a given committed index is reached.
func (l *Log) waitCommitted(index uint64) error {
	for {
		l.lock()
		state, committedIndex := l.state, l.commitIndex
		l.unlock()

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
		l.lock()
		lastLogIndex := l.lastLogIndex
		//l.tracef("waitUncommitted: %s / %d", l.state, l.lastLogIndex)
		l.unlock()

		if lastLogIndex >= index {
			return nil
		}
		time.Sleep(WaitInterval)
	}
}

// append adds a log entry to the list of entries.
func (l *Log) append(e *LogEntry) error {
	// Exit if log is not in a running state.
	// Ignore replayed entries.
	if l.state == Stopped {
		return ErrClosed
	} else if e.Index <= l.lastLogIndex {
		return nil
	}

	// If the entry is not the next then the cluster may have changed leaders.
	// Attempt to trim the log to the index if it is not committed yet.
	if e.Index > l.lastLogIndex+1 {
		if e.Index >= l.commitIndex {
			l.truncateTo(e.Index)
		} else if e.Index < l.commitIndex {
			l.lastLogIndex = 0
			return ErrSnapshotRequired
		}
	}

	assert(e.Index == l.lastLogIndex+1, "log entry skipped(%d): idx=%d, prev=%d", l.id, e.Index, l.lastLogIndex)
	if len(l.entries) > 0 {
		lastEntry := l.entries[len(l.entries)-1]
		assert(e.Index == lastEntry.Index+1, "non-contiguous log entry(%d): idx=%d, prev=%d", l.id, e.Index, lastEntry.Index)
	}

	// Add to pending entries list to wait to be applied.
	l.entries = append(l.entries, e)
	l.lastLogIndex = e.Index
	l.lastLogTerm = e.Term

	// Encode entry to a byte slice.
	buf := make([]byte, logEntryHeaderSize+len(e.Data))
	copy(buf, e.encodedHeader())
	copy(buf[logEntryHeaderSize:], e.Data)

	// Write to tailing writers.
	l.appendToWriters(buf)

	return nil
}

// appendToWriters writes a byte slice to all attached writers.
func (l *Log) appendToWriters(buf []byte) {
	for i := 0; i < len(l.writers); i++ {
		w := l.writers[i]

		// If an error occurs then remove the writer and close it.
		if _, err := w.Write(buf); err != nil {
			l.printf("append to writers error: %s", err)
			l.removeWriter(w)
			i--
			continue
		}
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

		// Keep applying the next entry until there are no more committed
		// entries that have not been applied to the state machine.
		for {
			if err := l.applyNextUnappliedEntry(closing); err == errClosing {
				break
			} else if err != nil {
				panic(err.Error())
			}
		}

		// Trim entries.
		l.lock()
		l.trim()
		l.unlock()

		// Signal clock that apply is done.
		close(confirm)
	}
}

// applyNextUnappliedEntry applies the next committed entry that has not yet been applied.
func (l *Log) applyNextUnappliedEntry(closing <-chan struct{}) error {
	l.lock()
	defer l.unlock()

	// Verify, under lock, that we're not closing.
	select {
	case <-closing:
		return errClosing
	default:
	}

	// Ignore if there are no entries in the log.
	if len(l.entries) == 0 {
		return errClosing
	}

	// Determine next index to apply.
	// Ignore if next index is after the commit index.
	// Ignore if the entry is not streamed to the log yet.
	index := l.FSM.Index() + 1
	if index > l.commitIndex {
		return errClosing
	} else if index > l.entries[len(l.entries)-1].Index {
		return errClosing
	}

	// Retrieve next entry.
	e := l.entries[index-l.entries[0].Index]
	assert(e.Index == index, "apply: index mismatch: expected=%d, actual=%d (i=%d)", index, e.Index, index-l.entries[0].Index)

	// Special handling for internal log entries.
	switch e.Type {
	case LogEntryCommand, LogEntryNop:
	case LogEntryInitialize:
		l.mustApplyInitialize(e)
	case LogEntryAddPeer:
		l.mustApplyAddPeer(e)
	case LogEntryRemovePeer:
		l.mustApplyRemovePeer(e)
	default:
		return fmt.Errorf("unsupported command type: %d", e.Type)
	}

	// Apply to FSM.
	if err := l.FSM.Apply(e); err != nil {
		return fmt.Errorf("apply: %s", err)
	}

	return nil
}

// trim truncates the log based on the applied index and pending writers.
func (l *Log) trim() {
	if len(l.entries) == 0 {
		return
	}

	// Determine lowest index to trim to.
	index := l.FSM.Index()
	for _, w := range l.writers {
		if w.snapshotIndex > 0 && w.snapshotIndex < index {
			index = w.snapshotIndex
		}
	}

	// Ignore if the index is lower than the first entry.
	// This can occur on a new snapshot.
	if index < l.entries[0].Index {
		return
	}

	// Ignore if trimming would cause the entries to fall below the min cache size.
	offset := int(index-l.entries[0].Index) - l.LogEntryCacheSize
	if offset <= 0 {
		return
	}

	// Reslice entries list.
	l.entries = l.entries[offset:]
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
	l.lock()
	defer l.unlock()

	// Look up node.
	n := l.config.NodeByURL(u)
	if n == nil {
		return 0, 0, nil, fmt.Errorf("node not found")
	}

	return n.ID, l.leaderID, l.config.Clone(), nil
}

// RemovePeer removes an existing peer from the cluster by id.
func (l *Log) RemovePeer(id uint64) error {
	l.lock()
	defer l.unlock()

	// TODO(benbjohnson): Apply removePeerCommand.

	return nil
}

// Heartbeat establishes dominance by the current leader.
// Returns the current term and highest written log entry index.
func (l *Log) Heartbeat(term, commitIndex, leaderID uint64) (currentIndex uint64, err error) {
	// Ignore if snapshotting.
	if l.isSnapshotting() {
		return 0, ErrSnapshotting
	}

	// Otherwise obtain lock and process heartbeat.
	l.lock()
	defer l.unlock()

	// Check if log is closed.
	if !l.opened() || l.state == Stopped {
		l.tracef("recv heartbeat: closed")
		return 0, ErrClosed
	}

	// Ignore if the incoming term is less than the log's term.
	if term < l.term {
		l.tracef("recv heartbeat: stale term, ignore: %d < %d", term, l.term)
		return l.lastLogIndex, ErrStaleTerm
	}

	// Send heartbeat to channel for the state loop to process.
	select {
	case l.heartbeats <- heartbeat{term: term, commitIndex: commitIndex, leaderID: leaderID}:
	default:
	}

	l.tracef("recv heartbeat: (term=%d, commit=%d, leaderID: %d) (index=%d, term=%d)", term, commitIndex, leaderID, l.lastLogIndex, l.term)
	return l.lastLogIndex, nil
}

// RequestVote requests a vote from the log.
func (l *Log) RequestVote(term, candidateID, lastLogIndex, lastLogTerm uint64) (peerTerm uint64, err error) {
	// Ignore if snapshotting.
	if l.isSnapshotting() {
		return 0, ErrSnapshotting
	}

	// Otherwise obtain lock and process vote.
	l.lock()
	defer l.unlock()

	// Check if log is closed.
	if !l.opened() {
		return l.term, ErrClosed
	}

	defer func() {
		l.tracef("recv req vote: (term=%d, candidateID=%d, lastLogIndex=%d, lastLogTerm=%d) (err=%v)",
			term, candidateID, lastLogIndex, lastLogTerm, err)
	}()

	// Deny vote if:
	//   1. Candidate is requesting a vote from an earlier term. (§5.1)
	//   2. Already voted for a different candidate in this term. (§5.2)
	//   3. Candidate log is less up-to-date than local log. (§5.4)
	if term < l.term {
		return l.term, ErrStaleTerm
	} else if term == l.term && l.votedFor != 0 && l.votedFor != candidateID {
		return l.term, ErrAlreadyVoted
	}

	// Notify term change.
	l.mustSetTermIfHigher(term)

	// Reject request if log is out of date.
	if lastLogTerm < l.lastLogTerm {
		return l.term, ErrOutOfDateLog
	} else if lastLogTerm == l.lastLogTerm && lastLogIndex < l.lastLogIndex {
		return l.term, ErrOutOfDateLog
	}

	// Vote for candidate.
	l.votedFor = candidateID

	return l.term, nil
}

// WriteEntriesTo attaches a writer to the log from a given index.
// The index specified must be a committed index.
func (l *Log) WriteEntriesTo(w io.Writer, id, term, index uint64) error {
	// Write the snapshot and advance the writer through the log.
	// If an error occurs then remove the writer.
	var writer *logWriter
	if err := func() error {
		// Validate and initialize the writer.
		var err error
		writer, snapshotRequired, err := l.initWriter(w, id, term, index)
		if err != nil {
			l.printf("unable to init writer: %s", err)
			return fmt.Errorf("init writer: %s", err)
		}

		// Write snapshot, if necessary.
		if snapshotRequired {
			if err := l.writeSnapshotTo(writer, id, term, index); err != nil {
				return fmt.Errorf("write snapshot to: %s", err)
			}
		}

		// Wait for writer to finish.
		<-writer.done

		return nil
	}(); err != nil {
		l.lock()
		l.removeWriter(writer)
		l.unlock()
		l.printf("unable to write entries: %s", err)
		return err
	}

	return nil
}

// initWriter validates writer and adds it to the list of writers. It returns the writer,
// and a bool indicating whether the writer requires a snapshot.
func (l *Log) initWriter(w io.Writer, id, term, index uint64) (*logWriter, bool, error) {
	l.lock()
	defer l.unlock()

	// Check if log is closed.
	if !l.opened() {
		return nil, false, ErrClosed
	}

	// Do not begin streaming if:
	//   1. Node is not the leader.
	//   2. Term is after current term.
	if l.state != Leader {
		return nil, false, ErrNotLeader
	} else if term > l.term {
		l.mustSetTermIfHigher(term)
		return nil, false, ErrNotLeader
	}

	// If the index is past the leader's log then reset and begin from the end.
	// The follower will check the index and trim its log as needed. If the
	// follower cannot trim its log then it needs to retrieve a snapshot.
	if index > l.lastLogIndex {
		index = l.lastLogIndex
	}

	// Encode configuration.
	var buf bytes.Buffer
	err := NewConfigEncoder(&buf).Encode(l.config)
	assert(err == nil, "marshal config error: %s", err)

	// Write configuration.
	if err := NewLogEntryEncoder(w).Encode(&LogEntry{Type: logEntryConfig, Data: buf.Bytes()}); err != nil {
		return nil, false, err
	}
	flushWriter(w)

	// Wrap writer and append to log to tail.
	writer := &logWriter{
		Writer: w,
		id:     id,
		done:   make(chan struct{}),
	}
	l.writers = append(l.writers, writer)

	// If index is before first available entry, then require snapshot.
	if len(l.entries) == 0 || index < l.entries[0].Index {
		writer.snapshotIndex = l.FSM.Index()
	} else {
		writer.snapshotIndex = index
		if err := l.advanceWriter(writer); err != nil {
			return nil, false, fmt.Errorf("advance writer: %s", err)
		}
	}

	return writer, writer.snapshotIndex > 0, nil
}

func (l *Log) writeSnapshotTo(writer *logWriter, id, term, index uint64) error {
	// Write snapshot if the requested index is less than the snapshot index.
	// Write snapshot marker byte.
	if _, err := writer.Writer.Write([]byte{logEntrySnapshot}); err != nil {
		return err
	}

	// Begin streaming the snapshot.
	if _, err := l.FSM.WriteTo(writer.Writer); err != nil {
		return err
	}
	flushWriter(writer.Writer)

	// Begin advancing writer from the snapshot.
	l.lock()
	defer l.unlock()
	if err := l.advanceWriter(writer); err != nil {
		return fmt.Errorf("advance writer: %s", err)
	}

	return nil
}

// replays entries since the snapshot's index and begins tailing the log.
func (l *Log) advanceWriter(writer *logWriter) error {
	// Check if writer has been closed during snapshot.
	select {
	case <-writer.done:
		return errors.New("writer closed during snapshot")
	default:
	}

	// Write pending entries.
	if len(l.entries) > 0 {
		enc := NewLogEntryEncoder(writer.Writer)
		for _, e := range l.entries[writer.snapshotIndex-l.entries[0].Index+1:] {
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
	if writer == nil {
		return
	}

	for i, w := range l.writers {
		if w == writer {
			copy(l.writers[i:], l.writers[i+1:])
			l.writers[len(l.writers)-1] = nil
			l.writers = l.writers[:len(l.writers)-1]
			_ = w.Close()
			l.printf("writer removed: id=%d idx=%d", w.id, w.snapshotIndex)
			break
		}
	}
	return
}

// ReadFrom continually reads log entries from a reader.
func (l *Log) ReadFrom(r io.ReadCloser) error {
	l.tracef("ReadFrom")
	if err := l.initReadFrom(r); err == errTransitioning {
		return err
	} else if err != nil {
		return fmt.Errorf("init read from: %s", err)
	}

	// If a nil reader is passed in then exit.
	if r == nil {
		return nil
	}

	l.tracef("reading from stream")

	// Continually decode entries.
	dec := NewLogEntryDecoder(r)
	for {
		// Decode single entry.
		e := &LogEntry{}
		if err := dec.Decode(e); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// Apply special config & snapshot entries immediately.
		// All other entries get appended to the log.
		switch e.Type {
		case logEntryConfig:
			l.tracef("ReadFrom: config")
			if err := l.applyConfigLogEntry(e); err != nil {
				l.printf("error reading config from stream: %s", err)
				return fmt.Errorf("apply config log entry: %s", err)
			}

		case logEntrySnapshot:
			if err := l.applySnapshotLogEntry(e, r); err != nil {
				l.printf("error snapshotting from stream: %s", err)
				return fmt.Errorf("apply snapshot log entry: %s", err)
			}

		default:
			// Append entry to the log.
			if err := func() error {
				l.lock()
				defer l.unlock()
				if err := l.append(e); err != nil {
					return fmt.Errorf("append: %s", err)
				}

				return nil
			}(); err != nil {
				l.printf("error appending from stream: %s", err)
				return err
			}
		}
	}
}

// applyConfigLogEntry updates the config for a config log entry.
func (l *Log) applyConfigLogEntry(e *LogEntry) error {
	// Parse configuration from the log entry.
	config := &Config{}
	if err := NewConfigDecoder(bytes.NewReader(e.Data)).Decode(config); err != nil {
		return fmt.Errorf("decode config: %s", err)
	}

	// Write the configuration to disk.
	l.lock()
	defer l.unlock()
	if err := l.writeConfig(config); err != nil {
		return fmt.Errorf("write config: %s", err)
	}
	l.config = config

	return nil
}

// applySnapshotLogEntry restores a snapshot log entry.
func (l *Log) applySnapshotLogEntry(e *LogEntry, r io.Reader) error {
	l.lock()
	defer l.unlock()

	// Flag the log as snapshotting.
	atomic.StoreUint32(&l.snapshotting, 1)
	defer atomic.StoreUint32(&l.snapshotting, 0)

	// Log snapshotting time.
	start := time.Now()
	l.printf("applying snapshot: begin")
	defer func() { l.printf("applying snapshot: done (%s)", time.Since(start)) }()

	// Let the FSM rebuild its state from the data in r.
	if _, err := l.FSM.ReadFrom(r); err != nil {
		return fmt.Errorf("fsm restore: %s", err)
	}

	// Update the indicies & clear the entries.
	index := l.FSM.Index()
	l.lastLogIndex = index
	l.commitIndex = index
	l.entries = nil

	return nil
}

// Initializes the ReadFrom() call under a lock and swaps out the readers.
func (l *Log) initReadFrom(r io.ReadCloser) error {
	l.lock()
	defer l.unlock()

	// Check if log is closed.
	if !l.opened() {
		return ErrClosed
	}

	// Close previous reader & set new one.
	if err := l.setReader(r); err == errTransitioning {
		return err
	} else if err != nil {
		return fmt.Errorf("set reader: %s", err)
	}

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
func (w *logWriter) Write(p []byte) (int, error) {
	// Ignore if the writer is currently snapshotting.
	if w.snapshotIndex != 0 {
		return 0, nil
	}

	// Write to underlying writer.
	n, err := w.Writer.Write(p)
	if err != nil {
		return n, err
	}

	// Flush writer if successful.
	flushWriter(w.Writer)
	return n, err
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
