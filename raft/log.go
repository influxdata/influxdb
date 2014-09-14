package raft

import (
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
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	// DefaultHeartbeatTimeout is the default time to wait between heartbeats.
	DefaultHeartbeatTimeout = 50 * time.Millisecond

	// DefaultElectionTimeout is the default time before starting an election.
	DefaultElectionTimeout = 150 * time.Millisecond

	// DefaultReconnectTimeout is the default time to wait before reconnecting.
	DefaultReconnectTimeout = 10 * time.Millisecond
)

// FSM represents the state machine that the log is applied to.
type FSM interface {
	Apply(*LogEntry) error
	Snapshot(io.Writer) error
	Restore(io.Reader) error
}

const logEntryHeaderSize = 8 + 8 + 8 // sz+index+term

// State represents whether the log is a follower, candidate, or leader.
type State int

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

	state State              // current node state
	ch    chan chan struct{} // state change channel

	currentTerm uint64 // current election term
	votedFor    uint64 // candidate voted for in current election term

	leaderID     uint64 // the current leader
	currentIndex uint64 // highest entry written to disk
	commitIndex  uint64 // highest entry to be committed
	appliedIndex uint64 // highest entry to applied to state machine

	nextIndex  map[uint64]uint64 // next entry to send to each follower
	matchIndex map[uint64]uint64 // highest known replicated entry for each follower

	reader  io.ReadCloser // incoming stream from leader
	writers []io.Writer   // outgoing streams to followers

	segment *segment // TODO(benbjohnson): support multiple segments

	done []chan chan struct{} // list of channels to signal close.

	// Network address to the reach the log.
	URL *url.URL

	// The state machine that log entries will be applied to.
	FSM FSM

	// The transport used to communicate with other nodes in the cluster.
	// If nil, then the DefaultTransport is used.
	Transport Transport

	// The amount of time between Append Entries RPC calls from the leader to
	// its followers.
	HeartbeatTimeout time.Duration

	// The amount of time before a follower attempts an election.
	ElectionTimeout time.Duration

	// The amount of time between stream reconnection attempts.
	ReconnectTimeout time.Duration

	// Clock is an abstraction of the time package. By default it will use
	// a real-time clock but a mock clock can be used for testing.
	Clock Clock

	// Rand returns a random number.
	Rand func() int64

	// This logs some asynchronous errors that occur within the log.
	Logger *log.Logger
}

// Path returns the data path of the Raft log.
// Returns an empty string if the log is closed.
func (l *Log) Path() string { return l.path }

func (l *Log) idPath() string     { return filepath.Join(l.path, "id") }
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

// Config returns a the log's current configuration.
func (l *Log) Config() *Config {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.config != nil {
		return l.config.clone()
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
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	l.path = path

	// Set some defaults.
	if l.Clock == nil {
		l.Clock = &clock{}
	}
	if l.Rand == nil {
		l.Rand = rand.Int63
	}
	if l.Logger == nil {
		l.Logger = log.New(os.Stderr, "[raft] ", log.LstdFlags)
	}

	// Set default timeouts.
	if l.HeartbeatTimeout == 0 {
		l.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if l.ElectionTimeout == 0 {
		l.ElectionTimeout = DefaultElectionTimeout
	}
	if l.ReconnectTimeout == 0 {
		l.ReconnectTimeout = DefaultReconnectTimeout
	}

	// Initialize log identifier.
	id, err := l.readID()
	if err != nil {
		_ = l.close()
		return err
	}
	l.id = id

	// Read config.
	c, err := readConfig(l.configPath())
	if err != nil {
		_ = l.close()
		return err
	}
	l.config = c

	// TODO(benbjohnson): Determine last applied index from FSM.

	// TEMP(benbjohnson): Create empty log segment.
	l.segment = &segment{
		path:  filepath.Join(l.path, "1.log"),
		index: 0,
	}

	// Start goroutine to apply logs.
	l.done = append(l.done, make(chan chan struct{}))
	go l.applier(l.done[len(l.done)-1])

	// TODO(benbjohnson): Open log segments.

	// TODO(benbjohnson): Replay latest log.

	return nil
}

// Close closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.close()
}

func (l *Log) close() error {
	// Shutdown all goroutines.
	for _, done := range l.done {
		ch := make(chan struct{})
		done <- ch
		<-ch
	}
	l.done = nil

	// Close the segments.
	if l.segment != nil {
		_ = l.segment.Close()
		l.segment = nil
	}

	// Clear log info.
	l.id = 0
	l.path = ""

	return nil
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
	return ioutil.WriteFile(l.idPath(), b, 0600)
}

// transport returns the log's transport or the default transport.
func (l *Log) transport() Transport {
	if t := l.Transport; t != nil {
		return t
	}
	return DefaultTransport
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
		config.addNode(id, l.URL)

		// Generate new 8-hex digit cluster identifier.
		config.ClusterID = uint64(l.Rand())

		// Generate log id.
		if err := l.writeID(id); err != nil {
			return err
		}
		l.id = id

		// Automatically promote to leader.
		l.currentTerm = 1
		l.setState(Leader)

		return nil
	}()
	if err != nil {
		return err
	}

	// Set initial configuration.
	b, _ := json.Marshal(&config)
	if err := l.internalApply(LogEntryInitialize, b); err != nil {
		return err
	}

	return nil
}

// Leader returns the id and URL associated with the current leader.
// Returns zero if there is no current leader.
func (l *Log) Leader() (id uint64, u *url.URL) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leader()
}

func (l *Log) leader() (id uint64, u *url.URL) {
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

// Join contacts a node in the cluster to request membership.
// A log cannot join a cluster if it has already been initialized.
func (l *Log) Join(u *url.URL) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if open.
	if !l.opened() {
		return ErrClosed
	} else if l.id != 0 {
		return ErrInitialized
	} else if l.URL == nil {
		return ErrURLRequired
	}

	// Send join request.
	id, config, err := l.transport().Join(u, l.URL)
	if err != nil {
		return err
	}

	// Write identifier.
	if err := l.writeID(id); err != nil {
		return err
	}
	l.id = id

	// Write config.
	if err := writeConfig(l.configPath(), config); err != nil {
		return err
	}
	l.config = config

	// Change to a follower state.
	l.setState(Follower)

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

// setState moves the log to a given state.
func (l *Log) setState(state State) {
	// Ignore if we're moving to the same state.
	if l.state == state {
		return
	}

	// Stop previous state.
	if l.ch != nil {
		ch := make(chan struct{})
		l.ch <- ch
		<-ch
		l.ch = nil
	}

	// Set the new state.
	l.state = state

	// Execute event loop for the given state.
	switch state {
	case Follower:
		l.ch = make(chan chan struct{})
		go l.followerLoop(l.ch)
	case Candidate:
		l.ch = make(chan chan struct{})
		go l.candidateLoop(l.ch)
	case Leader:
		l.ch = make(chan chan struct{})
		go l.leaderLoop(l.ch)
	}
}

// followerLoop continually attempts to stream the log from the current leader.
func (l *Log) followerLoop(done chan chan struct{}) {
	for {
		// Check if the loop has been closed.
		select {
		case ch := <-done:
			close(ch)
			return
		default:
		}

		// Retrieve the term, last index, & leader URL.
		l.mu.Lock()
		index, term := l.currentIndex, l.currentTerm
		_, u := l.leader()
		l.mu.Unlock()

		// If there's no leader URL then wait and try again.
		if u == nil {
			l.Clock.Sleep(l.ReconnectTimeout)
			continue
		}

		// Connect to leader.
		r, err := l.transport().ReadFrom(u, term, index)
		if err != nil {
			if l.Opened() {
				l.Logger.Printf("connect stream: %s", err)
				l.Clock.Sleep(l.ReconnectTimeout)
			}
			continue
		}

		// Attach the stream to the log.
		if err := l.ReadFrom(r); err != nil {
			if l.Opened() {
				l.Logger.Printf("read stream: %s", err)
				l.Clock.Sleep(l.ReconnectTimeout)
			}
			continue
		}
	}
}

// candidateLoop attempts to receive enough votes to become leader.
func (l *Log) candidateLoop(done chan chan struct{}) {
	// TODO(benbjohnson): Implement candidate loop.
}

// leaderLoop periodically sends heartbeats to all followers to maintain dominance.
func (l *Log) leaderLoop(done chan chan struct{}) {
	ticker := l.Clock.Ticker(l.HeartbeatTimeout)
	for {
		// Send hearbeat to followers.
		l.sendHeartbeat()

		select {
		case ch := <-done: // wait for state change.
			ticker.Stop()
			close(ch)
			return
		case <-ticker.C: // wait for next heartbeat
		}
	}
}

// sendHeartbeat sends heartbeats to all the nodes.
func (l *Log) sendHeartbeat() {
	// Retrieve config and term.
	l.mu.Lock()
	commitIndex, currentIndex := l.commitIndex, l.currentIndex
	term, leaderID := l.currentTerm, l.id
	config := l.config
	l.mu.Unlock()

	// Ignore if there is no config or nodes yet.
	if config == nil || len(config.Nodes) <= 1 {
		return
	}

	// Determine node count.
	nodeN := len(config.Nodes)

	// Send heartbeats to all followers.
	ch := make(chan uint64, nodeN)
	for _, n := range config.Nodes {
		if n.ID != l.id {
			go func(n *Node) {
				lastIndex, currentTerm, err := l.transport().Heartbeat(n.URL, term, commitIndex, leaderID)
				if err != nil {
					l.Logger.Printf("heartbeat: %s", err)
					return
				} else if currentTerm > term {
					// TODO(benbjohnson): Step down.
					return
				}
				ch <- lastIndex
			}(n)
		}
	}

	// Wait for heartbeat responses or timeout.
	after := l.Clock.After(l.HeartbeatTimeout)
	indexes := make([]uint64, 1, nodeN)
	indexes[0] = currentIndex
loop:
	for {
		select {
		case <-after:
			break loop
		case index := <-ch:
			indexes = append(indexes, index)
			if len(indexes) == nodeN {
				break loop
			}
		}
	}

	// Ignore if we don't have enough for a quorum ((n / 2) + 1).
	// We don't add the +1 because the slice starts from 0.
	quorumIndex := (nodeN / 2)
	if quorumIndex >= len(indexes) {
		return
	}

	// Determine commit index by quorum (n/2+1).
	sort.Sort(uint64Slice(indexes))
	newCommitIndex := indexes[quorumIndex]

	// Update the commit index, if higher.
	l.mu.Lock()
	if newCommitIndex > l.commitIndex {
		l.commitIndex = newCommitIndex
	}
	l.mu.Unlock()
}

// Apply executes a command against the log.
// This function returns once the command has been committed to the log.
func (l *Log) Apply(command []byte) error {
	return l.internalApply(LogEntryCommand, command)
}

func (l *Log) internalApply(typ LogEntryType, command []byte) error {
	var index uint64
	err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()

		// Do not apply if this node is not the leader.
		if l.state != Leader {
			return ErrNotLeader
		}

		// Create log entry.
		l.currentIndex++
		e := LogEntry{
			Type:  typ,
			Index: l.currentIndex,
			Term:  l.currentTerm,
			Data:  command,
		}
		index = e.Index

		// Append to the current log segment.
		if err := l.segment.append(&e); err != nil {
			return err
		}

		// If there is no config or only one node then move commit index forward.
		if l.config == nil || len(l.config.Nodes) <= 1 {
			l.commitIndex = l.currentIndex
		}

		return nil
	}()
	if err != nil {
		return err
	}

	// Wait for consensus.
	// HACK(benbjohnson): Notify via channel instead.
	for {
		l.mu.Lock()
		state, appliedIndex := l.state, l.appliedIndex
		l.mu.Unlock()

		// If we've changed leadership then return error.
		// If the last applied index has moved past our index then move forward.
		if state != Leader {
			return ErrNotLeader
		} else if appliedIndex >= index {
			return nil
		}

		// Otherwise wait.
		l.Clock.Sleep(10 * time.Millisecond)
	}
}

// applier runs in a separate goroutine and applies all entries between the
// previously applied index and the current commit index.
func (l *Log) applier(done chan chan struct{}) {
	for {
		// Wait for a close signal or timeout.
		select {
		case ch := <-done:
			close(ch)
			return

		// FIX(benbjohnson): Push notification over channel(?)
		case <-l.Clock.After(10 * time.Millisecond):
		}

		// Apply all entries committed since the previous apply.
		err := func() error {
			l.mu.Lock()
			defer l.mu.Unlock()

			// Retrieve entries.
			startIndex, endIndex := l.appliedIndex, l.commitIndex
			if startIndex > l.currentIndex {
				startIndex = l.currentIndex
			}
			if endIndex > l.currentIndex {
				endIndex = l.currentIndex
			}
			entries, err := l.entries(startIndex, endIndex)
			if err != nil {
				return err
			}

			// Iterate over each entry and apply it.
			for _, e := range entries {
				// Apply to FSM.
				switch e.Type {
				case LogEntryCommand:
					if err := l.FSM.Apply(e); err != nil {
						return err
					}

				case LogEntryNop:
					// nop

				case LogEntryInitialize:
					if err := l.applyInitialize(e); err != nil {
						return err
					}

				case LogEntryAddPeer:
					if err := l.applyAddPeer(e); err != nil {
						return err
					}

				case LogEntryRemovePeer:
					if err := l.applyRemovePeer(e); err != nil {
						return err
					}

				default:
					return fmt.Errorf("unsupported command type: %d", e.Type)
				}

				// Increment applied index.
				l.appliedIndex++
			}

			return nil
		}()

		// If error occurred then log it.
		// The log will retry after a given timeout.
		if err != nil {
			l.Logger.Printf("apply: %s", err)
			// TODO(benbjohnson): Longer timeout before retry?
			continue
		}
	}
}

// apply a log initialization command by parsing and setting the configuration.
func (l *Log) applyInitialize(e *LogEntry) error {
	// Parse the configuration from the log entry.
	var config *Config
	if err := json.Unmarshal(e.Data, &config); err != nil {
		return fmt.Errorf("initialize: %s", err)
	}

	// Set the last update index on the configuration.
	config.Index = e.Index

	// TODO(benbjohnson): Lock the log while we update the configuration.

	// Perist the configuration to disk.
	if err := writeConfig(l.configPath(), config); err != nil {
		return err
	}
	l.config = config

	return nil
}

// applyAddPeer adds a node to the cluster configuration.
func (l *Log) applyAddPeer(e *LogEntry) error {
	// Unmarshal node from entry data.
	var n *Node
	if err := json.Unmarshal(e.Data, &n); err != nil {
		return err
	}

	// Clone configuration.
	config := l.config.clone()

	// Increment the node identifier.
	config.MaxNodeID++
	n.ID = config.MaxNodeID

	// Add node to configuration.
	if err := config.addNode(n.ID, n.URL); err != nil {
		return err
	}

	// Set configuration index.
	config.Index = e.Index

	// Write configuration.
	if err := writeConfig(l.configPath(), config); err != nil {
		return err
	}
	l.config = config

	return nil
}

// applyRemovePeer removes a node from the cluster configuration.
func (l *Log) applyRemovePeer(e *LogEntry) error {
	// TODO(benbjohnson): Clone configuration.
	// TODO(benbjohnson): Remove node from configuration.
	// TODO(benbjohnson): Set configuration index.
	// TODO(benbjohnson): Write configuration.
	return nil
}

// AddPeer creates a new peer in the cluster.
// Returns the new peer's identifier and the current configuration.
func (l *Log) AddPeer(u *url.URL) (uint64, *Config, error) {
	// Validate URL.
	if u == nil {
		return 0, nil, fmt.Errorf("peer url required")
	}

	// Apply command.
	b, _ := json.Marshal(&Node{URL: u})
	if err := l.internalApply(LogEntryAddPeer, b); err != nil {
		return 0, nil, err
	}

	// Lock while we look up the node.
	l.mu.Lock()
	defer l.mu.Unlock()

	// Look up node.
	n := l.config.NodeByURL(u)
	if n == nil {
		return 0, nil, fmt.Errorf("node not found")
	}

	return n.ID, l.config.clone(), nil
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
func (l *Log) Heartbeat(term, commitIndex, leaderID uint64) (currentIndex, currentTerm uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return 0, 0, ErrClosed
	}

	// Ignore if the incoming term is less than the log's term.
	if term < l.currentTerm {
		return l.currentIndex, l.currentTerm, nil
	}

	// Step down if we see a higher term.
	if term > l.currentTerm {
		l.currentTerm = term
		l.setState(Follower)
	}
	l.commitIndex = commitIndex
	l.leaderID = leaderID

	return l.currentIndex, l.currentTerm, nil
}

// RequestVote requests a vote from the log.
func (l *Log) RequestVote(term, candidateID, lastLogIndex, lastLogTerm uint64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return 0, ErrClosed
	}

	// Deny vote if:
	//   1. Candidate is requesting a vote from an earlier term. (§5.1)
	//   2. Already voted for a different candidate in this term. (§5.2)
	//   3. Candidate log is less up-to-date than local log. (§5.4)
	if term < l.currentTerm {
		return l.currentTerm, ErrStaleTerm
	} else if term == l.currentTerm && l.votedFor != 0 && l.votedFor != candidateID {
		return l.currentTerm, ErrAlreadyVoted
	} else if lastLogTerm < l.currentTerm {
		return l.currentTerm, ErrOutOfDateLog
	} else if lastLogTerm == l.currentTerm && lastLogIndex < l.currentIndex {
		return l.currentTerm, ErrOutOfDateLog
	}

	// Vote for candidate.
	l.votedFor = candidateID

	// TODO(benbjohnson): Update term.

	return l.currentTerm, nil
}

// WriteTo attaches a writer to the log from a given index.
// The index specified must be a committed index.
func (l *Log) WriteTo(w io.Writer, term, index uint64) error {
	err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()

		// Check if log is closed.
		if !l.opened() {
			return ErrClosed
		}

		// Step down if from a higher term.
		if term > l.currentTerm {
			l.setState(Follower)
		}

		// Do not begin streaming if:
		//   1. Node is not the leader.
		//   2. Term is earlier than current term.
		//   3. Index is after the commit index.
		if l.state != Leader {
			return ErrNotLeader
		} else if index > l.commitIndex {
			return ErrUncommittedIndex
		}

		// Add writer.
		l.writers = append(l.writers, w)

		return nil
	}()
	if err != nil {
		return err
	}

	// TODO(benbjohnson): Write snapshot, if index is unavailable.

	// Write segment to the writer.
	warn("writeTo>>")
	if err := l.segment.writeTo(w, index); err != nil {
		return err
	}
	warn("  --writeTo")

	return nil
}

// ReadFrom continually reads log entries from a reader.
func (l *Log) ReadFrom(r io.ReadCloser) error {
	err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()

		// Check if log is closed.
		if !l.opened() {
			l.mu.Unlock()
			return ErrClosed
		}

		// Remove previous reader, if one exists.
		if l.reader != nil {
			_ = l.reader.Close()
		}

		// Set new reader.
		l.reader = r
		return nil
	}()
	if err != nil {
		return err
	}

	// If a nil reader is passed in then exit.
	if r == nil {
		return nil
	}

	// TODO(benbjohnson): Check first byte for snapshot marker.

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

		// Append entry to the log.
		if err := l.segment.append(&e); err != nil {
			return err
		}
		l.currentIndex = e.Index
	}
}

// entries retrieves sequential log entries where the index is in [startIndex, endIndex].
func (l *Log) entries(startIndex, endIndex uint64) ([]*LogEntry, error) {
	// TODO(benbjohnson): Split across multiple segments.
	return l.segment.entriesInRange(startIndex, endIndex)
}

// Elect increments the log's term and forces an election.
// This function does not guarentee that this node will become the leader.
func (l *Log) Elect() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.elect()
}

func (l *Log) elect() error {
	l.state = Candidate
	// TODO(benbjohnson): Hold election.
	return nil
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
)

// LogEntry represents a single command within the log.
type LogEntry struct {
	Type  LogEntryType
	Index uint64
	Term  uint64
	Data  []byte
}

// EncodedHeader returns the encoded header for the entry.
func (e *LogEntry) EncodedHeader() []byte {
	var b [logEntryHeaderSize]byte
	binary.BigEndian.PutUint64(b[0:8], (uint64(e.Type)<<60)|uint64(len(e.Data)))
	binary.BigEndian.PutUint64(b[8:16], e.Index)
	binary.BigEndian.PutUint64(b[16:24], e.Term)
	return b[:]
}

// LogEntryEncoder encodes entries to a writer.
type LogEntryEncoder struct {
	w io.Writer
}

// NewLogEntryEncoder returns a new instance of the LogEntryEncoder that
// will encode to a writer.
func NewLogEntryEncoder(w io.Writer) *LogEntryEncoder {
	return &LogEntryEncoder{w: w}
}

// Encode writes a log entry to the encoder's writer.
func (enc *LogEntryEncoder) Encode(e *LogEntry) error {
	// Write header.
	if _, err := enc.w.Write(e.EncodedHeader()); err != nil {
		return err
	}

	// Write data.
	if _, err := enc.w.Write(e.Data); err != nil {
		return err
	}
	return nil
}

// LogEntryDecoder decodes entries from a reader.
type LogEntryDecoder struct {
	r io.Reader
}

// NewLogEntryDecoder returns a new instance of the LogEntryDecoder that
// will decode from a reader.
func NewLogEntryDecoder(r io.Reader) *LogEntryDecoder {
	return &LogEntryDecoder{r: r}
}

// Decode reads a log entry from the decoder's reader.
func (dec *LogEntryDecoder) Decode(e *LogEntry) error {
	// Read header.
	var b [logEntryHeaderSize]byte
	if _, err := io.ReadFull(dec.r, b[:]); err != nil {
		return err
	}
	sz := binary.BigEndian.Uint64(b[0:8])
	e.Type, sz = LogEntryType(sz>>60), sz&0x0FFFFFFF
	e.Index = binary.BigEndian.Uint64(b[8:16])
	e.Term = binary.BigEndian.Uint64(b[16:24])

	// Read data.
	data := make([]byte, sz)
	if _, err := io.ReadFull(dec.r, data); err != nil {
		return err
	}
	e.Data = data

	return nil
}

// segment represents a contiguous subset of the log.
// The segment can be represented on-disk and/or in-memory.
type segment struct {
	mu sync.RWMutex

	path    string  // path of segment on-disk
	sealed  bool    // true if entries committed and cannot change.
	index   uint64  // starting index
	offsets []int64 // byte offset of each index

	f       *os.File    // on-disk representation
	buf     []byte      // in-memory cache, nil means uncached
	entries []*LogEntry // decode log entries

	writers []*segmentWriter // segment tailing
}

// Close closes the segment.
func (s *segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeWriters()
	return nil
}

func (s *segment) closeWriters() {
	warn("** CLOSE WRITERS **", len(s.writers))
	for _, w := range s.writers {
		warn("  !!")
		w.Close()
	}
}

// seal sets the segment as sealed.
func (s *segment) seal() {
	s.mu.Lock()
	defer s.mu.Lock()

	// Seal off segment.
	s.sealed = true

	// Close all tailing writers.
	for _, w := range s.writers {
		w.Close()
	}
}

// append writes a set of entries to the segment.
func (s *segment) append(e *LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Encode header and record offset.
	header := e.EncodedHeader()
	offset := int64(len(s.buf))

	// TODO(benbjohnson): Write to the file, if available.

	// Write to the cache, if available.
	s.buf = append(s.buf, header...)
	s.buf = append(s.buf, e.Data...)

	// Save offset & entry.
	s.offsets = append(s.offsets, offset)
	s.entries = append(s.entries, e)

	// Write to tailing writers.
	for i, w := range s.writers {
		// If an error occurs then remove the writer and close it.
		if _, err := w.Write(s.buf[offset:]); err != nil {
			copy(s.writers[i:], s.writers[i+1:])
			s.writers[len(s.writers)-1] = nil
			s.writers = s.writers[:len(s.writers)-1]
			w.Close()
		}

		// Flush, if possible.
		if w, ok := w.Writer.(http.Flusher); ok {
			w.Flush()
		}
	}

	return nil
}

// entriesInRange retrieves sequential log entries where the index is in [startIndex, endIndex].
func (s *segment) entriesInRange(startIndex, endIndex uint64) ([]*LogEntry, error) {
	// TODO(benbjohnson): Assert indexes are within segment bounds.
	return s.entries[startIndex:endIndex], nil
}

// truncate removes all entries after a given index.
func (s *segment) truncate(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO(benbjohnson): Truncate the file, if available.
	// TODO(benbjohnson): Truncate the cache, if available.

	return nil
}

// writerTo writes to a writer from a given log index.
func (s *segment) writeTo(w io.Writer, index uint64) error {
	var writer *segmentWriter
	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// TODO(benbjohnson): Create buffered output to prevent blocking.

		// Catch up writer to the end of the segment.
		offset := s.offsets[index-s.index]
		if _, err := w.Write(s.buf[offset:]); err != nil {
			return err
		}

		// Flush, if applicable.
		if w, ok := w.(http.Flusher); ok {
			w.Flush()
		}

		// Wrap writer and append to segment to tail.
		// If segment is already closed then simply close the channel immediately.
		writer = &segmentWriter{w, make(chan error)}
		if s.sealed {
			writer.Close()
		} else {
			s.writers = append(s.writers, writer)
		}
		warn(">", len(s.writers))

		return nil
	}()
	if err != nil {
		return err
	}

	// Wait for segment to finish writing.
	return <-writer.ch
}

// segmentWriter wraps writers to provide a channel for close notification.
type segmentWriter struct {
	io.Writer
	ch chan error
}

func (w *segmentWriter) Close() {
	close(w.ch)
}

// Config represents the configuration for the log.
type Config struct {
	// Cluster identifier. Used to prevent separate clusters from
	// accidentally communicating with one another.
	ClusterID uint64 `json:"clusterID,omitempty"`

	// List of nodes in the cluster.
	Nodes []*Node `json:"nodes,omitempty"`

	// Index is the last log index when the configuration was updated.
	Index uint64 `json:"index,omitempty"`

	// MaxNodeID is the largest node identifier generated for this config.
	MaxNodeID uint64 `json:"maxNodeID,omitempty"`
}

// NodeByID returns a node by identifier.
func (c *Config) NodeByID(id uint64) *Node {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// NodeByULR returns a node by URL.
func (c *Config) NodeByURL(u *url.URL) *Node {
	for _, n := range c.Nodes {
		if n.URL.String() == u.String() {
			return n
		}
	}
	return nil
}

// addNode adds a new node to the config.
// Returns an error if a node with the same id or url exists.
func (c *Config) addNode(id uint64, u *url.URL) error {
	if id <= 0 {
		return errors.New("invalid node id")
	} else if u == nil {
		return errors.New("node url required")
	}

	for _, n := range c.Nodes {
		if n.ID == id {
			return fmt.Errorf("node id already exists")
		} else if n.URL.String() == u.String() {
			return fmt.Errorf("node url already in use")
		}
	}

	c.Nodes = append(c.Nodes, &Node{ID: id, URL: u})

	return nil
}

// removeNode removes a node by id.
// Returns an error if the node does not exist.
func (c *Config) removeNode(id uint64) error {
	for i, node := range c.Nodes {
		if node.ID == id {
			copy(c.Nodes[i:], c.Nodes[i+1:])
			c.Nodes[len(c.Nodes)-1] = nil
			c.Nodes = c.Nodes[:len(c.Nodes)-1]
			return nil
		}
	}
	return fmt.Errorf("node not found: %d", id)
}

// clone returns a deep copy of the configuration.
func (c *Config) clone() *Config {
	other := &Config{
		ClusterID: c.ClusterID,
		Index:     c.Index,
		MaxNodeID: c.MaxNodeID,
	}
	other.Nodes = make([]*Node, len(c.Nodes))
	for i, n := range c.Nodes {
		other.Nodes[i] = n.clone()
	}
	return other
}

// Node represents a single machine in the raft cluster.
type Node struct {
	ID  uint64   `json:"id"`
	URL *url.URL `json:"url,omitempty"`
}

// clone returns a deep copy of the node.
func (n *Node) clone() *Node {
	other := &Node{ID: n.ID, URL: &url.URL{}}
	*other.URL = *n.URL
	return other
}

// nodeJSONMarshaler represents the JSON serialized form of the Node type.
type nodeJSONMarshaler struct {
	ID  uint64 `json:"id"`
	URL string `json:"url,omitempty"`
}

// MarshalJSON encodes the node into a JSON-formatted byte slice.
func (n *Node) MarshalJSON() ([]byte, error) {
	var o nodeJSONMarshaler
	o.ID = n.ID
	if n.URL != nil {
		o.URL = n.URL.String()
	}
	return json.Marshal(&o)
}

// UnmarshalJSON decodes a JSON-formatted byte slice into a node.
func (n *Node) UnmarshalJSON(data []byte) error {
	// Unmarshal into temporary type.
	var o nodeJSONMarshaler
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Convert values to a node.
	n.ID = o.ID
	if o.URL != "" {
		u, err := url.Parse(o.URL)
		if err != nil {
			return err
		}
		n.URL = u
	}

	return nil
}

// readConfig reads the configuration from disk.
func readConfig(path string) (*Config, error) {
	// Read config from disk.
	f, err := os.Open(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	// Marshal file to a config type.
	var config *Config
	if f != nil {
		if err := json.NewDecoder(f).Decode(&config); err != nil {
			return nil, err
		}
	}
	return config, nil
}

// writeConfig writes the configuration to disk.
func writeConfig(path string, config *Config) error {
	// FIX(benbjohnson): Atomic write.

	// Open file.
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Marshal config into file.
	if err := json.NewEncoder(f).Encode(config); err != nil {
		return err
	}

	return nil
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("asser failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func jsonify(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
