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

	state State         // current node state
	ch    chan struct{} // state change channel

	term        uint64    // current election term
	leaderID    uint64    // the current leader
	votedFor    uint64    // candidate voted for in current election term
	lastContact time.Time // last contact from the leader

	index        uint64 // highest entry available (FSM or log)
	commitIndex  uint64 // highest entry to be committed
	appliedIndex uint64 // highest entry to applied to state machine

	reader  io.ReadCloser // incoming stream from leader
	writers []*logWriter  // outgoing streams to followers

	entries []*LogEntry

	done []chan chan struct{} // list of channels to signal close.

	// Network address to the reach the log.
	URL *url.URL

	// The state machine that log entries will be applied to.
	FSM FSM

	// The transport used to communicate with other nodes in the cluster.
	Transport interface {
		Join(u *url.URL, nodeURL *url.URL) (uint64, *Config, error)
		Leave(u *url.URL, id uint64) error
		Heartbeat(u *url.URL, term, commitIndex, leaderID uint64) (lastIndex, currentTerm uint64, err error)
		ReadFrom(u *url.URL, id, term, index uint64) (io.ReadCloser, error)
		RequestVote(u *url.URL, term, candidateID, lastLogIndex, lastLogTerm uint64) (uint64, error)
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
		Clock:     NewClock(),
		Transport: &HTTPTransport{},
		Rand:      rand.Int63,
	}
	l.SetLogOutput(os.Stderr)
	return l
}

// Path returns the data path of the Raft log.
// Returns an empty string if the log is closed.
func (l *Log) Path() string { return l.path }

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
	l.tracef("fsm: index=%d", index)
	l.index = index
	l.appliedIndex = index
	l.commitIndex = index

	// If this log is the only node then promote to leader immediately.
	// Otherwise if there's any configuration then start it as a follower.
	if c != nil && len(c.Nodes) == 1 && c.Nodes[0].ID == l.id {
		l.Logger.Println("log open: promoting to leader immediately")
		l.setState(Leader)
	} else if l.config != nil {
		l.setState(Follower)
		l.lastContact = l.Clock.Now()
	}

	// Start goroutine to apply logs.
	l.done = append(l.done, make(chan chan struct{}))
	go l.applier(l.done[len(l.done)-1])

	// Start goroutine to check for election timeouts.
	l.done = append(l.done, make(chan chan struct{}))
	go l.elector(l.done[len(l.done)-1])

	l.Logger.Printf("log open: created at %s, with ID %d, term %d, last applied index of %d",
		path, l.id, l.term, l.index)

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
	// Close the reader, if open.
	if l.reader != nil {
		_ = l.reader.Close()
		l.reader = nil
	}

	// Stop the log.
	l.setState(Stopped)

	// Retrieve closing channels under lock.
	a := l.done
	l.done = nil

	// Unlock while we shutdown all goroutines.
	l.mu.Unlock()
	for _, done := range a {
		ch := make(chan struct{})
		done <- ch
		<-ch
	}
	l.mu.Lock()

	// Close the writers.
	for _, w := range l.writers {
		_ = w.Close()
	}
	l.writers = nil

	l.tracef("closing")

	// Clear log info.
	l.setID(0)
	l.path = ""
	l.index, l.term = 0, 0
	l.config = nil

	return nil
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
	return ioutil.WriteFile(l.idPath(), b, 0600)
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
	return ioutil.WriteFile(l.termPath(), b, 0600)
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
		config.addNode(id, l.URL)

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
		l.setState(Leader)

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
	if l.URL != nil {
		host = l.URL.Host
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
	// Validate under lock.
	var nodeURL *url.URL
	if err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()

		if !l.opened() {
			return ErrClosed
		} else if l.id != 0 {
			return ErrInitialized
		} else if l.URL == nil {
			return ErrURLRequired
		}

		nodeURL = l.URL
		return nil
	}(); err != nil {
		return err
	}

	l.tracef("joining to: %s", u)

	// Send join request.
	id, config, err := l.Transport.Join(u, nodeURL)
	if err != nil {
		return err
	}

	l.tracef("confirmed join")

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

	// Change to a follower state.
	l.setState(Follower)
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

// setState moves the log to a given state.
func (l *Log) setState(state State) {
	l.Logger.Printf("log state change: %s => %s", l.state, state)

	// Stop previous state.
	if l.ch != nil {
		close(l.ch)
		l.ch = nil
	}

	// Remove previous reader, if one exists.
	if l.reader != nil {
		_ = l.reader.Close()
	}

	// Set the new state.
	l.state = state

	// Execute event loop for the given state.
	switch state {
	case Follower:
		l.ch = make(chan struct{})
		go l.followerLoop(l.ch)
	case Candidate:
		l.ch = make(chan struct{})
		go l.elect(l.ch)
	case Leader:
		l.ch = make(chan struct{})
		go l.leaderLoop(l.ch)
	}
}

// followerLoop continually attempts to stream the log from the current leader.
func (l *Log) followerLoop(done chan struct{}) {
	l.tracef("follower loop")
	var rch chan struct{}
	for {
		// Retrieve the term, last index, & leader URL.
		l.mu.Lock()
		if err := check(done); err != nil {
			l.mu.Unlock()
			return
		}
		id, index, term := l.id, l.index, l.term
		_, u := l.leader()
		l.mu.Unlock()

		// If no leader exists then wait momentarily and retry.
		if u == nil {
			l.tracef("follower loop: no leader")
			time.Sleep(1 * time.Millisecond)
			continue
		}

		// Connect to leader.
		l.tracef("follower loop: read from: %s, id=%d, term=%d, index=%d", u.String(), id, term, index)
		r, err := l.Transport.ReadFrom(u, id, term, index)
		if err != nil {
			l.Logger.Printf("connect stream: %s", err)
		}

		// Stream the log in from a separate goroutine.
		rch = make(chan struct{})
		go func(u *url.URL, term, index uint64, rch chan struct{}) {
			// Attach the stream to the log.
			if err := l.ReadFrom(r); err != nil {
				close(rch)
			}
		}(u, term, index, rch)

		// Check if the state has changed, stream has closed, or an
		// election timeout has passed.
		select {
		case <-done:
			return
		case <-rch:
			// FIX: l.Clock.Sleep(l.ReconnectTimeout)
			continue
		}
	}
}

// elect requests votes from other nodes in an attempt to become the new leader.
func (l *Log) elect(done chan struct{}) {
	// Retrieve config and term.
	l.mu.Lock()
	if err := check(done); err != nil {
		l.mu.Unlock()
		return
	}
	term, id := l.term, l.id
	lastLogIndex, lastLogTerm := l.index, l.term // FIX: Find actual last index/term.
	config := l.config
	l.mu.Unlock()

	// Determine node count.
	nodeN := len(config.Nodes)

	// Request votes from all other nodes.
	ch := make(chan struct{}, nodeN)
	for _, n := range config.Nodes {
		if n.ID != id {
			go func(n *ConfigNode) {
				peerTerm, err := l.Transport.RequestVote(n.URL, term, id, lastLogIndex, lastLogTerm)
				if err != nil {
					l.Logger.Printf("request vote: %s", err)
					return
				} else if peerTerm > term {
					// TODO(benbjohnson): Step down.
					return
				}
				ch <- struct{}{}
			}(n)
		}
	}

	// Wait for respones or timeout.
	after := l.Clock.AfterElectionTimeout()
	voteN := 1
loop:
	for {
		select {
		case <-done:
			return
		case ch := <-after:
			defer close(ch)
			break loop
		case <-ch:
			voteN++
			if voteN >= (nodeN/2)+1 {
				break loop
			}
		}
	}

	// Exit if we don't have a quorum.
	if voteN < (nodeN/2)+1 {
		return
	}

	// Change to a leader state.
	l.mu.Lock()
	if err := check(done); err != nil {
		l.mu.Unlock()
		return
	}
	l.setState(Leader)
	l.mu.Unlock()

	return
}

// leaderLoop periodically sends heartbeats to all followers to maintain dominance.
func (l *Log) leaderLoop(done chan struct{}) {
	l.tracef("leader loop: start")
	confirm := make(chan struct{}, 0)
	for {
		// Send hearbeat to followers.
		if err := l.sendHeartbeat(done); err != nil {
			close(confirm)
			return
		}

		// Signal clock that the heartbeat has occurred.
		close(confirm)

		select {
		case <-done: // wait for state change.
			return
		case confirm = <-l.Clock.AfterHeartbeatInterval(): // wait for next heartbeat
		}
	}
}

// sendHeartbeat sends heartbeats to all the nodes.
func (l *Log) sendHeartbeat(done chan struct{}) error {
	l.tracef("sending heartbeat")

	// Retrieve config and term.
	l.mu.Lock()
	if err := check(done); err != nil {
		l.mu.Unlock()
		return err
	}
	commitIndex, localIndex := l.commitIndex, l.index
	term, leaderID := l.term, l.id
	config := l.config
	l.mu.Unlock()

	// Ignore if there is no config or nodes yet.
	if config == nil || len(config.Nodes) <= 1 {
		l.tracef("sending heartbeat: no peers")
		return nil
	}

	// Determine node count.
	nodeN := len(config.Nodes)

	// Send heartbeats to all followers.
	ch := make(chan uint64, nodeN)
	for _, n := range config.Nodes {
		if n.ID != l.id {
			go func(n *ConfigNode) {
				l.tracef("sending heartbeat: url=%s, term=%d, commit=%d, leaderID=%d", n.URL, term, commitIndex, leaderID)
				peerIndex, peerTerm, err := l.Transport.Heartbeat(n.URL, term, commitIndex, leaderID)
				if err != nil {
					l.Logger.Printf("heartbeat: %s", err)
					return
				} else if peerTerm > term {
					// TODO(benbjohnson): Step down.
					l.tracef("sending heartbeat: TODO step down: peer=%d, term=%d", peerTerm, term)
					return
				}
				ch <- peerIndex
			}(n)
		}
	}

	// Wait for heartbeat responses or timeout.
	after := l.Clock.AfterHeartbeatInterval()
	indexes := make([]uint64, 1, nodeN)
	indexes[0] = localIndex
loop:
	for {
		select {
		case <-done:
			return errDone
		case ch := <-after:
			defer close(ch)
			l.tracef("sending heartbeat: timeout")
			break loop
		case index := <-ch:
			indexes = append(indexes, index)
			if len(indexes) == nodeN {
				l.tracef("sending heartbeat: received heartbeats")
				break loop
			}
		}
	}

	// Ignore if we don't have enough for a quorum ((n / 2) + 1).
	// We don't add the +1 because the slice starts from 0.
	quorumIndex := (nodeN / 2)
	if quorumIndex >= len(indexes) {
		l.tracef("sending heartbeat: no quorum: n=%d", quorumIndex)
		return nil
	}

	// Determine commit index by quorum (n/2+1).
	sort.Sort(uint64Slice(indexes))
	newCommitIndex := indexes[quorumIndex]

	// Update the commit index, if higher.
	l.mu.Lock()
	if err := check(done); err != nil {
		l.mu.Unlock()
		return err
	}
	if newCommitIndex > l.commitIndex {
		l.tracef("sending heartbeat: commit index %d => %d", l.commitIndex, newCommitIndex)
		l.commitIndex = newCommitIndex
	}
	l.mu.Unlock()
	return nil
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
		Index: l.index + 1,
		Term:  l.term,
		Data:  command,
	}
	index = e.Index

	// Append to the log.
	l.append(&e)

	// If there is no config or only one node then move commit index forward.
	if l.config == nil || len(l.config.Nodes) <= 1 {
		l.commitIndex = l.index
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
		time.Sleep(1 * time.Millisecond)
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
		time.Sleep(1 * time.Millisecond)
	}
}

// waitUncommitted blocks until a given uncommitted index is reached.
func (l *Log) waitUncommitted(index uint64) error {
	for {
		l.mu.Lock()
		state, uncommittedIndex := l.state, l.index
		l.mu.Unlock()

		if state == Stopped {
			return ErrClosed
		} else if uncommittedIndex >= index {
			return nil
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// append adds a log entry to the list of entries.
func (l *Log) append(e *LogEntry) {
	l.tracef("append: idx=%d, prev=%d", e.Index, l.index)
	assert(e.Index == l.index+1, "non-contiguous log index(%d): idx=%d, prev=%d", l.id, e.Index, l.index)

	// Encode entry to a byte slice.
	buf := make([]byte, logEntryHeaderSize+len(e.Data))
	copy(buf, e.encodedHeader())
	copy(buf[logEntryHeaderSize:], e.Data)

	// Add to pending entries list to wait to be applied.
	l.entries = append(l.entries, e)
	l.index = e.Index

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
func (l *Log) applier(done chan chan struct{}) {
	for {
		// Wait for a close signal or timeout.
		var confirm chan struct{}
		select {
		case ch := <-done:
			close(ch)
			return

		case confirm = <-l.Clock.AfterApplyInterval():
		}

		l.tracef("applying")

		// Apply all entries committed since the previous apply.
		err := func() error {
			l.mu.Lock()
			defer l.mu.Unlock()

			// Verify, under lock, that we're not closing.
			select {
			case ch := <-done:
				close(ch)
				return errDone
			default:
			}

			// Ignore if there are no pending entries.
			// Ignore if all entries are applied.
			if len(l.entries) == 0 {
				l.tracef("applying: no entries")
				return nil
			} else if l.appliedIndex == l.commitIndex {
				l.tracef("applying: up to date")
				return nil
			}

			// Calculate start index.
			min := l.entries[0].Index
			startIndex, endIndex := l.appliedIndex+1, l.commitIndex
			if maxIndex := l.entries[len(l.entries)-1].Index; l.commitIndex > maxIndex {
				endIndex = maxIndex
			}

			// Determine entries to apply.
			entries := l.entries[startIndex-min : endIndex-min+1]

			// Determine low water mark for entries to cut off.
			max := endIndex
			for _, w := range l.writers {
				if w.snapshotIndex > 0 && w.snapshotIndex < max {
					max = w.snapshotIndex
				}
			}
			l.entries = l.entries[max-min:]

			// Iterate over each entry and apply it.
			for _, e := range entries {
				l.tracef("applying: entry: idx=%d", e.Index)

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
		if err == errDone {
			close(confirm)
			return
		} else if err != nil {
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
	config := l.config.clone()

	// Increment the node identifier.
	config.MaxNodeID++
	n.ID = config.MaxNodeID

	// Add node to configuration.
	if err := config.addNode(n.ID, n.URL); err != nil {
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
func (l *Log) AddPeer(u *url.URL) (uint64, *Config, error) {
	// Validate URL.
	if u == nil {
		return 0, nil, fmt.Errorf("peer url required")
	}

	// Apply command.
	b, _ := json.Marshal(&ConfigNode{URL: u})
	index, err := l.internalApply(LogEntryAddPeer, b)
	if err != nil {
		return 0, nil, err
	}
	if err := l.Wait(index); err != nil {
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

	l.tracef("heartbeat: term=%d, commit=%d, leaderID: %d", term, commitIndex, leaderID)

	// Check if log is closed.
	if !l.opened() {
		l.tracef("heartbeat: closed")
		return 0, 0, ErrClosed
	}

	// Ignore if the incoming term is less than the log's term.
	if term < l.term {
		l.tracef("heartbeat: stale term, ignore")
		return l.index, l.term, nil
	}

	// Step down if we see a higher term.
	if term > l.term {
		l.tracef("heartbeat: higher term, stepping down")
		l.term = term
		l.setState(Follower)
	}

	l.commitIndex = commitIndex
	l.leaderID = leaderID
	l.lastContact = l.Clock.Now()

	return l.index, l.term, nil
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
	if term < l.term {
		return l.term, ErrStaleTerm
	} else if term == l.term && l.votedFor != 0 && l.votedFor != candidateID {
		return l.term, ErrAlreadyVoted
	} else if lastLogTerm < l.term {
		return l.term, ErrOutOfDateLog
	} else if lastLogTerm == l.term && lastLogIndex < l.index {
		return l.term, ErrOutOfDateLog
	}

	// Vote for candidate.
	l.votedFor = candidateID

	// TODO(benbjohnson): Update term.

	return l.term, nil
}

// elector runs in a separate goroutine and checks for election timeouts.
func (l *Log) elector(done chan chan struct{}) {
	for {
		// Wait for a close signal or election timeout.
		var confirm chan struct{}
		select {
		case ch := <-done:
			close(ch)
			return
		case confirm = <-l.Clock.AfterElectionTimeout(): // TODO(election): Randomize
		}
		l.tracef("check election")

		// If log is a follower or candidate and an election timeout has passed
		// since a contact from a heartbeat then start a new election.
		err := func() error {
			l.mu.Lock()
			defer l.mu.Unlock()

			// Verify, under lock, that we're not closing.
			select {
			case ch := <-done:
				close(ch)
				return errDone
			default:
			}

			// Ignore if not a follower or a candidate.
			// Ignore if the last contact was less than the election timeout.
			if l.state != Follower && l.state != Candidate {
				l.tracef("elector: log is not follower or candidate")
				return nil
			} else if l.lastContact.IsZero() {
				l.tracef("elector: last contact is zero")
				return nil
			} else if l.Clock.Now().Sub(l.lastContact) < DefaultElectionTimeout { // TODO: Refactor into follower loop and candidate loop.
				l.tracef("elector: last contact is less than election timeout")
				return nil
			}

			l.tracef("elector: beginning election in term %d", l.term+1)

			// Otherwise start a new election and promote.
			term := l.term + 1
			if err := l.writeTerm(term); err != nil {
				return fmt.Errorf("write term: %s", err)
			}
			l.term = term
			l.setState(Candidate)

			return nil
		}()

		// Check if we exited because we're closing.
		if err == errDone {
			close(confirm)
			return
		} else if err != nil {
			panic("unreachable")
		}

		// Signal clock that elector is done.
		close(confirm)
	}
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

	// Step down if from a higher term.
	if term > l.term {
		l.setState(Follower)
	}

	// Do not begin streaming if:
	//   1. Node is not the leader.
	//   2. Term is earlier than current term.
	//   3. Index is after the commit index.
	if l.state != Leader {
		return nil, ErrNotLeader
	} else if index > l.index {
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

	// Determine the highest snapshot index. The writer's snapshot index can
	// be higher if non-command entries have been applied.
	if writer.snapshotIndex > snapshotIndex {
		snapshotIndex = writer.snapshotIndex
	}
	snapshotIndex++

	// Write pending entries.
	if len(l.entries) > 0 {
		startIndex := l.entries[0].Index
		enc := NewLogEntryEncoder(writer.Writer)
		for _, e := range l.entries[snapshotIndex-startIndex:] {
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
		l.tracef("read from")

		// Decode single entry.
		var e LogEntry
		if err := dec.Decode(&e); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// If this is a config entry then update the config.
		if e.Type == logEntryConfig {
			l.tracef("read from: config")

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
			l.tracef("read from: snapshot")

			if err := l.FSM.Restore(r); err != nil {
				return err
			}
			l.tracef("read from: snapshot: restored")

			// Read the snapshot index off the end of the snapshot.
			var index uint64
			if err := binary.Read(r, binary.BigEndian, &index); err != nil {
				return fmt.Errorf("read snapshot index: %s", err)
			}
			l.tracef("read from: snapshot: index=%d", index)

			// Update the indicies.
			l.index = index
			l.commitIndex = index
			l.appliedIndex = index

			// Clear entries.
			l.entries = nil

			continue
		}

		// Append entry to the log.
		l.mu.Lock()
		if l.state == Stopped {
			l.mu.Unlock()
			return nil
		}
		l.tracef("read from: entry: index=%d / prev=%d / commit=%d", e.Index, l.index, l.commitIndex)
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
	if l.reader != nil {
		_ = l.reader.Close()
	}

	// Set new reader.
	l.reader = r
	return nil
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
