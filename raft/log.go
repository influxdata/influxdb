package raft

import (
	"encoding/binary"
	"io"
	"sync"
	"time"
)

// MaxLogEntrySize is the largest log entry that can be encoded.
const MaxLogEntrySize = 1 << 28 // 256MB

const logEntryHeaderSize = 4 + 8 + 8 // sz+index+term

// State represents whether the log is a follower, candidate, or leader.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Log represents a replicated log of commands.
type Log struct {
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

	mu      sync.Mutex
	path    string
	state   State
	offsets []int64

	// This interface is used to stub out time-based tests.
	// By default, it wraps the Go time package.
	time interface {
		Now() time.Time
	}
}

// Path returns the data path of the Raft log.
// Returns an empty string if the log is closed.
func (l *Log) Path() string { return l.path }

// State returns the current state.
func (l *Log) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// Open initializes the log from a path.
// If the path does not exist then it is created.
func (l *Log) Open(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Use the Go time package by default.
	if l.time == nil {
		l.time = &realtime{}
	}

	// TODO(benbjohnson): Create directory, if not exists.
	// TODO(benbjohnson): Restore peer list.
	// TODO(benbjohnson): Restore latest snapshot.
	// TODO(benbjohnson): Replay latest log.

	return nil
}

// Close closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// TODO(benbjohnson): Shutdown all goroutines.

	// Clear path.
	l.path = ""

	return nil
}

// AppendEntries appends a list of log entries to the log.
func (l *Log) AppendEntries(r *AppendEntriesRequest) (term int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return 0, nil // TODO(benbjohnson)
}

// RequestVote requests a vote from the log.
func (l *Log) RequestVote(r *VoteRequest) (term int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return 0, nil // TODO(benbjohnson)
}

// LogEntryType serves as an internal marker for log entries.
// Non-command entry types are handled by the library itself.
type LogEntryType uint8

const (
	LogEntryCommand LogEntryType = iota
	LogEntryNop
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
	if len(e.Data) > MaxLogEntrySize {
		return ErrLogEntryTooLarge
	}

	// Write header.
	var b [logEntryHeaderSize]byte
	binary.BigEndian.PutUint32(b[0:4], (uint32(e.Type)<<28)|uint32(len(e.Data)))
	binary.BigEndian.PutUint64(b[4:12], e.Index)
	binary.BigEndian.PutUint64(b[12:20], e.Term)
	if _, err := enc.w.Write(b[:]); err != nil {
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
	sz := binary.BigEndian.Uint32(b[0:4])
	e.Type, sz = LogEntryType(sz>>28), sz&0x0FFFFFFF
	e.Index = binary.BigEndian.Uint64(b[4:12])
	e.Term = binary.BigEndian.Uint64(b[12:20])

	// Read data.
	data := make([]byte, sz)
	if _, err := io.ReadFull(dec.r, data); err != nil {
		return err
	}
	e.Data = data

	return nil
}

// FSM represents the state machine that the log is applied to.
type FSM interface {
	Apply([]*LogEntry)
	Snapshot() error
	Restore() error
}

// realtime is a wrapper for the Go time package.
type realtime struct{}

func (r *realtime) Now() time.Time { return time.Now() }
