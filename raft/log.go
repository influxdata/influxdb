package raft

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

// FSM represents the state machine that the log is applied to.
type FSM interface {
	Apply([]*LogEntry)
	Snapshot(io.Writer) error
	Restore(io.Reader) error
}

const logEntryHeaderSize = 8 + 8 + 8 // sz+index+term

// State represents whether the log is a follower, candidate, or leader.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Log represents a replicated log of commands.
type Log struct {
	mu sync.Mutex

	id     uint64  // unique log identifier
	path   string  // data directory
	state  State   // current node state
	config *Config // cluster configuration

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

	// Clock is an abstraction of the time package. By default it will use
	// a real-time clock but a mock clock can be used for testing.
	Clock clock.Clock
}

// Path returns the data path of the Raft log.
// Returns an empty string if the log is closed.
func (l *Log) Path() string { return l.path }

// Opened returns true if the log is currently open.
func (l *Log) Opened() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.opened()
}

func (l *Log) opened() bool { return l.path != "" }

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

	// Do not allow an open log to be reopened.
	if l.opened() {
		return ErrAlreadyOpen
	}

	// Create directory, if not exists.
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	l.path = path

	// Use the realtime clock by default.
	if l.Clock == nil {
		l.Clock = clock.New()
	}

	// Read config.
	if err := l.restoreConfig(); err != nil {
		_ = l.close()
		return err
	}

	// TEMP(benbjohnson): Create empty log segment.
	l.segment = &segment{
		path:  filepath.Join(l.path, "1.log"),
		index: 0,
	}

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
	// TODO(benbjohnson): Shutdown all goroutines.

	// Clear path.
	l.path = ""

	return nil
}

// restoreConfig reads the configuration from disk and marshals it into a Config.
func (l *Log) restoreConfig() error {
	// Read config from disk.
	f, err := os.Open(filepath.Join(l.path, "config"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	defer func() { _ = f.Close() }()

	// Marshal file to a config type.
	var config *Config
	if f != nil {
		if err := json.NewDecoder(f).Decode(&config); err != nil {
			return err
		}
	}

	// Set config.
	l.config = config

	return nil
}

// Heartbeat establishes dominance by the current leader.
// Returns the current term and highest written log entry index.
func (l *Log) Heartbeat(term, commitIndex, leaderID uint64) (uint64, uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return 0, 0, ErrClosed
	}

	// Ignore if the incoming term is less than the log's term.
	if term < l.currentTerm {
		return l.currentTerm, l.currentIndex, nil
	}

	if term > l.currentTerm {
		// TODO(benbjohnson): stepdown
		l.currentTerm = term
	}
	l.commitIndex = commitIndex
	l.leaderID = leaderID

	return l.currentTerm, l.currentIndex, nil
}

// RequestVote requests a vote from the log.
func (l *Log) RequestVote(term, candidateId, lastLogIndex, lastLogTerm uint64) (uint64, error) {
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
	} else if term == l.currentTerm && l.votedFor != 0 && l.votedFor != candidateId {
		return l.currentTerm, ErrAlreadyVoted
	} else if lastLogTerm < l.currentTerm {
		return l.currentTerm, ErrOutOfDateLog
	} else if lastLogTerm == l.currentTerm && lastLogIndex < l.currentIndex {
		return l.currentTerm, ErrOutOfDateLog
	}

	// Vote for candidate.
	l.votedFor = candidateId

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

		// Do not begin streaming if:
		//   1. Node is not the leader.
		//   2. Term is earlier than current term.
		//   3. Index is after the commit index.
		if l.state != Leader {
			return ErrNotLeader
		} else if term != l.currentTerm {
			return ErrStaleTerm
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
	if err := l.segment.writeTo(w, index); err != nil {
		return err
	}

	return nil
}

// ReadFrom continually reads log entries from a reader.
func (l *Log) ReadFrom(r io.ReadCloser) error {
	l.mu.Lock()

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
	l.mu.Unlock()

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
	}
}

// append requests a vote from the log.
func (l *Log) append(e *LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return ErrClosed
	}

	// TODO(benbjohnson): Write to the end of the log.

	return nil
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

	// TEMP: Move to leader.
	l.state = Leader
	return nil
}

// LogEntryType serves as an internal marker for log entries.
// Non-command entry types are handled by the library itself.
type LogEntryType uint8

const (
	LogEntryCommand LogEntryType = iota
	LogEntryNop
	LogEntryConfig
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

	f   *os.File // on-disk representation
	buf []byte   // in-memory cache, nil means uncached

	writers []*segmentWriter // segment tailing
}

// seal sets the segment as sealed.
func (s *segment) seal() {
	s.mu.Lock()
	defer s.mu.Lock()
	s.sealed = true
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

	// Save offset.
	s.offsets = append(s.offsets, offset)

	return nil
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

		// Add segment reader to segment, if not sealed.
		if !s.sealed {
			writer = &segmentWriter{w, make(chan error)}
			s.writers = append(s.writers, writer)
		}

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
	w  io.Writer
	ch chan error
}

// Config represents the configuration for the log.
type Config struct {
	// List of peers in the cluster.
	Peers []*url.URL `json:"peers,omitempty"`
}

// UnmarshalJSON parses a JSON-formatted byte slice into a Config instance.
func (c *Config) UnmarshalJSON(data []byte) error {
	var o struct {
		Peers []string `json:"peers"`
	}

	// Unmarshal into temporary type.
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Convert data to Config type.
	for _, peer := range o.Peers {
		u, err := url.Parse(peer)
		if err != nil {
			return err
		}
		c.Peers = append(c.Peers, u)
	}

	return nil
}

// MarshalJSON converts a Config into a JSON-formatted byte slice.
func (c *Config) MarshalJSON() ([]byte, error) {
	var o struct {
		Peers []string `json:"peers"`
	}

	// Convert to temporary type.
	for _, u := range c.Peers {
		o.Peers = append(o.Peers, u.String())
	}

	return json.Marshal(&o)
}
