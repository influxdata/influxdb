package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdb/influxdb/raft"
)

// Log represents a fast, append-only log that is segmented into smaller topics.
// Each topic represents a linear series of events.
type Log struct {
	mu   sync.Mutex
	path string // data directory

	log    *raft.Log         // internal raft log
	topics map[string]*topic // topic writers by path
}

// New returns a new instance of a Log with default values.
func NewLog() *Log {
	l := &Log{log: raft.NewLog()}
	l.log.FSM = (*fsm)(l)
	return l
}

// Path returns the path used when opening the log.
// Returns empty string if the log is not open.
func (l *Log) Path() string { return l.path }

func (l *Log) opened() bool { return l.path != "" }

// Open initializes the log.
// The log then must be initialized or join a cluster before it can be used.
func (l *Log) Open(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Require a non-blank path.
	if path == "" {
		return fmt.Errorf("path required")
	}
	l.path = path

	// TODO(wal): Initialize all topics.

	// Open underlying raft log.
	if err := l.log.Open(filepath.Join(path, "raft")); err != nil {
		return fmt.Errorf("raft: %s", err)
	}

	return nil
}

// Close closes the log and all topics.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Return error if the log is already closed.
	if !l.opened() {
		return fmt.Errorf("log closed")
	}
	l.path = ""

	// TODO(wal): Close all topics.

	// Close raft log.
	_ = l.log.Close()

	return nil
}

// Initialize creates a new cluster.
func (l *Log) Initialize() error {
	if err := l.log.Initialize(); err != nil {
		return fmt.Errorf("raft: %s", err)
	}
	return nil
}

// Write writes a message to a topic in the log.
// Returns the index if added to the log. Otherwise returns an error.
func (l *Log) Write(topic string, m *Message) (uint64, error) {
	assert(len(topic) < 256, "topic too long: %s", topic)

	// Encode type, topic, and data together.
	b := make([]byte, 2+1+len(topic)+len(m.Data))
	binary.BigEndian.PutUint16(b, uint16(m.Type))
	b[2] = byte(len(topic))
	copy(b[3:], []byte(topic))
	copy(b[3+len(topic):], m.Data)

	// Apply to the raft log.
	return l.log.Apply(b)
}

// Returns the topic by name. Creates it if it doesn't exist.
func (l *Log) createTopicIfNotExists(name string) (*topic, error) {
	// Return it if it already exists.
	if t := l.topics[name]; t != nil {
		return t, nil
	}

	// Otherwise create it and open its writer.
	t := &topic{name: name}
	f, err := os.OpenFile(filepath.Join(l.path, name), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	t.w = f

	// Add it to the log.
	l.topics[name] = t

	return t, nil
}

// fsm implements the raft.FSM interface for the log.
// This is implemented as a separate type because it is not meant to be exported.
type fsm Log

// Apply executes a raft log entry against the log.
func (fsm *fsm) Apply(e *raft.LogEntry) error {
	l := (*Log)(fsm)

	// Ignore internal raft entries.
	if e.Type != raft.LogEntryCommand {
		// TODO: Save index.
		return nil
	}

	// Extract topic from Raft entry.
	// The third byte is the topic length.
	// The fourth byte is the start of the topic.
	sz := e.Data[2]
	topic := string(e.Data[3 : 3+sz])

	// Create a message from the entry data.
	var m Message
	m.Type = MessageType(binary.BigEndian.Uint16(e.Data))
	m.Index = e.Index
	m.Data = e.Data[3+sz:]

	// Retrieve the topic.
	t, err := l.createTopicIfNotExists(topic)
	if err != nil {
		return err
	}

	// Write message to the topic.
	if err := t.writeMessage(&m); err != nil {
		return err
	}

	return nil
}

// Index returns the highest index that the log has seen.
func (fsm *fsm) Index() (uint64, error) {
	// TODO: Retrieve index.
	return 0, nil
}

// Snapshot streams the current state of the log and returns the index.
func (fsm *fsm) Snapshot(w io.Writer) (uint64, error) {
	// TODO: Prevent truncation during snapshot.
	// TODO: Lock and calculate header.
	// TODO: Retrieve snapshot index.
	// TODO: Stream each topic.
	return 0, nil
}

// Restore reads the log state.
func (fsm *fsm) Restore(r io.Reader) error {
	// TODO: Read header.
	// TODO: Read in each file.
	return nil
}

// topic represents a single named stream of messages.
// Each topic is identified by a unique path.
type topic struct {
	name string    // unique identifier (and on-disk path)
	w    io.Writer // on-disk representation
	enc  *MessageEncoder
}

// write writes a message to the end of the topic.
func (t *topic) writeMessage(m *Message) error {
	// TODO
	return nil
}

/*
// A list of message type flags.
const (
	ConfigCommandType   = 1 << 16
	DatabaseCommandType = 1 << 15
	ShardCommandType    = 1 << 14
)

const (
	SetContinuousQueryTimestampCommandID = ConfigCommandType | 1
	SaveClusterAdminCommandID            = ConfigCommandType | 2

	JoinCommandID                   = ConfigCommandType | 3
	ForceLeaveCommandID             = ConfigCommandType | 4
	ChangeConnectionStringCommandID = ConfigCommandType | 5
)

const (
	CreateDatabaseCommandID = DatabaseCommandType | 1
	DropDatabaseCommandID   = DatabaseCommandType | 2

	CreateShardSpaceCommandID = DatabaseCommandType | 3
	DropShardSpaceCommandID   = DatabaseCommandType | 4
	UpdateShardSpaceCommandID = DatabaseCommandType | 5

	SaveDbUserCommandID     = DatabaseCommandType | 6
	ChangeDbUserPassword    = DatabaseCommandType | 7
	ChangeDbUserPermissions = DatabaseCommandType | 8

	DropSeriesCommandID           = DatabaseCommandType | 9
	CreateSeriesFieldIdsCommandID = DatabaseCommandType | 10

	CreateContinuousQueryCommandID = DatabaseCommandType | 11
	DeleteContinuousQueryCommandID = DatabaseCommandType | 12
)

//CreateShardsCommandID
//DropShardCommandID

*/

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}
