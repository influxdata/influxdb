package broker

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdb/influxdb/raft"
)

// Broker represents distributed messaging system segmented into topics.
// Each topic represents a linear series of events.
type Broker struct {
	mu   sync.Mutex
	path string // data directory

	log     *raft.Log          // internal raft log
	topics  map[string]*topic  // topic writers by path
	streams map[string]*Stream // stream by name
}

// New returns a new instance of a Broker with default values.
func New() *Broker {
	b := &Broker{
		log:     raft.NewLog(),
		topics:  make(map[string]*topic),
		streams: make(map[string]*Stream),
	}
	b.log.FSM = (*brokerFSM)(b)
	return b
}

// Path returns the path used when opening the broker.
// Returns empty string if the broker is not open.
func (b *Broker) Path() string { return b.path }

func (b *Broker) opened() bool { return b.path != "" }

// Open initializes the log.
// The broker then must be initialized or join a cluster before it can be used.
func (b *Broker) Open(path string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Require a non-blank path.
	if path == "" {
		return fmt.Errorf("path required")
	}
	b.path = path

	// TODO(wal): Initialize all topics.

	// Open underlying raft log.
	if err := b.log.Open(filepath.Join(path, "raft")); err != nil {
		return fmt.Errorf("raft: %s", err)
	}

	return nil
}

// Close closes the broker and all topics.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Return error if the broker is already closed.
	if !b.opened() {
		return fmt.Errorf("broker closed")
	}
	b.path = ""

	// TODO(wal): Close all topics.

	// Close raft log.
	_ = b.log.Close()

	return nil
}

// Initialize creates a new cluster.
func (b *Broker) Initialize() error {
	if err := b.log.Initialize(); err != nil {
		return fmt.Errorf("raft: %s", err)
	}
	return nil
}

// Publish writes a message to a topic.
// Returns the index of the message. Otherwise returns an error.
func (b *Broker) Publish(topic string, m *Message) (uint64, error) {
	assert(len(topic) < 256, "topic too long: %s", topic)

	// Encode type, topic, and data together.
	buf := make([]byte, 2+1+len(topic)+len(m.Data))
	binary.BigEndian.PutUint16(buf, uint16(m.Type))
	buf[2] = byte(len(topic))
	copy(buf[3:], []byte(topic))
	copy(buf[3+len(topic):], m.Data)

	// Apply to the raft log.
	return b.log.Apply(buf)
}

// Wait pauses until the given index has been applied.
func (b *Broker) Wait(index uint64) error {
	return b.log.Wait(index)
}

// Stream returns a stream by name.
func (b *Broker) Stream(name string) *Stream {
	return b.streams[name]
}

// CreateStream creates a new named stream.
func (b *Broker) CreateStream(name string) error {
	// Create message.
	var m Message
	m.Type = createStreamMessageType
	m.Data, _ = json.Marshal(&createStream{Name: name})

	// Write to the log.
	index, err := b.Publish("config", &m)
	if err != nil {
		return err
	}

	// Wait until applied.
	return b.log.Wait(index)
}

// RemoveStream deletes an existing stream by name.
func (b *Broker) RemoveStream(name string) error {
	// TODO: Add DeleteStream command to the log.
	// TODO: Wait until applied.
	return nil
}

// Returns the topic by name. Creates it if it doesn't exist.
func (b *Broker) createTopicIfNotExists(name string) (*topic, error) {
	// Return it if it already exists.
	if t := b.topics[name]; t != nil {
		return t, nil
	}

	// TODO: Disallow names starting with "raft" or containing "." or ".."

	// Otherwise create it.
	t := &topic{name: name}

	// Ensure the parent directory exists.
	path := filepath.Join(b.path, name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}

	// Open the writer to the on-disk file.
	f, err := os.OpenFile(filepath.Join(b.path, name), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	t.w = f

	// Cache the topic on the broker.
	b.topics[name] = t

	return t, nil
}

// brokerFSM implements the raft.FSM interface for the broker.
// This is implemented as a separate type because it is not meant to be exported.
type brokerFSM Broker

// Apply executes a raft log entry against the broker.
func (fsm *brokerFSM) Apply(e *raft.LogEntry) error {
	b := (*Broker)(fsm)

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

	// Update the broker configuration.
	var err error
	switch m.Type {
	case createStreamMessageType:
		err = fsm.applyCreateStream(&m)
	}
	if err != nil {
		return err
	}

	// Retrieve the topic.
	t, err := b.createTopicIfNotExists(topic)
	if err != nil {
		return err
	}

	// Write message to the topic.
	if err := t.writeMessage(&m); err != nil {
		return err
	}

	return nil
}

// applyCreateStream processes a createStream message.
func (fsm *brokerFSM) applyCreateStream(m *Message) error {
	var c createStream
	if err := json.Unmarshal(m.Data, &c); err != nil {
		return err
	}

	// Create a new named stream.
	fsm.streams[c.Name] = &Stream{
		broker:        (*Broker)(fsm),
		subscriptions: make(map[string]*subscription),
	}
	return nil
}

// Index returns the highest index that the broker has seen.
func (fsm *brokerFSM) Index() (uint64, error) {
	// TODO: Retrieve index.
	return 0, nil
}

// Snapshot streams the current state of the broker and returns the index.
func (fsm *brokerFSM) Snapshot(w io.Writer) (uint64, error) {
	// TODO: Prevent truncation during snapshot.
	// TODO: Lock and calculate header.
	// TODO: Retrieve snapshot index.
	// TODO: Stream each topic.
	return 0, nil
}

// Restore reads the broker state.
func (fsm *brokerFSM) Restore(r io.Reader) error {
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

// Stream represents a collection of subscriptions to topics on the broker.
// The stream maintains the highest index read for each topic so that the
// broker can use this high water mark for trimming the topic logs.
type Stream struct {
	broker        *Broker
	w             *streamWriter
	subscriptions map[string]*subscription
}

// Subscribe subscribes the stream to a given topic.
func (s *Stream) Subscribe(topic string, index uint64) error {
	// TODO: Add Subscribe command to the log.
	// TODO: Wait until applied.
	return nil
}

// Unsubscribe removes a subscription from the stream to a given topic.
func (s *Stream) Unsubscribe(topic string) error {
	// TODO: Add Unsubscribe command to the log.
	// TODO: Wait until applied.
	return nil
}

// WriteTo begins writing messages to a named stream.
// Only one writer is allowed on a stream at a time.
func (s *Stream) WriteTo(w io.Writer) (int, error) {
	// Close existing writer on stream.
	if s.w != nil {
		s.w.Close()
		s.w = nil
	}

	// Set a new writer on the stream.
	s.w = &streamWriter{w: w, done: make(chan struct{})}

	// TODO: Return bytes written.
	return 0, nil
}

type streamWriter struct {
	w    io.Writer
	done chan struct{}
}

// Close closes the writer.
func (w *streamWriter) Close() error {
	if w.done != nil {
		close(w.done)
		w.done = nil
	}
	return nil
}

// subscription represents a single topic subscription for a stream.
type subscription struct {
	name  string // topic name
	index uint64 // highest index received
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}
