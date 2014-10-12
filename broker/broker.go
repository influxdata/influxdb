package broker

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/influxdb/influxdb/raft"
)

const configTopicID = uint32(0)
const configTopicName = "config"

// Broker represents distributed messaging system segmented into topics.
// Each topic represents a linear series of events.
type Broker struct {
	mu   sync.RWMutex
	path string    // data directory
	log  *raft.Log // internal raft log

	streams map[string]*Stream // stream by name

	maxTopicID   uint32            // autoincrementing sequence
	topics       map[uint32]*topic // topics by id
	topicsByName map[string]*topic // topics by name
}

// New returns a new instance of a Broker with default values.
func New() *Broker {
	b := &Broker{
		log:          raft.NewLog(),
		streams:      make(map[string]*Stream),
		topics:       make(map[uint32]*topic),
		topicsByName: make(map[string]*topic),
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
		return ErrPathRequired
	}
	b.path = path

	// Initialize config topic.
	b.initTopic(configTopicID, configTopicName)

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
		return ErrClosed
	}
	b.path = ""

	// TODO: Close all topics.

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
	// Retrieve topic by name.
	b.mu.RLock()
	t := b.topicsByName[topic]
	b.mu.RUnlock()

	// Ensure topic exists.
	if t == nil {
		return 0, errors.New("topic not found")
	}

	return b.log.Apply(encodeTopicMessage(t.id, m))
}

// publishConfig writes a configuration change to the config topic.
// This always waits until the change is applied to the log.
func (b *Broker) publishConfig(m *Message) error {
	// Encode topic, type, and data together.
	buf := encodeTopicMessage(configTopicID, m)

	// Apply to the raft log.
	index, err := b.log.Apply(buf)
	if err != nil {
		return err
	}

	// Wait for index.
	return b.log.Wait(index)
}

// Wait pauses until the given index has been applied.
func (b *Broker) Wait(index uint64) error {
	return b.log.Wait(index)
}

// Stream returns a stream by name.
func (b *Broker) Stream(name string) *Stream {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.streams[name]
}

// CreateTopic creates a new topic.
func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure topic doesn't already exist.
	t := b.topicsByName[name]
	if t != nil {
		return errors.New("topic already exists")
	}

	// Generate the next id.
	id := b.maxTopicID + 1

	// Add command to create the topic.
	return b.publishConfig(&Message{
		Type: CreateTopicMessageType,
		Data: jsonify(&CreateTopicCommand{ID: id, Name: name}),
	})
}

// applyCreateTopic is called when the CreateTopicCommand is applied.
func (b *Broker) applyCreateTopic(id uint32, name string) {
	assert(b.topics[id] == nil, "duplicate topic id exists: %d", id)
	assert(b.topicsByName[name] == nil, "duplicate topic name exists: %s", name)
	b.initTopic(id, name)
}

// initializes a new topic object.
func (b *Broker) initTopic(id uint32, name string) {
	t := &topic{
		id:      id,
		name:    name,
		path:    filepath.Join(b.path, name),
		writers: make(map[string]*streamWriter),
	}
	b.topics[t.id] = t
	b.topicsByName[t.name] = t
}

// DeleteTopic deletes an existing topic.
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find topic.
	if t := b.topicsByName[name]; t == nil {
		return errors.New("topic not found")
	}

	// Add command to remove.
	return b.publishConfig(&Message{
		Type: DeleteTopicMessageType,
		Data: jsonify(&DeleteTopicCommand{Name: name}),
	})
}

// applyDeleteTopic is called when the DeleteTopicCommand is applied.
func (b *Broker) applyDeleteTopic(name string) {
	t := b.topicsByName[name]
	assert(t != nil, "topic missing: %s", name)

	// Close topic.
	_ = t.Close()

	// Remove subscriptions.
	for _, s := range b.streams {
		delete(s.topics, name)
	}

	// Remove from lookups.
	delete(b.topics, t.id)
	delete(b.topicsByName, t.name)
}

// CreateStream creates a new named stream.
func (b *Broker) CreateStream(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure stream doesn't already exist.
	s := b.streams[name]
	if s != nil {
		return errors.New("stream already exists")
	}

	// Add command to create stream.
	return b.publishConfig(&Message{
		Type: CreateStreamMessageType,
		Data: jsonify(&CreateStreamCommand{Name: name}),
	})
}

// applyCreateStream is called when the CreateStreamCommand is applied.
func (b *Broker) applyCreateStream(name string) {
	s := &Stream{
		broker: b,
		name:   name,
		topics: make(map[string]uint64),
	}

	// Automatically subscribe to the config topic.
	t := b.topics[configTopicID]
	assert(t != nil, "config topic missing")
	s.topics[configTopicName] = t.index

	// Add stream to the broker.
	b.streams[name] = s
}

// DeleteStream deletes an existing stream by name.
func (b *Broker) DeleteStream(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure stream exists.
	if s := b.streams[name]; s == nil {
		return errors.New("stream not found")
	}

	// Issue command to remove stream.
	return b.publishConfig(&Message{
		Type: DeleteStreamMessageType,
		Data: jsonify(&DeleteStreamCommand{Name: name}),
	})
}

// applyDeleteStream is called when the DeleteStreamCommand is applied.
func (b *Broker) applyDeleteStream(name string) {
	panic("not yet implemented")
}

// Subscribe adds a subscription to a topic from a stream.
func (b *Broker) Subscribe(stream string, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure stream & topic exist.
	if b.streams[stream] == nil {
		return errors.New("stream not found")
	} else if b.topicsByName[topic] == nil {
		return errors.New("topic not found")
	}

	// Issue command to subscribe to topic.
	return b.publishConfig(&Message{
		Type: SubscribeMessageType,
		Data: jsonify(&SubscribeCommand{Stream: stream, Topic: topic}),
	})
}

// applySubscribe is called when the SubscribeCommand is applied.
func (b *Broker) applySubscribe(stream string, topic string) error {
	// Retrieve stream.
	s := b.streams[stream]
	assert(s != nil, "stream not found: %s", stream)

	// Retrieve topic.
	t := b.topicsByName[topic]
	assert(t != nil, "topic not found: %s", topic)

	// Save current index on topic.
	index := t.index

	// Ensure topic is not already subscribed to.
	if _, ok := s.topics[t.name]; ok {
		warn("already subscribed to topic")
		return nil
	}

	// Add subscription to stream.
	s.topics[t.name] = index

	// Add tailing stream writer to topic.
	if s.writer != nil {
		if err := t.addWriter(s.name, s.writer, index); err != nil {
			return fmt.Errorf("add writer: %s", err)
		}
	}

	return nil
}

// Unsubscribe removes a subscription for a topic from a stream.
func (b *Broker) Unsubscribe(stream string, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure stream & topic exist.
	if b.streams[stream] == nil {
		return errors.New("stream not found")
	} else if b.topicsByName[topic] == nil {
		return errors.New("topic not found")
	}

	// Issue command to unsubscribe from topic.
	return b.publishConfig(&Message{
		Type: UnsubscribeMessageType,
		Data: jsonify(&UnsubscribeCommand{Stream: stream, Topic: topic}),
	})
}

// applyUnsubscribe is called when the UnsubscribeCommand is applied.
func (b *Broker) applyUnsubscribe(stream string, topic string) {
	// Retrieve stream.
	s := b.streams[stream]
	assert(s != nil, "stream not found: %s", stream)

	// Retrieve topic.
	t := b.topicsByName[topic]
	assert(t != nil, "topic not found: %d", topic)

	// Remove subscription.
	delete(s.topics, topic)

	// Remove stream writer.
	t.removeWriter(stream)
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

	// Decode the topic message from the raft log.
	topicID, m := decodeTopicMessage(e.Data)
	m.Index = e.Index

	// Find topic by id. Ignore topic if it doesn't exist.
	t := b.topics[topicID]
	assert(t != nil, "topic not found: %d", topicID)

	// Update the broker configuration.
	switch m.Type {
	case CreateTopicMessageType:
		var c CreateTopicCommand
		if err := json.Unmarshal(m.Data, &c); err != nil {
			return fmt.Errorf("unmarshal create topic: %s", err)
		}
		b.applyCreateTopic(c.ID, c.Name)

	case DeleteTopicMessageType:
		var c DeleteTopicCommand
		if err := json.Unmarshal(m.Data, &c); err != nil {
			return fmt.Errorf("unmarshal delete topic: %s", err)
		}
		b.applyDeleteTopic(c.Name)

	case CreateStreamMessageType:
		var c CreateStreamCommand
		if err := json.Unmarshal(m.Data, &c); err != nil {
			return fmt.Errorf("unmarshal create stream: %s", err)
		}
		b.applyCreateStream(c.Name)

	case DeleteStreamMessageType:
		var c DeleteStreamCommand
		if err := json.Unmarshal(m.Data, &c); err != nil {
			return fmt.Errorf("unmarshal delete stream: %s", err)
		}
		b.applyDeleteStream(c.Name)

	case SubscribeMessageType:
		var c SubscribeCommand
		if err := json.Unmarshal(m.Data, &c); err != nil {
			return fmt.Errorf("unmarshal subscribe: %s", err)
		}
		b.applySubscribe(c.Stream, c.Topic)

	case UnsubscribeMessageType:
		var c UnsubscribeCommand
		if err := json.Unmarshal(m.Data, &c); err != nil {
			return fmt.Errorf("unmarshal unsubscribe: %s", err)
		}
		b.applyUnsubscribe(c.Stream, c.Topic)
	}

	// Write to the topic.
	if err := t.encode(m); err != nil {
		return fmt.Errorf("encode: %s", err)
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

// encodes a topic id and message together for the raft log.
func encodeTopicMessage(topicID uint32, m *Message) []byte {
	b := make([]byte, 4+2+len(m.Data))
	binary.BigEndian.PutUint32(b, topicID)
	binary.BigEndian.PutUint16(b[4:6], uint16(m.Type))
	copy(b[6:], m.Data)
	return b
}

// decodes a topic id and message together from the raft log.
func decodeTopicMessage(b []byte) (topicID uint32, m *Message) {
	topicID = binary.BigEndian.Uint32(b[0:4])
	m = &Message{
		Type: MessageType(binary.BigEndian.Uint16(b[4:6])),
		Data: b[6:],
	}
	return
}

// topic represents a single named stream of messages.
// Each topic is identified by a unique path.
type topic struct {
	mu    sync.RWMutex
	id    uint32 // unique identifier
	name  string // unique name
	index uint64 // highest index written
	path  string // on-disk path

	file *os.File // on-disk representation

	writers map[string]*streamWriter // tailing stream writers by stream name
}

// open opens a topic for writing.
func (t *topic) open() error {
	if t.file != nil {
		return fmt.Errorf("topic already open")
	}

	// Ensure the parent directory exists.
	if err := os.MkdirAll(filepath.Dir(t.path), 0700); err != nil {
		return err
	}

	// Open the writer to the on-disk file.
	f, err := os.OpenFile(t.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	t.file = f

	return nil
}

// close closes the underlying file and all stream writers.
func (t *topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Close file.
	if t.file != nil {
		_ = t.file.Close()
		t.file = nil
	}

	// Close all stream writers.
	for _, w := range t.writers {
		_ = w.Close()
	}

	return nil
}

// addWriter catches up a stream writer starting from the given index
// and adds the writer to the list of caught up writers.
func (t *topic) addWriter(name string, w *streamWriter, index uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Remove previous stream writer.
	t.removeWriter(name)

	// TODO: If index is too old then return an error.

	// TODO: Only catch up if index is behind head.

	// Open topic file for reading.
	f, err := os.Open(t.path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Stream out all messages until EOF.
	dec := NewMessageDecoder(f)
	for {
		// Decode message.
		var m Message
		if err := dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode: %s", err)
		}

		// Ignore message if it's on or before high water mark.
		if m.Index <= index {
			continue
		}

		// Write message out to stream.
		if _, err := m.WriteTo(w); err != nil {
			return fmt.Errorf("write to: %s", err)
		}
	}

	// Add writer to tail the the topic.
	t.writers[name] = w
	return nil
}

// removeWriter removes a steam writer from the tailing writers on the topic.
func (t *topic) removeWriter(name string) {
	if w := t.writers[name]; w != nil {
		_ = w.Close()
		delete(t.writers, name)
	}
}

// encode writes a message to the end of the topic.
func (t *topic) encode(m *Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Ensure the topic is open and ready for writing.
	if t.file == nil {
		if err := t.open(); err != nil {
			return fmt.Errorf("open: %s", err)
		}
	}

	// Ensure message is in-order.
	assert(m.Index > t.index, "topic message out of order: %d -> %d", t.index, m.Index)

	// Encode message.
	b := make([]byte, messageHeaderSize+len(m.Data))
	copy(b, m.header())
	copy(b[messageHeaderSize:], m.Data)

	// Write to topic file.
	if _, err := t.file.Write(b); err != nil {
		return fmt.Errorf("encode header: %s", err)
	}

	// Move up high water mark on the topic.
	t.index = m.Index

	// Write message out to all attached stream writers.
	for name, w := range t.writers {
		if _, err := w.Write(b); err != nil {
			t.removeWriter(name)
		}
	}

	return nil
}

// Stream represents a collection of subscriptions to topics on the broker.
// The stream maintains the highest index read for each topic so that the
// broker can use this high water mark for trimming the topic logs.
type Stream struct {
	name   string
	broker *Broker
	writer *streamWriter

	topics map[string]uint64 // current index for each subscribed topic
}

// Subscribe adds a subscription to a topic for the stream.
func (s *Stream) Subscribe(topic string) error { return s.broker.Subscribe(s.name, topic) }

// Unsubscribe removes a subscription from the stream.
func (s *Stream) Unsubscribe(topic string) error { return s.broker.Unsubscribe(s.name, topic) }

// WriteTo begins writing messages to a named stream.
// Only one writer is allowed on a stream at a time.
func (s *Stream) WriteTo(w io.Writer) (int, error) {
	writer := newStreamWriter(w)

	// Close existing writer on stream.
	if s.writer != nil {
		s.writer.Close()
		s.writer = nil
	}

	// Set a new writer on the stream.
	s.writer = writer

	// Create a topic list with the "config" topic first.
	names := make([]string, 1, len(s.topics))
	names[0] = configTopicName
	for name := range s.topics {
		if name != configTopicName {
			names = append(names, name)
		}
	}
	sort.Strings(names[1:])

	// Catch up and attach writer to all subscribed topics.
	for _, name := range names {
		t := s.broker.topicsByName[name]
		assert(t != nil, "subscription topic missing: %s", name)
		if err := t.addWriter(s.name, writer, s.topics[name]); err != nil {
			return 0, fmt.Errorf("add stream writer: %s", err)
		}
	}

	// Wait for writer to close and return.
	<-writer.done
	return 0, nil
}

// streamWriter represents a writer that blocks Stream.WriteTo until it's closed.
type streamWriter struct {
	io.Writer
	done chan struct{}
}

// newStreamWriter returns an instance of streamWriter that wraps an io.Writer.
func newStreamWriter(w io.Writer) *streamWriter {
	return &streamWriter{Writer: w, done: make(chan struct{})}
}

// Close closes the writer.
func (w *streamWriter) Close() error {
	if w.done != nil {
		close(w.done)
		w.done = nil
	}
	return nil
}

// CreateTopicCommand creates a new named topic.
type CreateTopicCommand struct {
	ID   uint32 `json:"id"`   // topic id
	Name string `json:"name"` // topic name
}

// DeleteTopicCommand removes a topic by ID.
type DeleteTopicCommand struct {
	Name string `json:"name"`
}

// CreateStream creates a new named stream.
type CreateStreamCommand struct {
	Name string `json:"name"`
}

// DeleteStreamCommand removes a stream by name.
type DeleteStreamCommand struct {
	Name string `json:"name"`
}

// SubscribeCommand subscribes a stream to a new topic.
type SubscribeCommand struct {
	Stream string `json:"stream"` // stream name
	Topic  string `json:"topic"`  // topic name
}

// UnsubscribeCommand removes a subscription for a topic from a stream.
type UnsubscribeCommand struct {
	Stream string `json:"stream"` // stream name
	Topic  string `json:"topic"`  // topic name
}

// jsonify marshals a value to a JSON-encoded byte slice.
// This should only be used with internal data that will not return marshal errors.
func jsonify(v interface{}) []byte {
	b, err := json.Marshal(v)
	assert(err == nil, "json marshal error: %s", err)
	return b
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
