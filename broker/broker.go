package broker

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
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

	replicas map[string]*Replica // replica by name

	maxTopicID   uint32            // autoincrementing sequence
	topics       map[uint32]*topic // topics by id
	topicsByName map[string]*topic // topics by name
}

// New returns a new instance of a Broker with default values.
func New() *Broker {
	b := &Broker{
		log:          raft.NewLog(),
		replicas:     make(map[string]*Replica),
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

	// Close all replicas.
	for _, r := range b.replicas {
		r.closeWriter()
	}

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
		return 0, ErrTopicNotFound
	}

	// Attach topic id to the message and encode.
	m.TopicID = t.id
	buf, _ := m.MarshalBinary()
	return b.log.Apply(buf)
}

// publishConfig writes a configuration change to the config topic.
// This always waits until the change is applied to the log.
func (b *Broker) publishConfig(m *Message) error {
	// Encode topic, type, and data together.
	m.TopicID = configTopicID
	buf, _ := m.MarshalBinary()

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

// Replica returns a replica by name.
func (b *Broker) Replica(name string) *Replica {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.replicas[name]
}

// CreateTopic creates a new topic.
func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure topic doesn't already exist.
	t := b.topicsByName[name]
	if t != nil {
		return ErrTopicExists
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
		id:       id,
		name:     name,
		path:     filepath.Join(b.path, name),
		replicas: make(map[string]*Replica),
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
		return ErrTopicNotFound
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
	for _, s := range b.replicas {
		delete(s.topics, name)
	}

	// Remove from lookups.
	delete(b.topics, t.id)
	delete(b.topicsByName, t.name)
}

// CreateReplica creates a new named replica.
func (b *Broker) CreateReplica(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica doesn't already exist.
	s := b.replicas[name]
	if s != nil {
		return ErrReplicaExists
	}

	// Add command to create replica.
	return b.publishConfig(&Message{
		Type: CreateReplicaMessageType,
		Data: jsonify(&CreateReplicaCommand{Name: name}),
	})
}

// applyCreateReplica is called when the CreateReplicaCommand is applied.
func (b *Broker) applyCreateReplica(name string) {
	r := &Replica{
		broker: b,
		name:   name,
		topics: make(map[string]uint64),
	}

	// Automatically subscribe to the config topic.
	t := b.topics[configTopicID]
	assert(t != nil, "config topic missing")
	r.topics[configTopicName] = t.index

	// Add replica to the broker.
	b.replicas[name] = r
}

// DeleteReplica deletes an existing replica by name.
func (b *Broker) DeleteReplica(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica exists.
	if s := b.replicas[name]; s == nil {
		return ErrReplicaNotFound
	}

	// Issue command to remove replica.
	return b.publishConfig(&Message{
		Type: DeleteReplicaMessageType,
		Data: jsonify(&DeleteReplicaCommand{Name: name}),
	})
}

// applyDeleteReplica is called when the DeleteReplicaCommand is applied.
func (b *Broker) applyDeleteReplica(name string) {
	// Find replica.
	r := b.replicas[name]
	assert(r != nil, "replica missing: %s", name)

	// Remove replica from all subscribed topics.
	for topic := range r.topics {
		if t := b.topicsByName[topic]; t != nil {
			delete(t.replicas, name)
		}
	}
	r.topics = make(map[string]uint64)

	// Close replica's writer.
	r.closeWriter()

	// Remove replica from broker.
	delete(b.replicas, name)
}

// Subscribe adds a subscription to a topic from a replica.
func (b *Broker) Subscribe(replica string, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica & topic exist.
	if b.replicas[replica] == nil {
		return ErrReplicaNotFound
	} else if b.topicsByName[topic] == nil {
		return ErrTopicNotFound
	}

	// Issue command to subscribe to topic.
	return b.publishConfig(&Message{
		Type: SubscribeMessageType,
		Data: jsonify(&SubscribeCommand{Replica: replica, Topic: topic}),
	})
}

// applySubscribe is called when the SubscribeCommand is applied.
func (b *Broker) applySubscribe(replica string, topic string) error {
	// Retrieve replica.
	r := b.replicas[replica]
	assert(r != nil, "replica not found: %s", replica)

	// Retrieve topic.
	t := b.topicsByName[topic]
	assert(t != nil, "topic not found: %s", topic)

	// Save current index on topic.
	index := t.index

	// Ensure topic is not already subscribed to.
	if _, ok := r.topics[t.name]; ok {
		warn("already subscribed to topic")
		return nil
	}

	// Add subscription to replica.
	r.topics[t.name] = index
	t.replicas[r.name] = r

	// Catch up replica.
	t.writeTo(r, index)

	return nil
}

// Unsubscribe removes a subscription for a topic from a replica.
func (b *Broker) Unsubscribe(replica string, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica & topic exist.
	if b.replicas[replica] == nil {
		return ErrReplicaNotFound
	} else if b.topicsByName[topic] == nil {
		return ErrTopicNotFound
	}

	// Issue command to unsubscribe from topic.
	return b.publishConfig(&Message{
		Type: UnsubscribeMessageType,
		Data: jsonify(&UnsubscribeCommand{Replica: replica, Topic: topic}),
	})
}

// applyUnsubscribe is called when the UnsubscribeCommand is applied.
func (b *Broker) applyUnsubscribe(replica string, topic string) {
	// Retrieve replica.
	s := b.replicas[replica]
	assert(s != nil, "replica not found: %s", replica)

	// Retrieve topic.
	t := b.topicsByName[topic]
	assert(t != nil, "topic not found: %d", topic)

	// Remove subscription from replica and topic.
	delete(s.topics, topic)
	delete(t.replicas, replica)
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

	// Decode the message from the raft log.
	m := &Message{}
	err := m.UnmarshalBinary(e.Data)
	assert(err == nil, "message unmarshal: %s", err)

	// Add the raft index to the message.
	m.Index = e.Index

	// Find topic by id. Ignore topic if it doesn't exist.
	t := b.topics[m.TopicID]
	assert(t != nil, "topic not found: %d", m.TopicID)

	// Update the broker configuration.
	switch m.Type {
	case CreateTopicMessageType:
		var c CreateTopicCommand
		err := json.Unmarshal(m.Data, &c)
		assert(err == nil, "unmarshal create topic: %s", err)
		b.applyCreateTopic(c.ID, c.Name)

	case DeleteTopicMessageType:
		var c DeleteTopicCommand
		err := json.Unmarshal(m.Data, &c)
		assert(err == nil, "unmarshal delete topic: %s", err)
		b.applyDeleteTopic(c.Name)

	case CreateReplicaMessageType:
		var c CreateReplicaCommand
		err := json.Unmarshal(m.Data, &c)
		assert(err == nil, "unmarshal create replica: %s", err)
		b.applyCreateReplica(c.Name)

	case DeleteReplicaMessageType:
		var c DeleteReplicaCommand
		err := json.Unmarshal(m.Data, &c)
		assert(err == nil, "unmarshal delete replica: %s", err)
		b.applyDeleteReplica(c.Name)

	case SubscribeMessageType:
		var c SubscribeCommand
		err := json.Unmarshal(m.Data, &c)
		assert(err == nil, "unmarshal subscribe: %s", err)
		b.applySubscribe(c.Replica, c.Topic)

	case UnsubscribeMessageType:
		var c UnsubscribeCommand
		err := json.Unmarshal(m.Data, &c)
		assert(err == nil, "unmarshal unsubscribe: %s", err)
		b.applyUnsubscribe(c.Replica, c.Topic)
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

// topic represents a single named queue of messages.
// Each topic is identified by a unique path.
type topic struct {
	id    uint32 // unique identifier
	name  string // unique name
	index uint64 // highest index written
	path  string // on-disk path

	file *os.File // on-disk representation

	replicas map[string]*Replica // replicas subscribed to topic
}

// open opens a topic for writing.
func (t *topic) open() error {
	assert(t.file == nil, "topic already open: %s", t.name)

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

// close closes the underlying file.
func (t *topic) Close() error {
	// Close file.
	if t.file != nil {
		_ = t.file.Close()
		t.file = nil
	}
	return nil
}

// writeTo writes the topic to a replica since a given index.
// Returns an error if the starting index is unavailable.
func (t *topic) writeTo(r *Replica, index uint64) (int, error) {
	// TODO: If index is too old then return an error.

	// Open topic file for reading.
	f, err := os.Open(t.path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// Stream out all messages until EOF.
	total := 0
	dec := NewMessageDecoder(bufio.NewReader(f))
	for {
		// Decode message.
		var m Message
		if err := dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			return total, fmt.Errorf("decode: %s", err)
		}

		// Ignore message if it's on or before high water mark.
		if m.Index <= index {
			continue
		}

		// Write message out to stream.
		n, err := m.WriteTo(r)
		if err != nil {
			return total, fmt.Errorf("write to: %s", err)
		}
		total += n
	}

	return total, nil
}

// encode writes a message to the end of the topic.
func (t *topic) encode(m *Message) error {
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
	copy(b, m.marshalHeader())
	copy(b[messageHeaderSize:], m.Data)

	// Write to topic file.
	if _, err := t.file.Write(b); err != nil {
		return fmt.Errorf("encode header: %s", err)
	}

	// Move up high water mark on the topic.
	t.index = m.Index

	// Write message out to all replicas.
	for _, r := range t.replicas {
		_, _ = r.Write(b)
	}

	return nil
}

// Replica represents a collection of subscriptions to topics on the broker.
// The replica maintains the highest index read for each topic so that the
// broker can use this high water mark for trimming the topic logs.
type Replica struct {
	name   string
	url    *url.URL // TODO
	broker *Broker

	writer io.Writer     // currently attached writer
	done   chan struct{} // notify when current writer is removed

	topics map[string]uint64 // current index for each subscribed topic
}

// closeWriter removes the writer on the replica and closes the notify channel.
func (r *Replica) closeWriter() {
	if r.writer != nil {
		r.writer = nil
		close(r.done)
		r.done = nil
	}
}

// Subscribe adds a subscription to a topic for the replica.
func (r *Replica) Subscribe(topic string) error { return r.broker.Subscribe(r.name, topic) }

// Unsubscribe removes a subscription from the stream.
func (r *Replica) Unsubscribe(topic string) error { return r.broker.Unsubscribe(r.name, topic) }

// Topics returns a list of topic names that the replica is subscribed to.
func (r *Replica) Topics() []string {
	a := make([]string, 0, len(r.topics))
	for name := range r.topics {
		a = append(a, name)
	}
	sort.Strings(a)
	return a
}

// Write writes a byte slice to the underlying writer.
// If no writer is available then ErrReplicaUnavailable is returned.
func (r *Replica) Write(p []byte) (int, error) {
	// Check if there's a replica available.
	if r.writer == nil {
		return 0, errReplicaUnavailable
	}

	// If an error occurs on the write then remove the writer.
	n, err := r.writer.Write(p)
	if err != nil {
		r.closeWriter()
		return n, errReplicaUnavailable
	}

	// If the writer has a flush method then call it.
	if w, ok := r.writer.(flusher); ok {
		w.Flush()
	}

	return n, nil
}

// WriteTo begins writing messages to a named stream.
// Only one writer is allowed on a stream at a time.
func (r *Replica) WriteTo(w io.Writer) (int, error) {
	// Close previous writer, if set.
	r.closeWriter()

	// Set a new writer on the replica.
	r.writer = w
	done := make(chan struct{})
	r.done = done

	// Create a topic list with the "config" topic first.
	// Configuration changes need to be propagated to make sure topics exist.
	names := make([]string, 1, len(r.topics))
	names[0] = configTopicName
	for name := range r.topics {
		if name != configTopicName {
			names = append(names, name)
		}
	}
	sort.Strings(names[1:])

	// Catch up and attach replica to all subscribed topics.
	for _, name := range names {
		// Find topic.
		t := r.broker.topicsByName[name]
		assert(t != nil, "topic missing: %s", name)

		// Write topic messages from last known index.
		// Replica machine can ignore messages it already seen.
		index := r.topics[name]
		if _, err := t.writeTo(r, index); err != nil {
			return 0, fmt.Errorf("add stream writer: %s", err)
		}

		// Attach replica to topic to tail new messages.
		t.replicas[r.name] = r
	}

	// Wait for writer to close and then return.
	<-done
	return 0, nil
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

// CreateReplica creates a new named replica.
type CreateReplicaCommand struct {
	Name string `json:"name"`
}

// DeleteReplicaCommand removes a replica by name.
type DeleteReplicaCommand struct {
	Name string `json:"name"`
}

// SubscribeCommand subscribes a replica to a new topic.
type SubscribeCommand struct {
	Replica string `json:"replica"` // replica name
	Topic   string `json:"topic"`   // topic name
}

// UnsubscribeCommand removes a subscription for a topic from a replica.
type UnsubscribeCommand struct {
	Replica string `json:"replica"` // replica name
	Topic   string `json:"topic"`   // topic name
}

// MessageType represents the type of message.
type MessageType uint16

const (
	ConfigMessageType = 1 << 15
)

const (
	CreateTopicMessageType = ConfigMessageType | MessageType(0x00)
	DeleteTopicMessageType = ConfigMessageType | MessageType(0x01)

	CreateReplicaMessageType = ConfigMessageType | MessageType(0x10)
	DeleteReplicaMessageType = ConfigMessageType | MessageType(0x11)

	SubscribeMessageType   = ConfigMessageType | MessageType(0x20)
	UnsubscribeMessageType = ConfigMessageType | MessageType(0x21)
)

// The size of the encoded message header, in bytes.
const messageHeaderSize = 2 + 4 + 8 + 4

// Message represents a single item in a topic.
type Message struct {
	Type    MessageType
	TopicID uint32
	Index   uint64
	Data    []byte
}

// WriteTo encodes and writes the message to a writer. Implements io.WriterTo.
func (m *Message) WriteTo(w io.Writer) (n int, err error) {
	if n, err := w.Write(m.marshalHeader()); err != nil {
		return n, err
	}
	if n, err := w.Write(m.Data); err != nil {
		return messageHeaderSize + n, err
	}
	return messageHeaderSize + len(m.Data), nil
}

// MarshalBinary returns a binary representation of the message.
// This implements encoding.BinaryMarshaler. An error cannot be returned.
func (m *Message) MarshalBinary() ([]byte, error) {
	b := make([]byte, messageHeaderSize+len(m.Data))
	copy(b, m.marshalHeader())
	copy(b[messageHeaderSize:], m.Data)
	return b, nil
}

// UnmarshalBinary reads a message from a binary encoded slice.
// This implements encoding.BinaryUnmarshaler.
func (m *Message) UnmarshalBinary(b []byte) error {
	m.unmarshalHeader(b)
	if len(b[messageHeaderSize:]) < len(m.Data) {
		return fmt.Errorf("message data too short: %d < %d", len(b[messageHeaderSize:]), len(m.Data))
	}
	copy(m.Data, b[messageHeaderSize:])
	return nil
}

// marshalHeader returns a byte slice with the message header.
func (m *Message) marshalHeader() []byte {
	b := make([]byte, messageHeaderSize)
	binary.BigEndian.PutUint16(b[0:2], uint16(m.Type))
	binary.BigEndian.PutUint32(b[2:6], m.TopicID)
	binary.BigEndian.PutUint64(b[6:14], m.Index)
	binary.BigEndian.PutUint32(b[14:18], uint32(len(m.Data)))
	return b
}

// unmarshalHeader reads message header data from binary encoded slice.
// The data field is appropriately sized but is not filled.
func (m *Message) unmarshalHeader(b []byte) {
	m.Type = MessageType(binary.BigEndian.Uint16(b[0:2]))
	m.TopicID = binary.BigEndian.Uint32(b[2:6])
	m.Index = binary.BigEndian.Uint64(b[6:14])
	m.Data = make([]byte, binary.BigEndian.Uint32(b[14:18]))
}

// MessageDecoder decodes messages from a reader.
type MessageDecoder struct {
	r io.Reader
}

// NewMessageDecoder returns a new instance of the MessageDecoder.
func NewMessageDecoder(r io.Reader) *MessageDecoder {
	return &MessageDecoder{r: r}
}

// Decode reads a message from the decoder's reader.
func (dec *MessageDecoder) Decode(m *Message) error {
	// Read header bytes.
	var b [messageHeaderSize]byte
	if _, err := io.ReadFull(dec.r, b[:]); err != nil {
		return err
	}
	m.unmarshalHeader(b[:])

	// Read data.
	if _, err := io.ReadFull(dec.r, m.Data); err != nil {
		return err
	}

	return nil
}

type flusher interface {
	Flush()
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
