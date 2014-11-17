package messaging

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
	"strconv"
	"sync"

	"github.com/influxdb/influxdb/raft"
)

// BroadcastTopicID is the topic used to communicate with all replicas.
const BroadcastTopicID = uint64(0)

// Broker represents distributed messaging system segmented into topics.
// Each topic represents a linear series of events.
type Broker struct {
	mu   sync.RWMutex
	path string    // data directory
	log  *raft.Log // internal raft log

	replicas map[string]*Replica // replica by name

	maxTopicID uint64            // autoincrementing sequence
	topics     map[uint64]*topic // topics by id
}

// NewBroker returns a new instance of a Broker with default values.
func NewBroker() *Broker {
	b := &Broker{
		log:      raft.NewLog(),
		replicas: make(map[string]*Replica),
		topics:   make(map[uint64]*topic),
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

// Publish writes a message.
// Returns the index of the message. Otherwise returns an error.
func (b *Broker) Publish(m *Message) (uint64, error) {
	buf, _ := m.MarshalBinary()
	return b.log.Apply(buf)
}

// PublishSync writes a message and waits until the change is applied.
func (b *Broker) PublishSync(m *Message) error {
	// Publish message.
	index, err := b.Publish(m)
	if err != nil {
		return err
	}

	// Wait for message to apply.
	if err := b.Sync(index); err != nil {
		return err
	}

	return nil
}

// Sync pauses until the given index has been applied.
func (b *Broker) Sync(index uint64) error {
	return b.log.Wait(index)
}

// Replica returns a replica by name.
func (b *Broker) Replica(name string) *Replica {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.replicas[name]
}

// initializes a new topic object.
func (b *Broker) createTopic(id uint64) *topic {
	t := &topic{
		id:       id,
		path:     filepath.Join(b.path, strconv.FormatUint(uint64(id), 10)),
		replicas: make(map[string]*Replica),
	}
	b.topics[t.id] = t
	return t
}

func (b *Broker) createTopicIfNotExists(id uint64) *topic {
	if t := b.topics[id]; t != nil {
		return t
	}
	return b.createTopic(id)
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
	return b.PublishSync(&Message{
		Type: CreateReplicaMessageType,
		Data: mustMarshalJSON(&CreateReplicaCommand{Name: name}),
	})
}

func (b *Broker) applyCreateReplica(m *Message) {
	var c CreateReplicaCommand
	mustUnmarshalJSON(m.Data, &c)

	// Create replica.
	r := newReplica(b, c.Name)

	// Automatically subscribe to the config topic.
	t := b.createTopicIfNotExists(BroadcastTopicID)
	r.topics[BroadcastTopicID] = t.index

	// Add replica to the broker.
	b.replicas[c.Name] = r
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
	return b.PublishSync(&Message{
		Type: DeleteReplicaMessageType,
		Data: mustMarshalJSON(&DeleteReplicaCommand{Name: name}),
	})
}

func (b *Broker) applyDeleteReplica(m *Message) {
	var c DeleteReplicaCommand
	mustUnmarshalJSON(m.Data, &c)

	// Find replica.
	r := b.replicas[c.Name]
	if r == nil {
		return
	}

	// Remove replica from all subscribed topics.
	for topicID := range r.topics {
		if t := b.topics[topicID]; t != nil {
			delete(t.replicas, r.name)
		}
	}
	r.topics = make(map[uint64]uint64)

	// Close replica's writer.
	r.closeWriter()

	// Remove replica from broker.
	delete(b.replicas, c.Name)
}

// Subscribe adds a subscription to a topic from a replica.
func (b *Broker) Subscribe(replica string, topicID uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica & topic exist.
	if b.replicas[replica] == nil {
		return ErrReplicaNotFound
	}

	// Issue command to subscribe to topic.
	return b.PublishSync(&Message{
		Type: SubscribeMessageType,
		Data: mustMarshalJSON(&SubscribeCommand{Replica: replica, TopicID: topicID}),
	})
}

// applySubscribe is called when the SubscribeCommand is applied.
func (b *Broker) applySubscribe(m *Message) {
	var c SubscribeCommand
	mustUnmarshalJSON(m.Data, &c)

	// Retrieve replica.
	r := b.replicas[c.Replica]
	if r == nil {
		return
	}

	// Save current index on topic.
	t := b.createTopicIfNotExists(c.TopicID)
	index := t.index

	// Ensure topic is not already subscribed to.
	if _, ok := r.topics[c.TopicID]; ok {
		warn("already subscribed to topic")
		return
	}

	// Add subscription to replica.
	r.topics[c.TopicID] = index
	t.replicas[c.Replica] = r

	// Catch up replica.
	_, _ = t.writeTo(r, index)
}

// Unsubscribe removes a subscription for a topic from a replica.
func (b *Broker) Unsubscribe(replica string, topicID uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica & topic exist.
	if b.replicas[replica] == nil {
		return ErrReplicaNotFound
	}

	// Issue command to unsubscribe from topic.
	return b.PublishSync(&Message{
		Type: UnsubscribeMessageType,
		Data: mustMarshalJSON(&UnsubscribeCommand{Replica: replica, TopicID: topicID}),
	})
}

func (b *Broker) applyUnsubscribe(m *Message) {
	var c UnsubscribeCommand
	mustUnmarshalJSON(m.Data, &c)

	// Remove topic from replica.
	if r := b.replicas[c.Replica]; r != nil {
		delete(r.topics, c.TopicID)
	}

	// Remove replica from topic.
	if t := b.topics[c.TopicID]; t != nil {
		delete(t.replicas, c.Replica)
	}
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

	// Update the broker configuration.
	switch m.Type {
	case CreateReplicaMessageType:
		b.applyCreateReplica(m)
	case DeleteReplicaMessageType:
		b.applyDeleteReplica(m)
	case SubscribeMessageType:
		b.applySubscribe(m)
	case UnsubscribeMessageType:
		b.applyUnsubscribe(m)
	}

	// Write to the topic.
	t := b.createTopicIfNotExists(m.TopicID)
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
	id    uint64 // unique identifier
	index uint64 // highest index written
	path  string // on-disk path

	file *os.File // on-disk representation

	replicas map[string]*Replica // replicas subscribed to topic
}

// open opens a topic for writing.
func (t *topic) open() error {
	assert(t.file == nil, "topic already open: %d", t.id)

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
	// If it doesn't exist then just exit immediately.
	f, err := os.Open(t.path)
	if os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
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

	topics map[uint64]uint64 // current index for each subscribed topic
}

// newReplica returns a new named Replica instance associated with a broker.
func newReplica(b *Broker, name string) *Replica {
	return &Replica{
		broker: b,
		name:   name,
		topics: make(map[uint64]uint64),
	}
}

// closeWriter removes the writer on the replica and closes the notify channel.
func (r *Replica) closeWriter() {
	if r.writer != nil {
		r.writer = nil
		close(r.done)
		r.done = nil
	}
}

// Topics returns a list of topic names that the replica is subscribed to.
func (r *Replica) Topics() []uint64 {
	a := make([]uint64, 0, len(r.topics))
	for topicID := range r.topics {
		a = append(a, topicID)
	}
	sort.Sort(uint64Slice(a))
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
	ids := make([]uint64, 0, len(r.topics))
	for topicID := range r.topics {
		ids = append(ids, topicID)
	}
	sort.Sort(uint64Slice(ids))

	// Catch up and attach replica to all subscribed topics.
	for _, topicID := range ids {
		// Find topic.
		t := r.broker.topics[topicID]
		assert(t != nil, "topic missing: %s", topicID)

		// Write topic messages from last known index.
		// Replica machine can ignore messages it already seen.
		index := r.topics[topicID]
		if _, err := t.writeTo(r, index); err != nil {
			r.closeWriter()
			return 0, fmt.Errorf("add stream writer: %s", err)
		}

		// Attach replica to topic to tail new messages.
		t.replicas[r.name] = r
	}

	// Wait for writer to close and then return.
	<-done
	return 0, nil
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
	TopicID uint64 `json:"topicID"` // topic id
}

// UnsubscribeCommand removes a subscription for a topic from a replica.
type UnsubscribeCommand struct {
	Replica string `json:"replica"` // replica name
	TopicID uint64 `json:"topicID"` // topic id
}

// MessageType represents the type of message.
type MessageType uint16

const (
	BrokerMessageType = 0x8000
)

const (
	CreateReplicaMessageType = BrokerMessageType | MessageType(0x00)
	DeleteReplicaMessageType = BrokerMessageType | MessageType(0x01)

	SubscribeMessageType   = BrokerMessageType | MessageType(0x10)
	UnsubscribeMessageType = BrokerMessageType | MessageType(0x11)
)

// The size of the encoded message header, in bytes.
const messageHeaderSize = 2 + 8 + 8 + 4

// Message represents a single item in a topic.
type Message struct {
	Type    MessageType
	TopicID uint64
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
	binary.BigEndian.PutUint64(b[2:10], m.TopicID)
	binary.BigEndian.PutUint64(b[10:18], m.Index)
	binary.BigEndian.PutUint32(b[18:22], uint32(len(m.Data)))
	return b
}

// unmarshalHeader reads message header data from binary encoded slice.
// The data field is appropriately sized but is not filled.
func (m *Message) unmarshalHeader(b []byte) {
	m.Type = MessageType(binary.BigEndian.Uint16(b[0:2]))
	m.TopicID = binary.BigEndian.Uint64(b[2:10])
	m.Index = binary.BigEndian.Uint64(b[10:18])
	m.Data = make([]byte, binary.BigEndian.Uint32(b[18:22]))
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

// uint64Slice attaches the methods of Interface to []int, sorting in increasing order.
type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// mustMarshalJSON encodes a value to JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid marshal will cause corruption and a panic is appropriate.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal: " + err.Error())
	}
	return b
}

// mustUnmarshalJSON decodes a value from JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid unmarshal will cause corruption and a panic is appropriate.
func mustUnmarshalJSON(b []byte, v interface{}) {
	if err := json.Unmarshal(b, v); err != nil {
		panic("unmarshal: " + err.Error())
	}
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
