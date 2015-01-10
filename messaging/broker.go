package messaging

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	mu    sync.RWMutex
	path  string    // data directory
	index uint64    // highest applied index
	log   *raft.Log // internal raft log

	replicas map[uint64]*Replica // replica by id
	topics   map[uint64]*topic   // topics by id

	Logger *log.Logger
}

// NewBroker returns a new instance of a Broker with default values.
func NewBroker() *Broker {
	b := &Broker{
		log:      raft.NewLog(),
		replicas: make(map[uint64]*Replica),
		topics:   make(map[uint64]*topic),
		Logger:   log.New(os.Stderr, "[broker] ", log.LstdFlags),
	}
	b.log.FSM = (*brokerFSM)(b)
	return b
}

// Path returns the path used when opening the broker.
// Returns empty string if the broker is not open.
func (b *Broker) Path() string { return b.path }

func (b *Broker) metaPath() string {
	if b.path == "" {
		return ""
	}
	return filepath.Join(b.path, "meta")
}

func (b *Broker) opened() bool { return b.path != "" }

// SetLogOutput sets writer for all Broker log output.
func (b *Broker) SetLogOutput(w io.Writer) {
	b.Logger = log.New(w, "[broker] ", log.LstdFlags)
	b.log.SetLogOutput(w)
}

// Open initializes the log.
// The broker then must be initialized or join a cluster before it can be used.
func (b *Broker) Open(path string, u *url.URL) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Require a non-blank path.
	if path == "" {
		return ErrPathRequired
	}
	b.path = path

	// Require a non-blank connection address.
	if u == nil {
		return ErrConnectionAddressRequired
	}

	// Read meta data from snapshot.
	if err := b.load(); err != nil {
		_ = b.close()
		return err
	}

	// Open underlying raft log.
	if err := b.log.Open(filepath.Join(path, "raft")); err != nil {
		return fmt.Errorf("raft: %s", err)
	}

	// Copy connection URL.
	b.log.URL = &url.URL{}
	*b.log.URL = *u

	return nil
}

// Close closes the broker and all topics.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.close()
}

func (b *Broker) close() error {
	// Return error if the broker is already closed.
	if !b.opened() {
		return ErrClosed
	}
	b.path = ""

	// Close all topics & replicas.
	b.closeTopics()
	b.closeReplicas()

	// Close raft log.
	_ = b.log.Close()

	return nil
}

// closeTopics closes all topic files and clears the topics map.
func (b *Broker) closeTopics() {
	for _, t := range b.topics {
		_ = t.Close()
	}
	b.topics = make(map[uint64]*topic)
}

// closeReplicas closes all replica writers and clears the replica map.
func (b *Broker) closeReplicas() {
	for _, r := range b.replicas {
		r.closeWriter()
	}
	b.replicas = make(map[uint64]*Replica)
}

// load reads the broker metadata from disk.
func (b *Broker) load() error {
	// Read snapshot header from disk.
	// Ignore if no snapshot exists.
	f, err := os.Open(b.metaPath())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Read snapshot header from disk.
	hdr := &snapshotHeader{}
	if err := json.NewDecoder(f).Decode(&hdr); err != nil {
		return err
	}

	// Copy topic files from snapshot to local disk.
	for _, st := range hdr.Topics {
		t := b.createTopic(st.ID)
		t.index = st.Index

		// Open new empty topic file.
		if err := t.open(); err != nil {
			return fmt.Errorf("open topic: %s", err)
		}
	}

	// Update the replicas.
	for _, sr := range hdr.Replicas {
		// Create replica.
		r := newReplica(b, sr.ID)
		b.replicas[r.id] = r

		// Append replica's topics.
		for _, srt := range sr.Topics {
			r.topics[srt.TopicID] = srt.Index
		}
	}

	// Set the broker's index to the last index seen across all topics.
	b.index = hdr.maxIndex()

	return nil
}

// save persists the broker metadata to disk.
func (b *Broker) save() error {
	if b.path == "" {
		return ErrClosed
	}

	// Calculate header under lock.
	hdr, err := b.createSnapshotHeader()
	if err != nil {
		return fmt.Errorf("create snapshot: %s", err)
	}

	// Write snapshot to disk.
	f, err := os.Create(b.metaPath())
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Write snapshot to disk.
	if err := json.NewEncoder(f).Encode(&hdr); err != nil {
		return err
	}

	return nil
}

// mustSave persists the broker metadata to disk. Panic on error.
func (b *Broker) mustSave() {
	if err := b.save(); err != nil && err != ErrClosed {
		panic(err.Error())
	}
}

// createSnapshotHeader creates a snapshot header.
func (b *Broker) createSnapshotHeader() (*snapshotHeader, error) {
	// Create parent header.
	s := &snapshotHeader{}

	// Append topics.
	for _, t := range b.topics {
		// Retrieve current topic file size.
		var sz int64
		if t.file != nil {
			fi, err := t.file.Stat()
			if err != nil {
				return nil, err
			}
			sz = fi.Size()
		}

		// Append topic to the snapshot.
		s.Topics = append(s.Topics, &snapshotTopic{
			ID:    t.id,
			Index: t.index,
			Size:  sz,
			path:  t.path,
		})
	}

	// Append replicas and the current index for each topic.
	for _, r := range b.replicas {
		sr := &snapshotReplica{ID: r.id}

		for topicID, index := range r.topics {
			sr.Topics = append(sr.Topics, &snapshotReplicaTopic{
				TopicID: topicID,
				Index:   index,
			})
		}

		s.Replicas = append(s.Replicas, sr)
	}

	return s, nil
}

// URL returns the connection url for the broker.
func (b *Broker) URL() *url.URL {
	return b.log.URL
}

// LeaderURL returns the connection url for the leader broker.
func (b *Broker) LeaderURL() *url.URL {
	_, u := b.log.Leader()
	return u
}

// IsLeader returns true if the broker is the current leader.
func (b *Broker) IsLeader() bool { return b.log.State() == raft.Leader }

// Initialize creates a new cluster.
func (b *Broker) Initialize() error {
	if err := b.log.Initialize(); err != nil {
		return fmt.Errorf("raft: %s", err)
	}
	return nil
}

// Join joins an existing cluster.
func (b *Broker) Join(u *url.URL) error {
	if err := b.log.Join(u); err != nil {
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

// Replica returns a replica by id.
func (b *Broker) Replica(id uint64) *Replica {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.replicas[id]
}

// initializes a new topic object.
func (b *Broker) createTopic(id uint64) *topic {
	t := &topic{
		id:       id,
		path:     filepath.Join(b.path, strconv.FormatUint(uint64(id), 10)),
		replicas: make(map[uint64]*Replica),
	}
	b.topics[t.id] = t
	return t
}

func (b *Broker) createTopicIfNotExists(id uint64) *topic {
	if t := b.topics[id]; t != nil {
		return t
	}

	t := b.createTopic(id)
	b.mustSave()
	return t
}

// CreateReplica creates a new named replica.
func (b *Broker) CreateReplica(id uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica doesn't already exist.
	s := b.replicas[id]
	if s != nil {
		return ErrReplicaExists
	}

	// Add command to create replica.
	return b.PublishSync(&Message{
		Type: CreateReplicaMessageType,
		Data: mustMarshalJSON(&CreateReplicaCommand{ID: id}),
	})
}

func (b *Broker) mustApplyCreateReplica(m *Message) {
	var c CreateReplicaCommand
	mustUnmarshalJSON(m.Data, &c)

	// Create replica.
	r := newReplica(b, c.ID)

	// Automatically subscribe to the config topic.
	t := b.createTopicIfNotExists(BroadcastTopicID)
	r.topics[BroadcastTopicID] = t.index

	// Add replica to the broker.
	b.replicas[c.ID] = r

	b.mustSave()
}

// DeleteReplica deletes an existing replica by id.
func (b *Broker) DeleteReplica(id uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica exists.
	if s := b.replicas[id]; s == nil {
		return ErrReplicaNotFound
	}

	// Issue command to remove replica.
	return b.PublishSync(&Message{
		Type: DeleteReplicaMessageType,
		Data: mustMarshalJSON(&DeleteReplicaCommand{ID: id}),
	})
}

func (b *Broker) mustApplyDeleteReplica(m *Message) {
	var c DeleteReplicaCommand
	mustUnmarshalJSON(m.Data, &c)

	// Find replica.
	r := b.replicas[c.ID]
	if r == nil {
		return
	}

	// Remove replica from all subscribed topics.
	for topicID := range r.topics {
		if t := b.topics[topicID]; t != nil {
			delete(t.replicas, r.id)
		}
	}
	r.topics = make(map[uint64]uint64)

	// Close replica's writer.
	r.closeWriter()

	// Remove replica from broker.
	delete(b.replicas, c.ID)

	b.mustSave()
}

// Subscribe adds a subscription to a topic from a replica.
func (b *Broker) Subscribe(replicaID, topicID uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica & topic exist.
	if b.replicas[replicaID] == nil {
		return ErrReplicaNotFound
	}

	// Issue command to subscribe to topic.
	return b.PublishSync(&Message{
		Type: SubscribeMessageType,
		Data: mustMarshalJSON(&SubscribeCommand{ReplicaID: replicaID, TopicID: topicID}),
	})
}

func (b *Broker) mustApplySubscribe(m *Message) {
	var c SubscribeCommand
	mustUnmarshalJSON(m.Data, &c)

	// Retrieve replica.
	r := b.replicas[c.ReplicaID]
	if r == nil {
		return
	}

	// Save current index on topic.
	t := b.createTopicIfNotExists(c.TopicID)
	index := t.index

	// Ensure topic is not already subscribed to.
	if _, ok := r.topics[c.TopicID]; ok {
		b.Logger.Printf("already subscribed to topic: replica=%d, topic=%d", r.id, c.TopicID)
		return
	}

	// Add subscription to replica.
	r.topics[c.TopicID] = index
	t.replicas[c.ReplicaID] = r

	// Catch up replica.
	_, _ = t.writeTo(r, index)

	b.mustSave()
}

// Unsubscribe removes a subscription for a topic from a replica.
func (b *Broker) Unsubscribe(replicaID, topicID uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure replica & topic exist.
	if b.replicas[replicaID] == nil {
		return ErrReplicaNotFound
	}

	// Issue command to unsubscribe from topic.
	return b.PublishSync(&Message{
		Type: UnsubscribeMessageType,
		Data: mustMarshalJSON(&UnsubscribeCommand{ReplicaID: replicaID, TopicID: topicID}),
	})
}

func (b *Broker) mustApplyUnsubscribe(m *Message) {
	var c UnsubscribeCommand
	mustUnmarshalJSON(m.Data, &c)

	// Remove topic from replica.
	if r := b.replicas[c.ReplicaID]; r != nil {
		delete(r.topics, c.TopicID)
	}

	// Remove replica from topic.
	if t := b.topics[c.TopicID]; t != nil {
		delete(t.replicas, c.ReplicaID)
	}

	b.mustSave()
}

// brokerFSM implements the raft.FSM interface for the broker.
// This is implemented as a separate type because it is not meant to be exported.
type brokerFSM Broker

// MustApply executes a raft log entry against the broker.
// Non-repeatable errors such as system or disk errors must panic.
func (fsm *brokerFSM) MustApply(e *raft.LogEntry) {
	b := (*Broker)(fsm)

	// Create a message with the same index as Raft.
	m := &Message{}

	// Decode commands into messages.
	// Convert internal raft entries to no-ops to move the index forward.
	if e.Type == raft.LogEntryCommand {
		// Decode the message from the raft log.
		err := m.UnmarshalBinary(e.Data)
		assert(err == nil, "message unmarshal: %s", err)

		// Update the broker configuration.
		switch m.Type {
		case CreateReplicaMessageType:
			b.mustApplyCreateReplica(m)
		case DeleteReplicaMessageType:
			b.mustApplyDeleteReplica(m)
		case SubscribeMessageType:
			b.mustApplySubscribe(m)
		case UnsubscribeMessageType:
			b.mustApplyUnsubscribe(m)
		}
	} else {
		// Internal raft commands should be broadcast out as no-ops.
		m.TopicID = BroadcastTopicID
		m.Type = InternalMessageType
	}

	// Set the raft index.
	m.Index = e.Index

	// Write to the topic.
	t := b.createTopicIfNotExists(m.TopicID)
	if err := t.encode(m); err != nil {
		panic("encode: " + err.Error())
	}

	// Save highest applied index.
	// TODO: Persist to disk for raft commands.
	b.index = e.Index

	// HACK: Persist metadata after each apply.
	//       This should be derived on startup from the topic logs.
	b.mustSave()
}

// Index returns the highest index that the broker has seen.
func (fsm *brokerFSM) Index() (uint64, error) {
	b := (*Broker)(fsm)
	return b.index, nil
}

// Snapshot streams the current state of the broker and returns the index.
func (fsm *brokerFSM) Snapshot(w io.Writer) (uint64, error) {
	b := (*Broker)(fsm)

	// TODO: Prevent truncation during snapshot.

	// Calculate header under lock.
	b.mu.RLock()
	hdr, err := b.createSnapshotHeader()
	b.mu.RUnlock()
	if err != nil {
		return 0, fmt.Errorf("create snapshot: %s", err)
	}

	// Encode snapshot header.
	buf, err := json.Marshal(&hdr)
	if err != nil {
		return 0, fmt.Errorf("encode snapshot header: %s", err)
	}

	// Write header frame.
	if err := binary.Write(w, binary.BigEndian, uint32(len(buf))); err != nil {
		return 0, fmt.Errorf("write header size: %s", err)
	}
	if _, err := w.Write(buf); err != nil {
		return 0, fmt.Errorf("write header: %s", err)
	}

	// Stream each topic sequentially.
	for _, t := range hdr.Topics {
		if _, err := copyFileN(w, t.path, t.Size); err != nil {
			return 0, err
		}
	}

	// Return the snapshot and its last applied index.
	return hdr.maxIndex(), nil
}

// Restore reads the broker state.
func (fsm *brokerFSM) Restore(r io.Reader) error {
	b := (*Broker)(fsm)

	b.mu.Lock()
	defer b.mu.Unlock()

	// Read header frame.
	var sz uint32
	if err := binary.Read(r, binary.BigEndian, &sz); err != nil {
		return fmt.Errorf("read header size: %s", err)
	}
	buf := make([]byte, sz)
	if _, err := io.ReadFull(r, buf); err != nil {
		return fmt.Errorf("read header: %s", err)
	}

	// Decode header.
	s := &snapshotHeader{}
	if err := json.Unmarshal(buf, &s); err != nil {
		return fmt.Errorf("decode header: %s", err)
	}

	// Close any topics and replicas which might be open and clear them out.
	b.closeTopics()
	b.closeReplicas()

	// Copy topic files from snapshot to local disk.
	for _, st := range s.Topics {
		t := b.createTopic(st.ID)
		t.index = st.Index

		// Remove existing file if it exists.
		if err := os.Remove(t.path); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Open new empty topic file.
		if err := t.open(); err != nil {
			return fmt.Errorf("open topic: %s", err)
		}

		// Copy data from snapshot into file.
		if _, err := io.CopyN(t.file, r, st.Size); err != nil {
			return fmt.Errorf("copy topic: %s", err)
		}
	}

	// Update the replicas.
	for _, sr := range s.Replicas {
		// Create replica.
		r := newReplica(b, sr.ID)
		b.replicas[r.id] = r

		// Append replica's topics.
		for _, srt := range sr.Topics {
			r.topics[srt.TopicID] = srt.Index
		}
	}

	return nil
}

// copyFileN copies n bytes from a path to a writer.
func copyFileN(w io.Writer, path string, n int64) (int64, error) {
	// Open file for reading.
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// Copy file up to n bytes.
	return io.CopyN(w, f, n)
}

// snapshotHeader represents the header of a snapshot.
type snapshotHeader struct {
	Replicas []*snapshotReplica `json:"replicas"`
	Topics   []*snapshotTopic   `json:"topics"`
}

// maxIndex returns the highest applied index across all topics.
func (s *snapshotHeader) maxIndex() uint64 {
	var idx uint64
	for _, t := range s.Topics {
		if t.Index > idx {
			idx = t.Index
		}
	}
	return idx
}

type snapshotReplica struct {
	ID     uint64                  `json:"id"`
	Topics []*snapshotReplicaTopic `json:"topics"`
}

type snapshotTopic struct {
	ID    uint64 `json:"id"`
	Index uint64 `json:"index"`
	Size  int64  `json:"size"`

	path string
}

type snapshotReplicaTopic struct {
	TopicID uint64 `json:"topicID"`
	Index   uint64 `json:"index"`
}

// topic represents a single named queue of messages.
// Each topic is identified by a unique path.
type topic struct {
	id    uint64 // unique identifier
	index uint64 // highest index written
	path  string // on-disk path

	file *os.File // on-disk representation

	replicas map[uint64]*Replica // replicas subscribed to topic
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
func (t *topic) writeTo(r *Replica, index uint64) (int64, error) {
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
	var total int64
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
	id     uint64
	url    *url.URL // TODO
	broker *Broker

	writer io.Writer     // currently attached writer
	done   chan struct{} // notify when current writer is removed

	topics map[uint64]uint64 // current index for each subscribed topic
}

// newReplica returns a new Replica instance associated with a broker.
func newReplica(b *Broker, id uint64) *Replica {
	return &Replica{
		broker: b,
		id:     id,
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
func (r *Replica) WriteTo(w io.Writer) (int64, error) {
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
		t.replicas[r.id] = r
	}

	// Wait for writer to close and then return.
	<-done
	return 0, nil
}

// CreateReplica creates a new replica.
type CreateReplicaCommand struct {
	ID uint64 `json:"id"`
}

// DeleteReplicaCommand removes a replica.
type DeleteReplicaCommand struct {
	ID uint64 `json:"id"`
}

// SubscribeCommand subscribes a replica to a new topic.
type SubscribeCommand struct {
	ReplicaID uint64 `json:"replicaID"` // replica id
	TopicID   uint64 `json:"topicID"`   // topic id
}

// UnsubscribeCommand removes a subscription for a topic from a replica.
type UnsubscribeCommand struct {
	ReplicaID uint64 `json:"replicaID"` // replica id
	TopicID   uint64 `json:"topicID"`   // topic id
}

// MessageType represents the type of message.
type MessageType uint16

const (
	BrokerMessageType = 0x8000
)

const (
	InternalMessageType = BrokerMessageType | MessageType(0x00)

	CreateReplicaMessageType = BrokerMessageType | MessageType(0x10)
	DeleteReplicaMessageType = BrokerMessageType | MessageType(0x11)

	SubscribeMessageType   = BrokerMessageType | MessageType(0x20)
	UnsubscribeMessageType = BrokerMessageType | MessageType(0x21)
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
func (m *Message) WriteTo(w io.Writer) (n int64, err error) {
	if n, err := w.Write(m.marshalHeader()); err != nil {
		return int64(n), err
	}
	if n, err := w.Write(m.Data); err != nil {
		return int64(messageHeaderSize + n), err
	}
	return int64(messageHeaderSize + len(m.Data)), nil
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
