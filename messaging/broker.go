package messaging

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/raft"
)

// Broker represents distributed messaging system segmented into topics.
// Each topic represents a linear series of events.
type Broker struct {
	mu    sync.RWMutex
	path  string    // data directory
	index uint64    // highest applied index
	log   *raft.Log // internal raft log

	meta   *bolt.DB          // metadata
	topics map[uint64]*Topic // topics by id

	Logger *log.Logger
}

// NewBroker returns a new instance of a Broker with default values.
func NewBroker() *Broker {
	b := &Broker{
		log:    raft.NewLog(),
		topics: make(map[uint64]*Topic),
		Logger: log.New(os.Stderr, "[broker] ", log.LstdFlags),
	}
	b.log.FSM = (*brokerFSM)(b)
	return b
}

// Path returns the path used when opening the broker.
// Returns empty string if the broker is not open.
func (b *Broker) Path() string { return b.path }

// Log returns the underlying raft log.
func (b *Broker) Log() *raft.Log { return b.log }

// metaPath returns the file path to the broker's metadata file.
func (b *Broker) metaPath() string {
	if b.path == "" {
		return ""
	}
	return filepath.Join(b.path, "meta")
}

// TopicPath returns the file path to a topic's data.
// Returns a blank string if the broker is closed.
func (b *Broker) TopicPath(id uint64) string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topicPath(id)
}

func (b *Broker) topicPath(id uint64) string {
	if b.path == "" {
		return ""
	}
	return filepath.Join(b.path, strconv.FormatUint(id, 10))
}

// Index returns the highest index seen by the broker across all topics.
// Returns 0 if the broker is closed.
func (b *Broker) Index() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.index
}

// opened returns true if the broker is in an open and running state.
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

	// Ensure root directory exists.
	if err := os.MkdirAll(path, 0700); err != nil {
		b.close()
		return fmt.Errorf("mkdir: %s", err)
	}

	// Open meta file.
	meta, err := bolt.Open(b.metaPath(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		b.close()
		return fmt.Errorf("open meta: %s", err)
	}
	b.meta = meta

	// Initialize data from meta store.
	if err := b.meta.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("meta"))

		// Read in index from meta store, if set.
		if v := tx.Bucket([]byte("meta")).Get([]byte("index")); v != nil {
			b.index = btou64(v)
		}

		return nil
	}); err != nil {
		_ = b.close()
		return err
	}

	// Read all topic metadata into memory.
	if err := b.openTopics(); err != nil {
		b.close()
		return fmt.Errorf("open topics: %s", err)
	}

	// Open underlying raft log.
	if err := b.log.Open(filepath.Join(path, "raft")); err != nil {
		_ = b.close()
		return fmt.Errorf("raft: %s", err)
	}

	// Copy connection URL.
	b.log.URL = &url.URL{}
	*b.log.URL = *u

	return nil
}

// loadTopics reads all topic metadata into memory.
func (b *Broker) openTopics() error {
	// Open handle to directory.
	f, err := os.Open(b.path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Read directory items.
	fis, err := f.Readdir(0)
	if err != nil {
		return err
	}

	// Create a topic for each directory with a numeric name.
	for _, fi := range fis {
		// Ignore non-directory entries.
		if !fi.IsDir() {
			continue
		}

		// Filename must be numeric.
		topicID, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			continue
		}

		// Create and open topic.
		t := NewTopic(topicID)
		if err := t.Open(filepath.Join(b.path, fi.Name())); err != nil {
			return fmt.Errorf("open topic: id=%d, err=%s", topicID, err)
		}
		b.topics[t.id] = t
	}

	// Retrieve the highest index across all topics.
	for _, t := range b.topics {
		if t.index > b.index {
			b.index = t.index
		}
	}

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

	// Close all topics.
	b.closeTopics()

	// Close raft log.
	_ = b.log.Close()

	return nil
}

// closeTopics closes all topic files and clears the topics map.
func (b *Broker) closeTopics() {
	for _, t := range b.topics {
		_ = t.Close()
	}
	b.topics = make(map[uint64]*Topic)
}

// createSnapshotHeader creates a snapshot header.
func (b *Broker) createSnapshotHeader() (*snapshotHeader, error) {
	// Create parent header.
	sh := &snapshotHeader{}

	// Append topics.
	for _, t := range b.topics {
		// Create snapshot topic.
		st := &snapshotTopic{ID: t.id}

		// TODO: Read segments from disk, not topic.
		segments, err := ReadSegments(t.path)
		if err != nil {
			return nil, fmt.Errorf("read segments: %s", err)
		}

		// Add segments to topic.
		for _, s := range segments {
			// Retrieve current segment file size from disk.
			var size int64
			fi, err := os.Stat(s.Path)
			if os.IsNotExist(err) {
				size = 0
			} else if err == nil {
				size = fi.Size()
			} else {
				return nil, fmt.Errorf("stat segment: %s", err)
			}

			// Append segment.
			st.Segments = append(st.Segments, &snapshotTopicSegment{
				Index: s.Index,
				Size:  size,
				path:  s.Path,
			})
		}

		// Bump the snapshot header max index.
		if t.index > sh.Index {
			sh.Index = t.index
		}

		// Append topic to the snapshot.
		sh.Topics = append(sh.Topics, st)
	}

	return sh, nil
}

// U/RL returns the connection url for the broker.
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
	buf, err := m.MarshalBinary()
	assert(err == nil, "marshal binary error: %s", err)
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
func (b *Broker) Sync(index uint64) error { return b.log.Wait(index) }

// SetTopicMaxIndex updates the highest replicated index for a topic.
// If a higher index is already set on the topic then the call is ignored.
// This index is only held in memory and is used for topic segment reclamation.
func (b *Broker) SetTopicMaxIndex(topicID, index uint64) error {
	_, err := b.Publish(&Message{
		Type: SetTopicMaxIndexMessageType,
		Data: marshalTopicIndex(topicID, index),
	})
	return err
}

func (b *Broker) mustApplySetTopicMaxIndex(m *Message) {
	topicID, index := unmarshalTopicIndex(m.Data)

	b.mu.Lock()
	defer b.mu.Unlock()

	// Ignore if the topic doesn't exist or the index is already higher.
	t := b.topics[topicID]
	if t == nil || t.index >= index {
		return
	}
	t.index = index
}

func marshalTopicIndex(topicID, index uint64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], topicID)
	binary.BigEndian.PutUint64(b[8:16], index)
	return b
}

func unmarshalTopicIndex(b []byte) (topicID, index uint64) {
	topicID = binary.BigEndian.Uint64(b[0:8])
	index = binary.BigEndian.Uint64(b[8:16])
	return
}

// Truncate removes log segments that have been replicated to all subscribed replicas.
func (b *Broker) Truncate() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// TODO: Generate a list of all segments.
	// TODO: Sort by index.
	// TODO: Delete segments until we reclaim enough space.
	// TODO: Add tombstone for the last index.

	/*
		// Loop over every topic.
		for _, t := range b.topics {
			// Determine the highest index replicated to all subscribed replicas.
			minReplicaTopicIndex := b.minReplicaTopicIndex(t.id)

			// Loop over segments and close as needed.
			newSegments := make(Segments, 0, len(t.segments))
			for i, s := range t.segments {
				// Find the next segment so we can find the upper index bound.
				var next *Segment
				if i < len(t.segments)-1 {
					next = t.segments[i+1]
				}

				// Ignore the last segment or if the next index is less than
				// the highest index replicated across all replicas.
				if next == nil || minReplicaTopicIndex < next.index {
					newSegments = append(newSegments, s)
					continue
				}

				// Remove the segment if the replicated index has moved pasted
				// all the entries inside this segment.
				s.close()
				if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("remove segment: topic=%d, segment=%d, err=%s", t.id, s.index, err)
				}
			}
		}
	*/

	return nil
}

// brokerFSM implements the raft.FSM interface for the broker.
// This is implemented as a separate type because it is not meant to be exported.
type brokerFSM Broker

// MustApply executes a raft log entry against the broker.
// Non-repeatable errors such as system or disk errors must panic.
func (fsm *brokerFSM) MustApply(e *raft.LogEntry) {
	b := (*Broker)(fsm)

	// Decode commands into messages.
	m := &Message{}
	if e.Type == raft.LogEntryCommand {
		err := m.UnmarshalBinary(e.Data)
		assert(err == nil, "message unmarshal: %s", err)
	} else {
		m.Type = InternalMessageType
	}
	m.Index = e.Index

	// Process internal commands separately than the topic writes.
	switch m.Type {
	case InternalMessageType:
		b.mustApplyInternal(m)
	case SetTopicMaxIndexMessageType:
		b.mustApplySetTopicMaxIndex(m)
	default:
		// Create topic if not exists.
		t := b.topics[m.TopicID]
		if t == nil {
			t = NewTopic(m.TopicID)
			if err := t.Open(b.topicPath(t.id)); err != nil {
				panic("open topic: " + err.Error())
			}
			b.topics[t.id] = t
		}

		// Write message to topic.
		if err := t.WriteMessage(m); err != nil {
			panic("write message: " + err.Error())
		}
	}

	// Save highest applied index in memory.
	// Only internal messages need to have their indexes saved to disk.
	b.index = e.Index
}

// mustApplyInternal updates the highest index applied to the broker.
func (b *Broker) mustApplyInternal(m *Message) {
	err := b.meta.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).Put([]byte("index"), u64tob(m.Index))
	})
	assert(err == nil, "apply internal message: idx=%d, err=%s", m.Index, err)
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
		for _, s := range t.Segments {
			if _, err := copyFileN(w, s.path, s.Size); err != nil {
				return 0, err
			}
		}
	}

	// Return the snapshot and its last applied index.
	return hdr.Index, nil
}

// Restore reads the broker state.
func (fsm *brokerFSM) Restore(r io.Reader) error {
	b := (*Broker)(fsm)

	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove and recreate broker path.
	if err := os.RemoveAll(b.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove all: %s", err)
	} else if err = os.MkdirAll(b.path, 0700); err != nil {
		return fmt.Errorf("mkdir: %s", err)
	}

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
	sh := &snapshotHeader{}
	if err := json.Unmarshal(buf, &sh); err != nil {
		return fmt.Errorf("decode header: %s", err)
	}

	// Close any topics which might be open and clear them out.
	b.closeTopics()

	// Copy topic files from snapshot to local disk.
	for _, st := range sh.Topics {
		t := NewTopic(st.ID)

		// Copy data from snapshot into segment files.
		// We don't instantiate the segments because that will be done
		// automatically when calling Open() on the topic.
		for _, ss := range st.Segments {
			if err := func() error {
				// Create a new file with the starting index.
				f, err := os.Open(t.segmentPath(ss.Index))
				if err != nil {
					return fmt.Errorf("open segment: %s", err)
				}
				defer func() { _ = f.Close() }()

				// Copy from stream into file.
				if _, err := io.CopyN(f, r, ss.Size); err != nil {
					return fmt.Errorf("copy segment: %s", err)
				}

				return nil
			}(); err != nil {
				return err
			}
		}

		// Open new empty topic file.
		if err := t.Open(b.topicPath(t.id)); err != nil {
			return fmt.Errorf("open topic: %s", err)
		}
		b.topics[t.id] = t
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
	Topics []*snapshotTopic `json:"topics"`
	Index  uint64           `json:"index"`
}

type snapshotTopic struct {
	ID       uint64                  `json:"id"`
	Segments []*snapshotTopicSegment `json:"segments"`
}

type snapshotTopicSegment struct {
	Index uint64 `json:"index"`
	Size  int64  `json:"size"`

	path string
}

// DefaultMaxSegmentSize is the largest a segment can get before starting a new segment.
const DefaultMaxSegmentSize = 10 * 1024 * 1024 // 10MB

// topic represents a single named queue of messages.
// Each topic is identified by a unique path.
//
// Topics write their entries to segmented log files which contain a
// contiguous range of entries.
type Topic struct {
	mu    sync.Mutex
	id    uint64   // unique identifier
	index uint64   // highest index replicated
	path  string   // on-disk path
	file  *os.File // last segment writer

	// The largest a segment can get before splitting into a new segment.
	MaxSegmentSize int64
}

// NewTopic returns a new instance of topic.
func NewTopic(id uint64) *Topic {
	return &Topic{
		id:             id,
		MaxSegmentSize: DefaultMaxSegmentSize,
	}
}

// SegmentPath returns the path to a segment starting with a given log index.
func (t *Topic) SegmentPath(index uint64) string {
	t.mu.Lock()
	defer t.mu.Lock()
	return t.segmentPath(index)
}

func (t *Topic) segmentPath(index uint64) string {
	if t.path == "" {
		return ""
	}
	return filepath.Join(t.path, strconv.FormatUint(index, 10))
}

// Open opens a topic for writing.
func (t *Topic) Open(path string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Ensure topic is not already open.
	if t.path != "" {
		return ErrTopicOpen
	}
	t.path = path

	// Ensure the parent directory exists.
	if err := os.MkdirAll(t.path, 0700); err != nil {
		t.close()
		return err
	}

	// Read available segments.
	segments, err := ReadSegments(t.path)
	if err != nil {
		t.close()
		return fmt.Errorf("read segments: %s", err)
	}

	// Read max index and open file handle if we have segments.
	if len(segments) > 0 {
		s := segments.Last()

		// Read the last segment and extract the last message index.
		index, err := ReadSegmentMaxIndex(s.Path)
		if err != nil {
			t.close()
			return fmt.Errorf("read segment max index: %s", err)
		}
		t.index = index

		// Open file handle on the segment.
		f, err := os.OpenFile(s.Path, os.O_RDWR|os.O_APPEND, 0600)
		if err != nil {
			t.close()
			return fmt.Errorf("open segment: %s", err)
		}
		t.file = f
	}

	return nil
}

// Close closes the topic and segment writer.
func (t *Topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.close()
}

func (t *Topic) close() error {
	if t.file != nil {
		_ = t.file.Close()
		t.file = nil
	}

	t.path = ""
	t.index = 0

	return nil
}

// ReadIndex reads the highest available index for a topic from disk.
func (t *Topic) ReadIndex() (uint64, error) {
	// Read a list of all segments.
	segments, err := ReadSegments(t.path)
	if err != nil {
		return 0, fmt.Errorf("read segments: %s", err)
	}

	// Ignore if there are no available segments.
	if len(segments) == 0 {
		return 0, nil
	}

	// Read highest index on the last segment.
	index, err := ReadSegmentMaxIndex(segments.Last().Path)
	if err != nil {
		return 0, fmt.Errorf("read segment max index: %s", err)
	}

	return index, nil
}

// WriteMessage writes a message to the end of the topic.
func (t *Topic) WriteMessage(m *Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Return error if message index is lower than the topic's highest index.
	if m.Index <= t.index {
		return ErrStaleWrite
	}

	// Close the current file handle if it's too large.
	if t.file != nil {
		if fi, err := t.file.Stat(); err != nil {
			return fmt.Errorf("stat: %s", err)
		} else if fi.Size() > t.MaxSegmentSize {
			_ = t.file.Close()
			t.file = nil
		}
	}

	// Create a new segment if we have no handle.
	if t.file == nil {
		f, err := os.OpenFile(t.segmentPath(m.Index), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return fmt.Errorf("create segment file: %s", err)
		}
		t.file = f
	}

	// Encode message.
	b := make([]byte, messageHeaderSize+len(m.Data))
	copy(b, m.marshalHeader())
	copy(b[messageHeaderSize:], m.Data)

	// Write to last segment.
	if _, err := t.file.Write(b); err != nil {
		return fmt.Errorf("write segment: %s", err)
	}

	return nil
}

// Segment represents a contiguous section of a topic log.
type Segment struct {
	Index uint64 // starting index of the segment and name
	Path  string // path to the segment file.
}

// Size returns the file size of the segment.
func (s *Segment) Size() (int64, error) {
	fi, err := os.Stat(s.Path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Segments represents a list of segments sorted by index.
type Segments []*Segment

// Last returns the last segment in the slice.
// Returns nil if there are no segments.
func (a Segments) Last() *Segment {
	if len(a) == 0 {
		return nil
	}
	return a[len(a)-1]
}

func (a Segments) Len() int           { return len(a) }
func (a Segments) Less(i, j int) bool { return a[i].Index < a[j].Index }
func (a Segments) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// ReadSegments reads all segments from a directory path.
func ReadSegments(path string) (Segments, error) {
	// Open handle to directory.
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	// Read directory items.
	fis, err := f.Readdir(0)
	if err != nil {
		return nil, err
	}

	// Create a segment for each file with a numeric name.
	var a Segments
	for _, fi := range fis {
		index, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			continue
		}

		a = append(a, &Segment{
			Index: index,
			Path:  filepath.Join(path, fi.Name()),
		})
	}
	sort.Sort(a)

	return a, nil
}

// ReadSegmentByIndex returns the segment that contains a given index.
func ReadSegmentByIndex(path string, index uint64) (*Segment, error) {
	// Find a list of all segments.
	segments, err := ReadSegments(path)
	if err != nil {
		return nil, fmt.Errorf("read segments: %s", err)
	}

	// If there are no segments then ignore.
	// If index is zero then start from the first segment.
	// If index is less than the first segment range then return error.
	if len(segments) == 0 {
		return nil, nil
	} else if index == 0 {
		return segments[0], nil
	} else if index < segments[0].Index {
		return nil, ErrSegmentReclaimed
	}

	// Find segment that contains index.
	for i := range segments[:len(segments)-1] {
		if index >= segments[i].Index && index < segments[i+1].Index {
			return segments[i], nil
		}
	}

	// If no segment ranged matched then return the last segment.
	return segments[len(segments)-1], nil
}

// ReadSegmentMaxIndex returns the highest index recorded in a segment.
func ReadSegmentMaxIndex(path string) (uint64, error) {
	// Open segment file.
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open: %s", err)
	}
	defer func() { _ = f.Close() }()

	// Read all messages until the end.
	dec := NewMessageDecoder(f)
	index := uint64(0)
	for {
		var m Message
		if err := dec.Decode(&m); err == io.EOF {
			return index, nil
		} else if err != nil {
			return 0, fmt.Errorf("decode: %s", err)
		}
		index = m.Index
	}
}

// TopicReader reads data on a single topic from a given index.
type TopicReader struct {
	mu        sync.Mutex
	path      string // topic directory path
	index     uint64 // starting index
	streaming bool   // true if reader should hang and wait for new messages

	file   *os.File // current segment file handler
	closed bool
}

// NewTopicReader returns a new instance of TopicReader that reads segments
// from a path starting from a given index.
func NewTopicReader(path string, index uint64, streaming bool) *TopicReader {
	return &TopicReader{
		path:      path,
		index:     index,
		streaming: streaming,
	}
}

// Read reads the next bytes from the reader into the buffer.
func (r *TopicReader) Read(p []byte) (int, error) {
	for {
		// Retrieve current segment file handle.
		f, err := r.File()
		if err != nil {
			return 0, fmt.Errorf("file: %s", err)
		} else if f == nil {
			return 0, io.EOF
		}

		// Write data to buffer.
		// If no more data is available, then retry with the next segment.
		if n, err := r.Read(p); err == io.EOF {
			f, err = r.NextFile()
			if err != nil {
				return 0, fmt.Errorf("next: %s", err)
			}
			continue
		} else {
			return n, err
		}
	}
}

// File returns the current segment file handle.
// Returns nil when there is no more data left.
func (r *TopicReader) File() (*os.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Exit if closed.
	if r.closed {
		return nil, errors.New("topic reader closed")
	}

	// If the first file hasn't been opened then open it and seek.
	if r.file == nil {
		// Find the segment containing the index.
		// Exit if no segments are available.
		segment, err := ReadSegmentByIndex(r.path, r.index)
		if err != nil {
			return nil, fmt.Errorf("segment by index: %s", err)
		} else if segment == nil {
			return nil, nil
		}

		// Open that segment file.
		f, err := os.Open(filepath.Join(r.path, segment.Path))
		if err != nil {
			return nil, fmt.Errorf("open: %s", err)
		}

		// Seek to index.
		if err := r.seekAfterIndex(f, r.index); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("seek to index: %s", err)
		}

		// Save file handle and segment name.
		r.file = f
	}

	return r.file, nil
}

// seekAfterIndex moves a segment file to the message after a given index.
func (r *TopicReader) seekAfterIndex(f *os.File, seek uint64) error {
	dec := NewMessageDecoder(f)
	for {
		var m Message
		if err := dec.Decode(&m); err == io.EOF || m.Index >= seek {
			return nil
		} else if err != nil {
			return err
		}
	}
}

// NextFile closes the current segment's file handle and opens the next segment.
func (r *TopicReader) NextFile() (*os.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	panic("not yet implemented")
}

// Close closes the reader.
func (r *TopicReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Close current handle.
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}

	// Mark reader as closed.
	r.closed = true

	return nil
}

// MessageType represents the type of message.
type MessageType uint16

// BrokerMessageType is a flag set on broker messages to prevent them
// from being passed through to topics.
const BrokerMessageType = 0x8000

const (
	InternalMessageType         = BrokerMessageType | MessageType(0x00)
	SetTopicMaxIndexMessageType = BrokerMessageType | MessageType(0x01)
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

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 converts an 8-byte slice into an uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
