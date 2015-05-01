package messaging

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
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

// DefaultPollInterval is the default amount of time a topic reader will wait
// between checks for new segments or new data on an existing segment. This
// only occurs when the reader is at the end of all the data.
const DefaultPollInterval = 100 * time.Millisecond

const DefaultTruncationInterval = 10 * time.Minute

// DefaultMaxTopicSize is the largest a topic can get before truncation.
const DefaultMaxTopicSize = 1024 * 1024 * 1024 // 1GB

// DefaultMaxSegmentSize is the largest a segment can get before starting a new segment.
const DefaultMaxSegmentSize = 10 * 1024 * 1024 // 10MB

var (
	// ErrTopicTruncated is returned when topic data is unavailable due to truncation.
	ErrTopicTruncated = errors.New("topic truncated")

	// ErrTopicNodesNotFound is returned when requested topic data has been truncated
	// but there are no nodes in the cluster that can provide a replica. This is a system
	// failure and should not occur on a healthy replicated system.
	ErrTopicNodesNotFound = errors.New("topic nodes not found")
)

// Broker represents distributed messaging system segmented into topics.
// Each topic represents a linear series of events.
type Broker struct {
	mu    sync.RWMutex
	path  string // data directory
	index uint64 // highest applied index

	meta   *bolt.DB          // metadata
	topics map[uint64]*Topic // topics by id

	TruncationInterval time.Duration
	MaxTopicSize       int64 // Maximum size of a topic in bytes
	MaxSegmentSize     int64 // Maximum size of a segment in bytes

	// Goroutinte shutdown
	done chan struct{}
	wg   sync.WaitGroup

	// Log is the distributed raft log that commands are applied to.
	Log interface {
		URL() url.URL
		URLs() []url.URL
		Leader() (uint64, url.URL)
		IsLeader() bool
		ClusterID() uint64
		Apply(data []byte) (index uint64, err error)
	}

	Logger *log.Logger
}

// NewBroker returns a new instance of a Broker with default values.
func NewBroker() *Broker {
	b := &Broker{
		topics:             make(map[uint64]*Topic),
		Logger:             log.New(os.Stderr, "[broker] ", log.LstdFlags),
		TruncationInterval: DefaultTruncationInterval,
		MaxTopicSize:       DefaultMaxTopicSize,
		MaxSegmentSize:     DefaultMaxSegmentSize,
		done:               make(chan struct{}),
	}
	return b
}

// Path returns the path used when opening the broker.
// Returns empty string if the broker is not open.
func (b *Broker) Path() string { return b.path }

// metaPath returns the file path to the broker's metadata file.
func (b *Broker) metaPath() string {
	if b.path == "" {
		return ""
	}
	return filepath.Join(b.path, "meta")
}

// URL returns the URL of the broker.
func (b *Broker) URL() url.URL { return b.Log.URL() }

// URLs returns a list of all broker URLs in the cluster.
func (b *Broker) URLs() []url.URL { return b.Log.URLs() }

// IsLeader returns true if the broker is the current cluster leader.
func (b *Broker) IsLeader() bool { return b.Log.IsLeader() }

// LeaderURL returns the URL to the leader broker.
func (b *Broker) LeaderURL() url.URL {
	_, u := b.Log.Leader()
	return u
}

// ClusterID returns the identifier for the cluster.
func (b *Broker) ClusterID() uint64 { return b.Log.ClusterID() }

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

// Topic returns a topic on a broker by id.
// Returns nil if the topic doesn't exist or the broker is closed.
func (b *Broker) Topic(id uint64) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[id]
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

// Open initializes the log.
// The broker then must be initialized or join a cluster before it can be used.
func (b *Broker) Open(path string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Require a non-blank path.
	if path == "" {
		return ErrPathRequired
	}

	if err := func() error {
		b.path = path

		// Ensure root directory exists.
		if err := os.MkdirAll(path, 0777); err != nil {
			return fmt.Errorf("mkdir: %s", err)
		}

		// Open meta file.
		meta, err := bolt.Open(b.metaPath(), 0666, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
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
			return err
		}

		// Read all topic metadata into memory.
		if err := b.openTopics(); err != nil {
			return fmt.Errorf("open topics: %s", err)
		}

		// Start periodic deletion of replicated topic segments.
		b.startTopicTruncation()

		return nil
	}(); err != nil {
		_ = b.close()
		return err
	}

	return nil
}

// openTopics reads all topic metadata into memory.
func (b *Broker) openTopics() error {
	// Read all topics from the broker directory.
	topics, err := ReadTopics(b.path)
	if err != nil {
		return fmt.Errorf("read topics: %s", err)
	}

	// Open each topic and append to the map.
	b.topics = make(map[uint64]*Topic)
	for _, t := range topics {
		if err := t.Open(); err != nil {
			return fmt.Errorf("open topic: id=%d, err=%s", t.id, err)
		}
		b.topics[t.id] = t
		b.topics[t.id].MaxSegmentSize = b.MaxSegmentSize
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

	// Close meta data.
	if b.meta != nil {
		_ = b.meta.Close()
		b.meta = nil
	}

	// Close all topics.
	b.closeTopics()

	// Shutdown all goroutines.
	close(b.done)
	b.wg.Wait()
	b.done = nil

	return nil
}

// closeTopics closes all topic files and clears the topics map.
func (b *Broker) closeTopics() {
	for _, t := range b.topics {
		_ = t.Close()
	}
	b.topics = make(map[uint64]*Topic)
}

// startTopicTruncation starts periodic topic truncation.
func (b *Broker) startTopicTruncation() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		tick := time.NewTicker(b.TruncationInterval)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				b.TruncateTopics()
			case <-b.done:
				return
			}
		}
	}()
}

// TruncateTopics forces topics to truncate such that they are equal to
// or less than the requested size, if possible.
func (b *Broker) TruncateTopics() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.Logger.Printf("executing truncation check for %d topic(s)", len(b.topics))
	for _, t := range b.topics {
		if s, d, err := t.Truncate(b.MaxTopicSize); err != nil {
			b.Logger.Printf("error truncating topic %s: %s", t.Path(), err.Error())
		} else if s > 0 {
			b.Logger.Printf("topic %s, %d segments, %d bytes, deleted", t.Path(), s, d)
		}
	}
	return
}

// SetMaxIndex sets the highest index applied by the broker.
// This is only used for internal log messages. Topics may have a higher index.
func (b *Broker) SetMaxIndex(index uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.setMaxIndex(index)
}

func (b *Broker) setMaxIndex(index uint64) error {
	// Update index in meta database.
	if err := b.meta.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).Put([]byte("index"), u64tob(index))
	}); err != nil {
		return err
	}

	// Set in-memory index.
	b.index = index

	return nil
}

// URLsForTopic returns a slice of URLs from where previously replicaed
// data for a given topic may be retrieved. The nodes at the URL will have
// data up to at least the given index. These URLs are provided when a node
// requests topic data that has been truncated.
func (b *Broker) DataURLsForTopic(id, index uint64) []url.URL {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t := b.topics[id]
	if t == nil {
		return nil
	}
	return t.DataURLsForIndex(index)
}

// WriteTo writes a snapshot of the broker to w.
func (b *Broker) WriteTo(w io.Writer) (int64, error) {
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

	return 0, nil
}

// createSnapshotHeader creates a snapshot header.
func (b *Broker) createSnapshotHeader() (*snapshotHeader, error) {
	// Create parent header.
	sh := &snapshotHeader{Index: b.index}

	// Append topics.
	for _, t := range b.topics {
		// Create snapshot topic.
		st := &snapshotTopic{ID: t.id}

		// Read segments from disk.
		segments, err := ReadSegments(t.path)
		if err != nil && !os.IsNotExist(err) {
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

		// Append topic to the snapshot.
		sh.Topics = append(sh.Topics, st)
	}

	return sh, nil
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

// ReadFrom reads a broker snapshot from r.
func (b *Broker) ReadFrom(r io.Reader) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove and recreate broker path.
	if err := b.reset(); err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("reset: %s", err)
	} else if err = os.MkdirAll(b.path, 0777); err != nil {
		return 0, fmt.Errorf("mkdir: %s", err)
	}

	// Read header frame.
	var sz uint32
	if err := binary.Read(r, binary.BigEndian, &sz); err != nil {
		return 0, fmt.Errorf("read header size: %s", err)
	}
	buf := make([]byte, sz)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, fmt.Errorf("read header: %s", err)
	}

	// Decode header.
	sh := &snapshotHeader{}
	if err := json.Unmarshal(buf, &sh); err != nil {
		return 0, fmt.Errorf("decode header: %s", err)
	}

	// Close any topics which might be open and clear them out.
	b.closeTopics()

	// Copy topic files from snapshot to local disk.
	for _, st := range sh.Topics {
		t := NewTopic(st.ID, b.topicPath(st.ID))
		t.MaxSegmentSize = b.MaxSegmentSize

		// Create topic directory.
		if err := os.MkdirAll(t.Path(), 0777); err != nil {
			return 0, fmt.Errorf("make topic dir: %s", err)
		}

		// Copy data from snapshot into segment files.
		// We don't instantiate the segments because that will be done
		// automatically when calling Open() on the topic.
		for _, ss := range st.Segments {
			if err := func() error {
				// Create a new file with the starting index.
				f, err := os.Create(t.segmentPath(ss.Index))
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
				return 0, err
			}
		}

		// Open topic.
		if err := t.Open(); err != nil {
			return 0, fmt.Errorf("open topic: %s", err)
		}
		b.topics[t.id] = t
	}

	// Set the highest seen index.
	if err := b.setMaxIndex(sh.Index); err != nil {
		return 0, fmt.Errorf("set max index: %s", err)
	}
	b.index = sh.Index

	return 0, nil
}

// reset removes all files in the broker directory besides the raft directory.
func (b *Broker) reset() error {
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

	// Remove all files & directories besides raft.
	for _, fi := range fis {
		if fi.Name() == "raft" {
			continue
		}

		if err := os.RemoveAll(fi.Name()); err != nil {
			return fmt.Errorf("remove: %s", fi.Name())
		}
	}

	return nil
}

// Publish writes a message.
// Returns the index of the message. Otherwise returns an error.
func (b *Broker) Publish(m *Message) (uint64, error) {
	buf, err := m.MarshalBinary()
	assert(err == nil, "marshal binary error: %s", err)
	return b.Log.Apply(buf)
}

// TopicReader returns a new topic reader for a topic starting from a given index.
func (b *Broker) TopicReader(topicID, index uint64, streaming bool) interface {
	io.ReadCloser
	io.Seeker
} {
	return NewTopicReader(b.TopicPath(topicID), index, streaming)
}

// SetTopicMaxIndex updates the highest replicated index for a topic and data URL.
// If a higher index is already set on the topic then the call is ignored.
// This index is only held in memory and is used for topic segment reclamation.
// The higheset replicated index per data URL is tracked separately from the current index
func (b *Broker) SetTopicMaxIndex(topicID, index uint64, u url.URL) error {
	_, err := b.Publish(&Message{
		Type: SetTopicMaxIndexMessageType,
		Data: marshalTopicIndex(topicID, index, u),
	})
	return err
}

func (b *Broker) applySetTopicMaxIndex(m *Message) {
	topicID, index, u := unmarshalTopicIndex(m.Data)

	// Track the highest replicated index for the topic, per data node URL.
	if t := b.topics[topicID]; t != nil {
		t.SetIndexForURL(index, u)
	}
}

func marshalTopicIndex(topicID, index uint64, u url.URL) []byte {
	s := []byte(u.String())
	b := make([]byte, 16+2+len(s))
	binary.BigEndian.PutUint64(b[0:8], topicID)
	binary.BigEndian.PutUint64(b[8:16], index)
	binary.BigEndian.PutUint16(b[16:18], uint16(len(s))) // URL string length
	n := copy(b[18:], s)                                 // URL string
	assert(n == len(s), "marshal topic index too short. have %d, expectd %d", n, len(s))
	return b
}

func unmarshalTopicIndex(b []byte) (topicID, index uint64, u url.URL) {
	assert(len(b) >= 18, "unmarshal topic index too short. have %d, expected %d", len(b), 20)
	topicID = binary.BigEndian.Uint64(b[0:8])
	index = binary.BigEndian.Uint64(b[8:16])
	n := binary.BigEndian.Uint16(b[16:18])     // URL length
	du, err := url.Parse(string(b[18 : 18+n])) // URL
	assert(err == nil, "unmarshal binary error: %s", err)
	u = *du
	return
}

// Apply executes a message against the broker.
func (b *Broker) Apply(m *Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Exit if broker isn't open.
	if !b.opened() {
		return ErrClosed
	}

	// Ensure messages with old indexes aren't re-applied.
	assert(m.Index > b.index, "stale apply: msg=%d, broker=%d", m.Index, b.index)

	// Process internal commands separately than the topic writes.
	switch m.Type {
	case SetTopicMaxIndexMessageType:
		b.applySetTopicMaxIndex(m)
	default:
		// Create topic if not exists.
		t := b.topics[m.TopicID]
		if t == nil {
			t = NewTopic(m.TopicID, b.topicPath(m.TopicID))
			t.MaxSegmentSize = b.MaxSegmentSize
			if err := t.Open(); err != nil {
				return fmt.Errorf("open topic: %s", err)
			}
			b.topics[t.id] = t
		}

		// Write message to topic.
		if err := t.WriteMessage(m); err != nil {
			return fmt.Errorf("write message: %s", err)
		}
	}

	// Save highest applied index in memory.
	// Only internal messages need to have their indexes saved to disk.
	b.index = m.Index

	return nil
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

// RaftFSM is a wrapper struct around the broker that implements the raft.FSM interface.
// It will panic for any errors that occur during Apply.
type RaftFSM struct {
	Broker interface {
		io.WriterTo
		io.ReaderFrom

		Apply(m *Message) error
		Index() uint64
		SetMaxIndex(uint64) error
	}
}

func (fsm *RaftFSM) Index() uint64                             { return fsm.Broker.Index() }
func (fsm *RaftFSM) WriteTo(w io.Writer) (n int64, err error)  { return fsm.Broker.WriteTo(w) }
func (fsm *RaftFSM) ReadFrom(r io.Reader) (n int64, err error) { return fsm.Broker.ReadFrom(r) }

// Apply applies a raft command to the broker.
func (fsm *RaftFSM) Apply(e *raft.LogEntry) error {
	switch e.Type {
	case raft.LogEntryCommand:
		// Decode message.
		m := &Message{}
		if err := m.UnmarshalBinary(e.Data); err != nil {
			panic("message unmarshal: " + err.Error())
		}
		m.Index = e.Index

		// Apply message.
		if err := fsm.Broker.Apply(m); err != nil {
			return fmt.Errorf("broker apply: %s", err)
		}

	default:
		// Move internal index forward if it's an internal raft comand.
		if err := fsm.Broker.SetMaxIndex(e.Index); err != nil {
			return fmt.Errorf("set max index: idx=%d, err=%s", e.Index, err)
		}
	}

	return nil
}

// topic represents a single named queue of messages.
// Each topic is identified by a unique path.
//
// Topics write their entries to segmented log files which contain a
// contiguous range of entries.
type Topic struct {
	mu    sync.RWMutex
	id    uint64 // unique identifier
	index uint64 // current index
	path  string // on-disk path

	// highest index replicated per data url.  The unique set of keys across all topics
	// provides a snapshot of the addresses of every data node in a cluster.
	indexByURL map[url.URL]uint64

	file   *os.File // last segment writer
	opened bool

	// The largest a segment can get before splitting into a new segment.
	MaxSegmentSize int64
}

// NewTopic returns a new instance of Topic.
func NewTopic(id uint64, path string) *Topic {
	return &Topic{
		id:             id,
		path:           path,
		indexByURL:     make(map[url.URL]uint64),
		MaxSegmentSize: DefaultMaxSegmentSize,
	}
}

// ID returns the topic identifier.
func (t *Topic) ID() uint64 { return t.id }

// Path returns the topic path.
func (t *Topic) Path() string { return t.path }

// TombstonePath returns the path of the tomstone file.
func (t *Topic) TombstonePath() string { return filepath.Join(t.path, "tombstone") }

// Truncated returns whether the topic has even been truncated.
func (t *Topic) Truncated() bool {
	return tombstoneExists(t.path)
}

// Index returns the highest replicated index for the topic.
func (t *Topic) Index() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.index
}

// DataURLs returns the data node URLs subscribed to this topic
func (t *Topic) DataURLs() []url.URL {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var urls []url.URL
	for u, _ := range t.indexByURL {
		urls = append(urls, u)
	}
	return urls
}

// DataURLsForIndex returns the data node URLs subscribed to this topic that have
// replicated at least up to the given index.
func (t *Topic) DataURLsForIndex(index uint64) []url.URL {
	t.mu.Lock()
	defer t.mu.Unlock()
	var urls []url.URL
	for u, idx := range t.indexByURL {
		if idx >= index {
			urls = append(urls, u)
		}
	}
	return urls
}

// IndexForURL returns the highest index replicated for a given data URL.
func (t *Topic) IndexForURL(u url.URL) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.indexByURL[u]
}

// SetIndexForURL sets the replicated index for a given data URL.
func (t *Topic) SetIndexForURL(index uint64, u url.URL) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.indexByURL[u] = index
}

// SegmentPath returns the path to a segment starting with a given log index.
func (t *Topic) SegmentPath(index uint64) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.segmentPath(index)
}

func (t *Topic) segmentPath(index uint64) string {
	if t.path == "" {
		return ""
	}
	return filepath.Join(t.path, strconv.FormatUint(index, 10))
}

// Open opens a topic for writing.
func (t *Topic) Open() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Ensure topic is not already open and it has a path.
	if t.opened {
		return ErrTopicOpen
	} else if t.path == "" {
		return ErrPathRequired
	}

	if err := func() error {
		t.opened = true

		// Ensure the parent directory exists.
		if err := os.MkdirAll(t.path, 0777); err != nil {
			return err
		}

		// Read available segments.
		segments, err := ReadSegments(t.path)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("read segments: %s", err)
		}

		// Read max index and open file handle if we have segments.
		if len(segments) > 0 {
			s := segments.Last()

			// Read the last segment and extract the last message index.
			index, err := RecoverSegment(s.Path)
			if err != nil {
				return fmt.Errorf("recover segment: %s", err)
			}
			t.index = index

			// Open file handle on the segment.
			f, err := os.OpenFile(s.Path, os.O_RDWR|os.O_APPEND, 0666)
			if err != nil {
				return fmt.Errorf("open segment: %s", err)
			}
			t.file = f
		}

		return nil
	}(); err != nil {
		_ = t.close()
		return err
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

	t.opened = false
	t.index = 0

	return nil
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
		f, err := os.OpenFile(t.segmentPath(m.Index), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return fmt.Errorf("create segment file: %s", err)
		}
		t.file = f
	}

	// Write to last segment.
	if _, err := m.WriteTo(t.file); err != nil {
		return fmt.Errorf("write segment: %s", err)
	}

	// Update index.
	t.index = m.Index

	return nil
}

// Truncate attempts to delete topic segments such that the total size of the topic on-disk
// is equal to or less-than maxSize. Returns the number of segments and bytes deleted, and
// error if any. This function is not guaranteed to be performant.
func (t *Topic) Truncate(maxSize int64) (int, int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	nSegmentsDeleted := 0

	segments, err := ReadSegments(t.Path())
	if err != nil {
		return 0, 0, err
	}

	totalSize, err := segments.Size()
	if err != nil {
		return 0, 0, err
	}

	// Find the highest-replicated index, this is a condition for deletion of segments
	// within this topic.
	var highestIndex uint64
	for _, idx := range t.indexByURL {
		if idx > highestIndex {
			highestIndex = idx
		}
	}

	var nBytesDeleted int64
	for {
		if (totalSize - nBytesDeleted) <= maxSize {
			break
		}

		// More than 2 segments are needed for current writes and replication checks.
		if len(segments) <= 2 {
			break
		}

		// Attempt deletion of oldest segment in topic. First and second segment needed
		// for this operation.
		first := segments[0]
		second := segments[1]

		// The first segment can only be deleted if the last index in that segment has
		// been replicated. The most efficient way to check this is to ensure that the
		// first index of the subsequent segment has been replicated.
		if second.Index > highestIndex {
			// No guarantee first segment has been replicated.
			break
		}

		// Deletion can proceed!
		size, err := first.Size()
		if err != nil {
			return nSegmentsDeleted, nBytesDeleted, err
		}
		if err = os.Remove(first.Path); err != nil {
			return nSegmentsDeleted, nBytesDeleted, err
		}
		nSegmentsDeleted += 1
		nBytesDeleted += size
		segments = segments[1:]

		// Create tombstone to indicate that the topic has been truncated at least once.
		f, err := os.Create(t.TombstonePath())
		if err != nil {
			return nSegmentsDeleted, nBytesDeleted, err
		}
		f.Close()
	}

	return nSegmentsDeleted, nBytesDeleted, err
}

// Topics represents a list of topics sorted by id.
type Topics []*Topic

func (a Topics) Len() int           { return len(a) }
func (a Topics) Less(i, j int) bool { return a[i].id < a[j].id }
func (a Topics) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// ReadTopics reads all topics from a directory path.
func ReadTopics(path string) (Topics, error) {
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

	// Create a topic for each directory with a numeric name.
	var a Topics
	for _, fi := range fis {
		// Skip non-directory paths.
		if !fi.IsDir() {
			continue
		}

		topicID, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			continue
		}

		a = append(a, NewTopic(topicID, filepath.Join(path, fi.Name())))
	}
	sort.Sort(a)

	return a, nil
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

func (a Segments) Size() (int64, error) {
	var size int64
	for _, s := range a {
		sz, err := s.Size()
		if err != nil {
			return 0, nil
		}
		size += sz
	}
	return size, nil
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
	if os.IsNotExist(err) {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("read segments: %s", err)
	}

	// Determine if this topic has been truncated.
	truncated := tombstoneExists(path)

	// If the requested index is less than that available, one of two things will
	// happen. If the topic has been truncated it means that topic data may exist
	// somewhere on the cluster that is not available here. Therefore this broker
	// cannot provide the data. If truncation has never taken place however, then
	// this broker can safely provide whatever data it has.

	if len(segments) == 0 {
		if truncated {
			return nil, ErrTopicTruncated
		}
		return nil, nil
	}

	// Is requested index lower than the broker can provide?
	if index < segments[0].Index {
		if truncated {
			return nil, ErrTopicTruncated
		}
		return segments[0], nil
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

// RecoverSegment parses the entire segment and truncates at any partial messages.
// Returns the last index seen in the segment.
func RecoverSegment(path string) (uint64, error) {
	// Open segment file.
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0, err
	} else if err != nil {
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
		} else if err == io.ErrUnexpectedEOF || err == ErrChecksum {
			// The decoder will unread any partially read data so we can
			// simply truncate at current position.
			if n, err := f.Seek(0, os.SEEK_CUR); err != nil {
				return 0, fmt.Errorf("seek: %s", err)
			} else if err := os.Truncate(path, n); err != nil {
				return 0, fmt.Errorf("truncate: n=%d, err=%s", n-1, err)
			}

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

	// The time between file system polling to check for new segments.
	PollInterval time.Duration
}

// NewTopicReader returns a new instance of TopicReader that reads segments
// from a path starting from a given index.
func NewTopicReader(path string, index uint64, streaming bool) *TopicReader {
	return &TopicReader{
		path:      path,
		index:     index,
		streaming: streaming,

		PollInterval: DefaultPollInterval,
	}
}

// Seek seeks to a position the current segment.
func (r *TopicReader) Seek(offset int64, whence int) (int64, error) {
	assert(whence == os.SEEK_CUR, "topic reader can only seek to a relative position")

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.file == nil {
		return 0, nil
	}
	return r.file.Seek(offset, whence)
}

// Read reads the next bytes from the reader into the buffer.
func (r *TopicReader) Read(p []byte) (int, error) {
	for {
		// Retrieve current segment file handle.
		// If the reader is closed then return EOF.
		// If we don't have a file and we're streaming then sleep and retry.
		f, err := r.File()
		if err == ErrReaderClosed {
			return 0, io.EOF
		} else if err != nil {
			return 0, err
		} else if f == nil {
			if r.streaming {
				time.Sleep(r.PollInterval)
				continue
			}
			return 0, io.EOF
		}

		// Read under lock so the underlying file cannot be closed.
		r.mu.Lock()
		n, err := f.Read(p)
		r.mu.Unlock()

		// Read into buffer.
		// If no more data is available, then retry with the next segment.
		if err == io.EOF {
			if err := r.nextSegment(); err != nil {
				return 0, fmt.Errorf("next segment: %s", err)
			}
			time.Sleep(r.PollInterval)
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
		return nil, ErrReaderClosed
	}

	// If the first file hasn't been opened then open it and seek.
	if r.file == nil {
		// Find the segment containing the index.
		// Exit if no segments are available or if path not found.
		segment, err := ReadSegmentByIndex(r.path, r.index)
		if os.IsNotExist(err) {
			return nil, nil
		} else if err == ErrTopicTruncated {
			return nil, err
		} else if err != nil {
			return nil, fmt.Errorf("segment by index: %s", err)
		} else if segment == nil {
			return nil, nil
		}

		// Open that segment file.
		f, err := os.Open(segment.Path)
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
		if err := dec.Decode(&m); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if m.Index >= seek {
			// Seek to message start.
			if _, err := f.Seek(-int64(MessageChecksumSize+MessageHeaderSize+len(m.Data)), os.SEEK_CUR); err != nil {
				return fmt.Errorf("seek: %s", err)
			}
			return nil
		}
	}
}

// nextSegment closes the current segment's file handle and opens the next segment.
func (r *TopicReader) nextSegment() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Skip if the reader is closed.
	if r.closed {
		return nil
	}

	// Find current segment index.
	index, err := strconv.ParseUint(filepath.Base(r.file.Name()), 10, 64)
	if err != nil {
		return fmt.Errorf("parse current segment index: %s", err)
	}

	// Read current segment list.
	// If no segments exist then exit.
	// If current segment is the last segment then ignore.
	segments, err := ReadSegments(r.path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("read segments: %s", err)
	} else if len(segments) == 0 {
		return nil
	} else if segments[len(segments)-1].Index == index {
		if !r.streaming {
			r.closed = true
		}
		return nil
	}

	// Loop over segments and find the next one.
	for i := range segments[:len(segments)-1] {
		if segments[i].Index == index {
			// Clear current file.
			if r.file != nil {
				r.file.Close()
				r.file = nil
			}

			// Open next segment.
			f, err := os.Open(segments[i+1].Path)
			if err != nil {
				return fmt.Errorf("open next segment: %s", err)
			}
			r.file = f
			return nil
		}
	}

	// This should only occur if our current segment was deleted.
	r.closed = true
	return nil
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
	SetTopicMaxIndexMessageType = BrokerMessageType | MessageType(0x00)
	FetchPeerShardMessageType   = BrokerMessageType | MessageType(0x10)
)

const (
	// MessageChecksumSize is the size of the encoded message checksum, in bytes.
	MessageChecksumSize = 4

	// MessageHeaderSize is the size of the encoded message header, in bytes.
	MessageHeaderSize = 2 + 8 + 8 + 4
)

// Message represents a single item in a topic.
type Message struct {
	Type    MessageType
	TopicID uint64
	Index   uint64
	Data    []byte
}

// WriteTo encodes and writes the message to a writer. Implements io.WriterTo.
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	b, err := m.MarshalBinary()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

// MarshalBinary returns a binary representation of the message.
// This implements encoding.BinaryMarshaler. An error cannot be returned.
func (m *Message) MarshalBinary() ([]byte, error) {
	// Encode message.
	b := make([]byte, MessageChecksumSize+MessageHeaderSize+len(m.Data))
	copy(b[MessageChecksumSize:], m.marshalHeader())
	copy(b[MessageChecksumSize+MessageHeaderSize:], m.Data)

	// Calculate and write the checksum.
	h := fnv.New32a()
	h.Write(b[MessageChecksumSize:])
	binary.BigEndian.PutUint32(b, h.Sum32())

	return b, nil
}

// UnmarshalBinary reads a message from a binary encoded slice.
// This implements encoding.BinaryUnmarshaler.
func (m *Message) UnmarshalBinary(b []byte) error {
	// Read checksum.
	if len(b) < MessageChecksumSize {
		return fmt.Errorf("message checksum too short: %d < %d", len(b), MessageChecksumSize)
	}
	sum := binary.BigEndian.Uint32(b)

	// Read header.
	if len(b)-MessageChecksumSize < MessageHeaderSize {
		return fmt.Errorf("message header too short: %d < %d", len(b)-MessageChecksumSize, MessageHeaderSize)
	}
	m.unmarshalHeader(b[MessageChecksumSize:])

	// Read data.
	if len(b)-MessageChecksumSize-MessageHeaderSize < len(m.Data) {
		return fmt.Errorf("message data too short: %d < %d", len(b)-MessageChecksumSize-MessageHeaderSize, len(m.Data))
	}
	copy(m.Data, b[MessageChecksumSize+MessageHeaderSize:])

	// Verify checksum.
	h := fnv.New32a()
	h.Write(b[MessageChecksumSize:])
	if h.Sum32() != sum {
		return ErrChecksum
	}

	return nil
}

// marshalHeader returns a byte slice with the message header.
func (m *Message) marshalHeader() []byte {
	b := make([]byte, MessageHeaderSize)
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
	r io.ReadSeeker
}

// NewMessageDecoder returns a new instance of the MessageDecoder.
func NewMessageDecoder(r io.ReadSeeker) *MessageDecoder {
	return &MessageDecoder{r: r}
}

// Decode reads a message from the decoder's reader.
func (dec *MessageDecoder) Decode(m *Message) error {
	// Read header bytes.
	// Unread if there is a partial read.
	var b [MessageChecksumSize + MessageHeaderSize]byte
	if n, err := io.ReadFull(dec.r, b[:]); err == io.EOF {
		return err
	} else if err == io.ErrUnexpectedEOF {
		if _, err := dec.r.Seek(-int64(n), os.SEEK_CUR); err != nil {
			return fmt.Errorf("cannot unread header: n=%d, err=%s", n, err)
		}
		warnf("unexpected eof(0): len=%d, n=%d, err=%s", len(b[:]), n, err)
		return err
	} else if err != nil {
		return err
	}

	// Read checksum.
	sum := binary.BigEndian.Uint32(b[:])

	// Read header.
	m.unmarshalHeader(b[MessageChecksumSize:])

	// Read data.
	if n, err := io.ReadFull(dec.r, m.Data); err == io.EOF || err == io.ErrUnexpectedEOF {
		if _, err := dec.r.Seek(-int64(len(b)+n), os.SEEK_CUR); err != nil {
			return fmt.Errorf("cannot unread header+data: n=%d, err=%s", n, err)
		}
		warnf("unexpected eof(1): len=%d, n=%d, err=%s", len(m.Data), n, err)
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return fmt.Errorf("read data: %s", err)
	}

	// Verify checksum.
	h := fnv.New32a()
	h.Write(b[MessageChecksumSize:])
	h.Write(m.Data)
	if h.Sum32() != sum {
		if _, err := dec.r.Seek(-int64(len(b)+len(m.Data)), os.SEEK_CUR); err != nil {
			return fmt.Errorf("cannot unread after checksum: err=%s", err)
		}
		return ErrChecksum
	}

	return nil
}

// tombstoneExists returns whether the given directory contains a tombstone
func tombstoneExists(path string) bool {
	if _, err := os.Stat(filepath.Join(path, "tombstone")); os.IsNotExist(err) {
		return false
	}
	return true
}

// UnmarshalMessage decodes a byte slice into a message.
func UnmarshalMessage(data []byte) (*Message, error) {
	m := &Message{}
	if err := m.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return m, nil
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
