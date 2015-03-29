package messaging_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdb/influxdb/messaging"
	"github.com/influxdb/influxdb/raft"
)

func init() {
	// Ensure the broker matches the handler's interface.
	_ = messaging.Handler{Broker: messaging.NewBroker()}
}

// Ensure that opening a broker without a path returns an error.
func TestBroker_Open_ErrPathRequired(t *testing.T) {
	b := messaging.NewBroker()
	if err := b.Open(""); err != messaging.ErrPathRequired {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that closing an already closed broker returns an error.
func TestBroker_Close_ErrClosed(t *testing.T) {
	b := NewBroker()
	b.Close()
	if err := b.Broker.Close(); err != messaging.ErrClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the broker can write messages to the appropriate topics.
func TestBroker_Publish(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	b.Log().ApplyFunc = func(data []byte) (uint64, error) {
		var m messaging.Message
		dec := messaging.NewMessageDecoder(bytes.NewReader(data))
		if err := dec.Decode(&m); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(&m, &messaging.Message{Type: 100, TopicID: 20, Data: []byte("0000")}) {
			t.Fatalf("unexpected message: %#v", &m)
		}
		return 2, nil
	}

	// Write a message to the broker.
	index, err := b.Publish(&messaging.Message{Type: 100, TopicID: 20, Data: []byte("0000")})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if index != 2 {
		t.Fatalf("unexpected index: %d", index)
	}
}

// Ensure the broker can apply messages from the log.
func TestBroker_Apply(t *testing.T) {
	b := OpenBroker()
	defer b.Close()

	// Write two message to topic #20, one message to topic #21.
	if err := b.Apply(&messaging.Message{Index: 2, TopicID: 20, Data: []byte{0}}); err != nil {
		t.Fatal(err)
	} else if err := b.Apply(&messaging.Message{Index: 3, TopicID: 21, Data: []byte{100}}); err != nil {
		t.Fatal(err)
	} else if err := b.Apply(&messaging.Message{Index: 4, TopicID: 20, Data: []byte{200}}); err != nil {
		t.Fatal(err)
	}

	// Ensure topic exists.
	if topic := b.Topic(20); topic == nil {
		t.Fatal("topic not created")
	}

	// Read message from topic.
	r := b.TopicReader(20, 0, false)
	defer r.Close()
	dec := messaging.NewMessageDecoder(r)

	var m messaging.Message
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("message decode error: %s", err)
	} else if !reflect.DeepEqual(&m, &messaging.Message{Index: 2, TopicID: 20, Data: []byte{0}}) {
		t.Fatalf("unexpected message: %#v", &m)
	}
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("message decode error: %s", err)
	} else if !reflect.DeepEqual(&m, &messaging.Message{Index: 4, TopicID: 20, Data: []byte{200}}) {
		t.Fatalf("unexpected message: %#v", &m)
	}

	// Verify broker high water mark.
	if index := b.Index(); index != 4 {
		t.Fatalf("unexpected broker index: %d", index)
	}
}

// Ensure the broker can apply topic high water mark messages.
func TestBroker_Apply_SetMaxTopicIndex(t *testing.T) {
	b := OpenBroker()
	defer b.Close()

	// Write data to topic so it exists.
	if err := b.Apply(&messaging.Message{Index: 1, TopicID: 20}); err != nil {
		t.Fatal(err)
	} else if b.Topic(20) == nil {
		t.Fatal("topic not created")
	}

	// Set topic #1's index to "2".
	if err := b.Apply(&messaging.Message{
		Index: 2,
		Type:  messaging.SetTopicMaxIndexMessageType,
		Data:  []byte{0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 5}, // topicID=20, index=5
	}); err != nil {
		t.Fatalf("apply error: %s", err)
	}
	if topic := b.Topic(20); topic.Index() != 5 {
		t.Fatalf("unexpected topic index: %d", topic.Index())
	}
}

// Ensure the broker can read from topics after reopening.
func TestBroker_Reopen(t *testing.T) {
	b := OpenBroker()
	defer b.Close()

	// Write two message to topic #20, one message to topic #21.
	if err := b.Apply(&messaging.Message{Index: 2, TopicID: 20, Data: []byte{0}}); err != nil {
		t.Fatal(err)
	} else if err := b.Apply(&messaging.Message{Index: 3, TopicID: 21, Data: []byte{100}}); err != nil {
		t.Fatal(err)
	} else if err := b.Apply(&messaging.Message{Index: 4, TopicID: 20, Data: []byte{200}}); err != nil {
		t.Fatal(err)
	}

	// Reopen the broker.
	b.MustReopen()

	// Ensure topics exist.
	if b.Topic(20) == nil {
		t.Fatal("topic(20) not created")
	} else if b.Topic(21) == nil {
		t.Fatal("topic(21) not created")
	}

	// Verify broker high water mark.
	if index := b.Index(); index != 4 {
		t.Fatalf("unexpected broker index: %d", index)
	}

	// Read messages from topic.
	if a := b.MustReadAllTopic(20); len(a) != 2 {
		t.Fatalf("unexpected message count: %d", len(a))
	} else if !reflect.DeepEqual(a[0], &messaging.Message{Index: 2, TopicID: 20, Data: []byte{0}}) {
		t.Fatalf("unexpected message(0): %d", a[0])
	} else if !reflect.DeepEqual(a[1], &messaging.Message{Index: 4, TopicID: 20, Data: []byte{200}}) {
		t.Fatalf("unexpected message(1): %d", a[1])
	}
}

// Ensure the broker can snapshot and restore its data.
func TestBroker_Snapshot(t *testing.T) {
	b0 := OpenBroker()
	defer b0.Close()

	// Write messages to the first broker.
	if err := b0.Apply(&messaging.Message{Index: 2, TopicID: 20, Data: []byte{0}}); err != nil {
		t.Fatal(err)
	} else if err := b0.Apply(&messaging.Message{Index: 3, TopicID: 21, Data: []byte{100}}); err != nil {
		t.Fatal(err)
	} else if err := b0.Apply(&messaging.Message{Index: 4, TopicID: 20, Data: []byte{200}}); err != nil {
		t.Fatal(err)
	}

	// Snapshot the first broker.
	var buf bytes.Buffer
	if _, err := b0.WriteTo(&buf); err != nil {
		t.Fatalf("snapshot error: %s", err)
	}

	// Restore to the second broker.
	b1 := OpenBroker()
	defer b1.Close()
	if _, err := b1.ReadFrom(&buf); err != nil {
		t.Fatalf("restore error: %s", err)
	}

	// Ensure topic exists.
	if topic := b1.Topic(20); topic == nil {
		t.Fatal("topic not created")
	}

	// Read message from topic.
	r := b1.TopicReader(20, 0, false)
	defer r.Close()
	dec := messaging.NewMessageDecoder(r)

	var m messaging.Message
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("message decode error: %s", err)
	} else if !reflect.DeepEqual(&m, &messaging.Message{Index: 2, TopicID: 20, Data: []byte{0}}) {
		t.Fatalf("unexpected message: %#v", &m)
	}
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("message decode error: %s", err)
	} else if !reflect.DeepEqual(&m, &messaging.Message{Index: 4, TopicID: 20, Data: []byte{200}}) {
		t.Fatalf("unexpected message: %#v", &m)
	}

	// Verify broker high water mark.
	if index := b1.Index(); index != 4 {
		t.Fatalf("unexpected broker index: %d", index)
	}
}

// Ensure the broker can set the topic high water mark.
func TestBroker_SetTopicMaxIndex(t *testing.T) {
	b := OpenBroker()
	defer b.Close()

	// Ensure the appropriate message is sent to the log.
	b.Log().ApplyFunc = func(data []byte) (uint64, error) {
		m, _ := messaging.UnmarshalMessage(data)
		if !bytes.Equal(m.Data[0:8], []byte{0, 0, 0, 0, 0, 0, 0, 1}) {
			t.Fatalf("unexpected topic id data: %x", data[0:8])
		} else if !bytes.Equal(m.Data[8:16], []byte{0, 0, 0, 0, 0, 0, 0, 2}) {
			t.Fatalf("unexpected index data: %x", data[8:16])
		}
		return 1, nil
	}

	// Set the highest replicated topic index.
	if err := b.SetTopicMaxIndex(1, 2); err != nil {
		t.Fatal(err)
	}
}

// Ensure the FSM can apply messages.
func TestRaftFSM_MustApply_Message(t *testing.T) {
	fsm := NewRaftFSM()

	// Ensure index is added to message.
	var called bool
	fsm.Broker().ApplyFunc = func(m *messaging.Message) error {
		if !reflect.DeepEqual(m, &messaging.Message{Index: 2, TopicID: 20, Data: []byte{}}) {
			t.Fatalf("unexpected message: %#v", m)
		}
		called = true
		return nil
	}

	// Encode message and apply it as a log entry.
	m := messaging.Message{TopicID: 20}
	data, _ := m.MarshalBinary()
	if err := fsm.Apply(&raft.LogEntry{Index: 2, Data: data}); err != nil {
		t.Fatal(err)
	} else if !called {
		t.Fatal("Apply() not called")
	}
}

// Ensure the FSM can move the index forward with raft internal messages.
func TestRaftFSM_MustApply_Internal(t *testing.T) {
	fsm := NewRaftFSM()

	// Ensure index is added to message.
	var called bool
	fsm.Broker().SetMaxIndexFunc = func(index uint64) error {
		if index != 2 {
			t.Fatalf("unexpected index: %#v", index)
		}
		called = true
		return nil
	}

	// Encode message and apply it as a log entry.
	if err := fsm.Apply(&raft.LogEntry{Type: raft.LogEntryAddPeer, Index: 2}); err != nil {
		t.Fatal(err)
	} else if !called {
		t.Fatal("Apply() not called")
	}
}

// RaftFSM is a mockable wrapper around messaging.RaftFSM.
type RaftFSM struct {
	*messaging.RaftFSM
}

// NewRaftFSM returns a new instance of RaftFSM.
func NewRaftFSM() *RaftFSM {
	return &RaftFSM{
		&messaging.RaftFSM{Broker: &RaftFSMBroker{}},
	}
}

func (fsm *RaftFSM) Broker() *RaftFSMBroker { return fsm.RaftFSM.Broker.(*RaftFSMBroker) }

// RaftFSMBroker is a mockable object implementing RaftFSM.Broker.
type RaftFSMBroker struct {
	ApplyFunc       func(*messaging.Message) error
	SetMaxIndexFunc func(uint64) error
}

func (b *RaftFSMBroker) Apply(m *messaging.Message) error { return b.ApplyFunc(m) }
func (b *RaftFSMBroker) SetMaxIndex(index uint64) error   { return b.SetMaxIndexFunc(index) }

func (b *RaftFSMBroker) Index() uint64                             { return 0 }
func (b *RaftFSMBroker) WriteTo(w io.Writer) (n int64, err error)  { return 0, nil }
func (b *RaftFSMBroker) ReadFrom(r io.Reader) (n int64, err error) { return 0, nil }

// Ensure a list of topics can be read from a directory.
func TestReadTopics(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	MustWriteFile(filepath.Join(path, "1/12"), []byte{})      // ok
	MustWriteFile(filepath.Join(path, "1/20"), []byte{})      // ok
	MustWriteFile(filepath.Join(path, "312/35"), []byte{})    // ok
	MustWriteFile(filepath.Join(path, "meta/data"), []byte{}) // non-numeric name
	MustWriteFile(filepath.Join(path, "123"), []byte{})       // non-directory

	a, err := messaging.ReadTopics(path)
	if err != nil {
		t.Fatal(err)
	} else if len(a) != 2 {
		t.Fatalf("unexpected count: %d", len(a))
	} else if a[0].ID() != 1 {
		t.Fatalf("unexpected topic(0) id: %d", a[0].ID())
	} else if a[0].Path() != filepath.Join(path, "1") {
		t.Fatalf("unexpected topic(0) path: %s", a[0].Path())
	} else if a[1].ID() != 312 {
		t.Fatalf("unexpected topic(1) id: %d", a[1].ID())
	}
}

// Ensure a list of segments can be read from a directory.
func TestReadSegments(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	MustWriteFile(filepath.Join(path, "12"), []byte{})
	MustWriteFile(filepath.Join(path, "118332"), []byte{})
	MustWriteFile(filepath.Join(path, "6"), []byte{})
	MustWriteFile(filepath.Join(path, "xxx"), []byte{})

	segments, err := messaging.ReadSegments(path)
	if err != nil {
		t.Fatal(err)
	} else if len(segments) != 3 {
		t.Fatalf("unexpected segment count: %d", len(segments))
	} else if segments[0].Index != 6 {
		t.Fatalf("unexpected segment(0) index: %d", segments[0].Index)
	} else if segments[0].Path != filepath.Join(path, "6") {
		t.Fatalf("unexpected segment(0) path: %s", segments[0].Path)
	} else if segments[1].Index != 12 {
		t.Fatalf("unexpected segment(1) index: %d", segments[1].Index)
	} else if segments[2].Index != 118332 {
		t.Fatalf("unexpected segment(2) index: %d", segments[2].Index)
	}
}

// Ensure a list of segments returns an error if the path doesn't exist.
func TestReadSegments_ENOENT(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	os.RemoveAll(path)

	_, err := messaging.ReadSegments(path)
	if err == nil || !strings.Contains(err.Error(), "no such file or directory") {
		t.Fatal(err)
	}
}

// Ensure the appropriate segment can be found by index.
func TestReadSegmentByIndex(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	MustWriteFile(filepath.Join(path, "6"), []byte{})
	MustWriteFile(filepath.Join(path, "12"), []byte{})
	MustWriteFile(filepath.Join(path, "20"), []byte{})

	for i, tt := range []struct {
		index        uint64
		segmentIndex uint64
		err          error
	}{
		{index: 0, segmentIndex: 6},
		{index: 6, segmentIndex: 6},
		{index: 7, segmentIndex: 6},
		{index: 11, segmentIndex: 6},
		{index: 12, segmentIndex: 12},
		{index: 13, segmentIndex: 12},
		{index: 19, segmentIndex: 12},
		{index: 20, segmentIndex: 20},
		{index: 21, segmentIndex: 20},
	} {
		segment, err := messaging.ReadSegmentByIndex(path, tt.index)
		if tt.err != nil {
			if tt.err != err {
				t.Errorf("%d. %d: error mismatch: exp=%s, got=%s", i, tt.index, tt.err, err)
			}
		} else if err != nil {
			t.Errorf("%d. %d: unexpected error: %s", i, tt.index, err)
		} else if tt.segmentIndex != segment.Index {
			t.Errorf("%d. %d: index mismatch: exp=%d, got=%d", i, tt.index, tt.segmentIndex, segment.Index)
		}
	}
}

// Ensure reading a segment by index with no segments returns a nil segment.
func TestReadSegmentByIndex_NoSegment(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	segment, err := messaging.ReadSegmentByIndex(path, 0)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if segment != nil {
		t.Fatalf("expected nil segment: %#v", segment)
	}
}

// Ensure reading a segment by index fails if the path doesn't exist.
func TestReadSegmentByIndex_ENOENT(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	os.RemoveAll(path)

	_, err := messaging.ReadSegmentByIndex(path, 0)
	if err == nil || !strings.Contains(err.Error(), "no such file or directory") {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure a topic reader can read messages in order from a given index.
func TestTopicReader(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	// Generate segments in directory.
	MustWriteFile(filepath.Join(path, "6"),
		MustMarshalMessages([]*messaging.Message{
			{Index: 6},
			{Index: 7},
			{Index: 10},
		}),
	)
	MustWriteFile(filepath.Join(path, "12"),
		MustMarshalMessages([]*messaging.Message{
			{Index: 12},
		}),
	)
	MustWriteFile(filepath.Join(path, "13"),
		MustMarshalMessages([]*messaging.Message{
			{Index: 13},
			{Index: 14},
		}),
	)

	// Execute table tests.
	for i, tt := range []struct {
		index   uint64   // starting index
		results []uint64 // returned indices
	}{
		{index: 0, results: []uint64{6, 7, 10, 12, 13, 14}},
		{index: 6, results: []uint64{6, 7, 10, 12, 13, 14}},
		{index: 7, results: []uint64{7, 10, 12, 13, 14}},
		{index: 9, results: []uint64{10, 12, 13, 14}},
		{index: 10, results: []uint64{10, 12, 13, 14}},
		{index: 11, results: []uint64{12, 13, 14}},
		{index: 12, results: []uint64{12, 13, 14}},
		{index: 13, results: []uint64{13, 14}},
		{index: 14, results: []uint64{14}},
		{index: 15, results: []uint64{}},
	} {
		// Start topic reader from an index.
		r := messaging.NewTopicReader(path, tt.index, false)

		// Slurp all message ids from the reader.
		results := make([]uint64, 0)
		dec := messaging.NewMessageDecoder(r)
		for {
			m := &messaging.Message{}
			if err := dec.Decode(m); err == io.EOF {
				break
			} else if err != nil {
				t.Fatalf("%d. decode error: %s", i, err)
			} else {
				results = append(results, m.Index)
			}
		}

		// Verify the retrieved indices match what's expected.
		if !reflect.DeepEqual(results, tt.results) {
			t.Fatalf("%d. %v: result mismatch:\n\nexp=%#v\n\ngot=%#v", i, tt.index, tt.results, results)
		}
	}
}

// Ensure a topic reader can stream new messages.
func TestTopicReader_streaming(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	// Start topic reader from the beginning.
	r := messaging.NewTopicReader(path, 0, true)
	r.PollInterval = 1 * time.Millisecond

	// Write a segments with delays.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(2 * time.Millisecond)
		MustWriteFile(filepath.Join(path, "6"),
			MustMarshalMessages([]*messaging.Message{
				{Index: 6},
				{Index: 7},
				{Index: 10},
			}),
		)

		// Write two more segments.
		time.Sleep(5 * time.Millisecond)
		MustWriteFile(filepath.Join(path, "12"),
			MustMarshalMessages([]*messaging.Message{
				{Index: 12},
			}),
		)

		MustWriteFile(filepath.Join(path, "13"),
			MustMarshalMessages([]*messaging.Message{
				{Index: 13},
				{Index: 14},
			}),
		)
	}()

	// Slurp all message ids from the reader.
	indices := make([]uint64, 0)
	dec := messaging.NewMessageDecoder(r)
	for {
		m := &messaging.Message{}
		if err := dec.Decode(m); err == io.EOF {
			t.Fatalf("unexpected EOF")
		} else if err != nil {
			t.Fatalf("decode error: %s", err)
		} else {
			indices = append(indices, m.Index)
		}

		if m.Index == 14 {
			break
		}
	}

	// Verify we received the correct indices.
	if !reflect.DeepEqual(indices, []uint64{6, 7, 10, 12, 13, 14}) {
		t.Fatalf("unexpected indices: %#v", indices)
	}

	r.Close()
}

// Ensure multiple topic readers can read from the same topic directory.
func BenchmarkTopicReaderStreaming(b *testing.B) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	// Configurable settings.
	readerN := 10   // number of readers
	messageN := b.N // total message count
	dataSize := 50  // per message data size
	pollInterval := 1 * time.Millisecond

	// Create a topic to write into.
	topic := messaging.NewTopic(1, path)
	topic.MaxSegmentSize = 64 * 1024 // 64KB
	if err := topic.Open(); err != nil {
		b.Fatal(err)
	}
	defer topic.Close()

	// Stream from multiple readers in parallel.
	var wg sync.WaitGroup
	wg.Add(readerN)
	readers := make([]*messaging.TopicReader, readerN)
	for i := range readers {
		r := messaging.NewTopicReader(path, 0, true)
		r.PollInterval = pollInterval
		readers[i] = r

		// Read messages in sequence.
		go func(r *messaging.TopicReader) {
			defer r.Close()
			defer wg.Done()

			var index uint64
			dec := messaging.NewMessageDecoder(r)
			for {
				var m messaging.Message
				if err := dec.Decode(&m); err == io.EOF {
					b.Fatalf("unexpected EOF")
				} else if err != nil {
					b.Fatalf("decode error: %s", err)
				} else if index+1 != m.Index {
					b.Fatalf("out of order: %d..%d", index, m.Index)
				}
				index = m.Index

				if index == uint64(messageN) {
					break
				}
			}
		}(r)
	}

	// Write messages into topic but stagger them by small, random intervals.
	for i := 0; i < messageN; i++ {
		time.Sleep(time.Duration(rand.Intn(int(pollInterval))))

		index := uint64(i) + 1
		if err := topic.WriteMessage(&messaging.Message{Index: index, Data: make([]byte, dataSize)}); err != nil {
			b.Fatalf("write message error: %s", err)
		}
	}

	wg.Wait()
}

// Broker is a wrapper for broker.Broker that creates the broker in a temporary location.
type Broker struct {
	*messaging.Broker
}

// NewBroker returns a new Broker instance with a mockable log.
func NewBroker() *Broker {
	b := &Broker{messaging.NewBroker()}
	b.Broker.Log = &BrokerLog{}
	return b
}

// OpenBroker returns a new, open Broker instance.
func OpenBroker() *Broker {
	b := NewBroker()
	if err := b.Open(tempfile()); err != nil {
		panic("open: " + err.Error())
	}
	return b
}

// Close closes and deletes the temporary broker.
func (b *Broker) Close() {
	defer os.RemoveAll(b.Path())
	b.Broker.Close()
}

// MustReopen closes and reopens the broker in a new instance.
func (b *Broker) MustReopen() {
	path, log := b.Broker.Path(), b.Broker.Log
	b.Broker.Close()

	b.Broker = messaging.NewBroker()
	b.Broker.Log = log

	if err := b.Open(path); err != nil {
		panic("reopen: " + err.Error())
	}
}

// Log returns the mock broker log on the underlying broker.
func (b *Broker) Log() *BrokerLog {
	return b.Broker.Log.(*BrokerLog)
}

// MustReadAllTopic reads all messages on a topic. Panic on error.
func (b *Broker) MustReadAllTopic(topicID uint64) (a []*messaging.Message) {
	r := b.TopicReader(topicID, 0, false)
	defer r.Close()

	dec := messaging.NewMessageDecoder(r)
	for {
		m := &messaging.Message{}
		if err := dec.Decode(m); err == io.EOF {
			return
		} else if err != nil {
			panic("read all topic: " + err.Error())
		}
		a = append(a, m)
	}
}

// BrokerLog is a mockable object that implements Broker.Log.
type BrokerLog struct {
	ApplyFunc     func(data []byte) (uint64, error)
	ClusterIDFunc func() uint64
	IsLeaderFunc  func() bool
	LeaderFunc    func() (uint64, url.URL)
	URLFunc       func() url.URL
	URLsFunc      func() []url.URL
}

func (l *BrokerLog) Apply(data []byte) (uint64, error) { return l.ApplyFunc(data) }
func (l *BrokerLog) ClusterID() uint64                 { return l.ClusterIDFunc() }
func (l *BrokerLog) IsLeader() bool                    { return l.IsLeaderFunc() }
func (l *BrokerLog) Leader() (uint64, url.URL)         { return l.LeaderFunc() }
func (l *BrokerLog) URL() url.URL                      { return l.URLFunc() }
func (l *BrokerLog) URLs() []url.URL                   { return l.URLsFunc() }

// Messages represents a collection of messages.
// This type provides helper functions.
type Messages []*messaging.Message

// First returns the first message in the collection.
func (a Messages) First() *messaging.Message {
	if len(a) == 0 {
		return nil
	}
	return a[0]
}

// Last returns the last message in the collection.
func (a Messages) Last() *messaging.Message {
	if len(a) == 0 {
		return nil
	}
	return a[len(a)-1]
}

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "influxdb-messaging-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

// MustWriteFile writes data to a file. Panic on error.
func MustWriteFile(filename string, data []byte) {
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		panic(err.Error())
	}
	if err := ioutil.WriteFile(filename, data, 0666); err != nil {
		panic(err.Error())
	}
}

// MustMarshalMessages marshals a slice of messages to bytes. Panic on error.
func MustMarshalMessages(a []*messaging.Message) []byte {
	var buf bytes.Buffer
	for _, m := range a {
		if _, err := m.WriteTo(&buf); err != nil {
			panic(err.Error())
		}
	}
	return buf.Bytes()
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
