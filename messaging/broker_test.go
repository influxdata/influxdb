package messaging_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure that opening a broker without a path returns an error.
func TestBroker_Open_ErrPathRequired(t *testing.T) {
	b := messaging.NewBroker()
	if err := b.Open("", &url.URL{Host: "127.0.0.1:8080"}); err != messaging.ErrPathRequired {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that opening a broker without a connection address returns an error.
func TestBroker_Open_ErrAddressRequired(t *testing.T) {
	b := messaging.NewBroker()
	f := tempfile()
	defer os.Remove(f)

	if err := b.Open(f, nil); err != messaging.ErrConnectionAddressRequired {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that closing an already closed broker returns an error.
func TestBroker_Close_ErrClosed(t *testing.T) {
	b := NewBroker(nil)
	b.Close()
	if err := b.Broker.Close(); err != messaging.ErrClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

/*
// Ensure the broker can write messages to the appropriate topics.
func TestBroker_Publish(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	// Write a message to the broker.
	index, err := b.Publish(&messaging.Message{Type: 100, TopicID: 20, Data: []byte("0000")})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if index != 4 {
		t.Fatalf("unexpected index: %d", index)
	}
	if err := b.Sync(index); err != nil {
		t.Fatalf("sync error: %s", err)
	}

	// Read message from the replica.
	var buf bytes.Buffer
	go func() {
		if _, err := b.Replica(2000).WriteTo(&buf); err != nil {
			t.Fatalf("write to: %s", err)
		}
	}()
	time.Sleep(10 * time.Millisecond)

	// Read out the config messages first.
	var m messaging.Message
	dec := messaging.NewMessageDecoder(&buf)
	if err := dec.Decode(&m); err != nil || m.Type != messaging.InternalMessageType {
		t.Fatalf("decode(internal): %x (%v)", m.Type, err)
	}
	if err := dec.Decode(&m); err != nil || m.Type != messaging.CreateReplicaMessageType {
		t.Fatalf("decode(create replica): %x (%v)", m.Type, err)
	}
	if err := dec.Decode(&m); err != nil || m.Type != messaging.SubscribeMessageType {
		t.Fatalf("decode(subscribe): %x (%v)", m.Type, err)
	}

	// Read out the published message.
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("decode: %s", err)
	} else if !reflect.DeepEqual(&m, &messaging.Message{Type: 100, TopicID: 20, Index: 4, Data: []byte("0000")}) {
		t.Fatalf("unexpected message: %#v", &m)
	}

	// Unsubscribe replica from the topic.
	if err := b.Unsubscribe(2000, 20); err != nil {
		t.Fatalf("unsubscribe: %s", err)
	}

	// Write another message (that shouldn't be read).
	if _, err := b.Publish(&messaging.Message{Type: 101, TopicID: 20}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Read unsubscribe.
	if err := dec.Decode(&m); err != nil || m.Type != messaging.UnsubscribeMessageType {
		t.Fatalf("decode(unsubscribe): %x (%v)", m.Type, err)
	}

	// EOF
	if err := dec.Decode(&m); err != io.EOF {
		t.Fatalf("decode(eof): %x (%v)", m.Type, err)
	}
}


// Ensure the broker can reopen and recover correctly.
func TestBroker_Reopen(t *testing.T) {
	t.Skip("pending")
}

// Benchmarks a single broker without HTTP.
func BenchmarkBroker_Publish(b *testing.B) {
	br := NewBroker(nil)
	defer br.Close()

	b.ResetTimer()

	var index uint64
	for i := 0; i < b.N; i++ {
		var err error
		index, err = br.Publish(&messaging.Message{Type: 0, TopicID: 1, Data: make([]byte, 50)})
		if err != nil {
			b.Fatalf("unexpected error: %s", err)
		}
	}

	// Wait for the broker to commit.
	if err := br.Sync(index); err != nil {
		b.Fatalf("sync error: %s", err)
	}
}
*/

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
		{index: 5, segmentIndex: 6, err: messaging.ErrSegmentReclaimed},
	} {
		segment, err := messaging.ReadSegmentByIndex(path, tt.index)
		if err != nil {
			t.Errorf("%d. %d: error: %s", i, tt.index, err)
		} else if tt.segmentIndex != segment.Index {
			t.Errorf("%d. %d: index mismatch: exp=%d got=%d", i, tt.index, tt.segmentIndex, segment.Index)
		}
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
			t.Fatalf("%d. result mismatch:\n\nexp=%#v\n\ngot=%#v", i, tt.results, results)
		}
	}

}

// Broker is a wrapper for broker.Broker that creates the broker in a temporary location.
type Broker struct {
	*messaging.Broker
}

// NewBroker returns a new open tempoarary broker.
func NewBroker(u *url.URL) *Broker {
	b := NewUninitializedBroker(u)
	if err := b.Initialize(); err != nil {
		panic("initialize: " + err.Error())
	}
	return b
}

// NewUninitializedBroker returns a new broker that has not been initialized.
func NewUninitializedBroker(u *url.URL) *Broker {
	// Default the broker URL if not passed in.
	if u == nil {
		u = &url.URL{Scheme: "http", Host: "127.0.0.1:8080"}
	}

	// Open a new broker.
	b := messaging.NewBroker()
	if err := b.Open(tempfile(), u); err != nil {
		panic("open: " + err.Error())
	}
	return &Broker{b}
}

// Close closes and deletes the temporary broker.
func (b *Broker) Close() {
	defer os.RemoveAll(b.Path())
	b.Broker.Close()
}

// MustSync syncs to a broker index. Panic on error.
func (b *Broker) MustSync(index uint64) {
	if err := b.Sync(index); err != nil {
		panic(err.Error())
	}
}

// MustPublish publishes a message to the broker. Panic on error.
func (b *Broker) MustPublish(m *messaging.Message) uint64 {
	index, err := b.Publish(&messaging.Message{Type: 100, TopicID: 20, Data: []byte("0000")})
	if err != nil {
		panic(err.Error())
	}
	return index
}

// MustPublishSync publishes a message to the broker and syncs to that index. Panic on error.
func (b *Broker) MustPublishSync(m *messaging.Message) uint64 {
	index := b.MustPublish(m)
	b.MustSync(index)
	return index
}

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
	if err := ioutil.WriteFile(filename, data, 0600); err != nil {
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
