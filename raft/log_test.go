package raft_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"testing/quick"

	"github.com/influxdb/influxdb/raft"
)

// Ensure that a new log can be successfully opened and closed.
func TestLog_Open(t *testing.T) {
	l := NewUnopenedTestLog()
	if err := l.Open(tempfile()); err != nil {
		t.Fatal("open: ", err)
	} else if !l.Opened() {
		t.Fatal("expected log to be open")
	}
	if err := l.Close(); err != nil {
		t.Fatal("close: ", err)
	} else if l.Opened() {
		t.Fatal("expected log to be closed")
	}
}

// Ensure that log entries can be encoded to a writer.
func TestLogEntryEncoder_Encode(t *testing.T) {
	var buf bytes.Buffer

	// Encode the entry to the buffer.
	enc := raft.NewLogEntryEncoder(&buf)
	if err := enc.Encode(&raft.LogEntry{Index: 2, Term: 3, Data: []byte{4, 5, 6}}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Check that the encoded bytes match what's expected.
	exp := []byte{0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6}
	if v := buf.Bytes(); !bytes.Equal(exp, v) {
		t.Fatalf("value:\n\nexp: %x\n\ngot: %x\n\n", exp, v)
	}
}

// Ensure that log entries can be decoded from a reader.
func TestLogEntryDecoder_Decode(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x10, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6})

	// Create a blank entry and an expected result.
	entry := &raft.LogEntry{}

	// Decode the entry from the buffer.
	dec := raft.NewLogEntryDecoder(buf)
	if err := dec.Decode(entry); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if index := uint64(2); index != entry.Index {
		t.Fatalf("index: exp: %v, got: %v", index, entry.Index)
	}
	if term := uint64(3); term != entry.Term {
		t.Fatalf("term: exp: %v, got: %v", term, entry.Term)
	}
	if data := []byte{4, 5, 6}; !bytes.Equal(data, entry.Data) {
		t.Fatalf("data: exp: %x, got: %x", data, entry.Term)
	}
}

// Ensure that random entries can be encoded and decoded correctly.
func TestLogEntryEncodeDecode(t *testing.T) {
	f := func(entries []raft.LogEntry) bool {
		var buf bytes.Buffer
		enc := raft.NewLogEntryEncoder(&buf)
		dec := raft.NewLogEntryDecoder(&buf)

		// Encode entries.
		for _, e := range entries {
			if e.Type == 0xFF {
				buf.WriteByte(0xFF)
				continue
			}
			if err := enc.Encode(&e); err != nil {
				t.Fatalf("encode: %s", err)
			}
		}

		// Decode entries.
		for _, e := range entries {
			var entry raft.LogEntry
			if err := dec.Decode(&entry); err != nil {
				t.Fatalf("decode: %s", err)
			} else if entry.Type == 0xFF {
				if !reflect.DeepEqual(&entry, &raft.LogEntry{Type: 0xFF}) {
					t.Fatalf("invalid snapshot entry: %#v", &entry)
				}
			} else if !reflect.DeepEqual(e, entry) {
				t.Fatalf("mismatch:\n\nexp: %#v\n\ngot: %#v\n\n", e, entry)
			}
		}

		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func BenchmarkLogEntryEncoderEncode_8b(b *testing.B)  { benchmarkLogEntryEncoderEncode(b, 8) }
func BenchmarkLogEntryEncoderEncode_32b(b *testing.B) { benchmarkLogEntryEncoderEncode(b, 32) }

func benchmarkLogEntryEncoderEncode(b *testing.B, sz int) {
	var buf bytes.Buffer
	enc := raft.NewLogEntryEncoder(&buf)
	entry := &raft.LogEntry{Data: make([]byte, sz)}

	// Record single encoding size.
	enc.Encode(entry)
	b.SetBytes(int64(buf.Len()))
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(entry); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	runtime.GC()
}

func BenchmarkLogEntryDecoderDecode_8b(b *testing.B)  { benchmarkLogEntryDecoderDecode(b, 8) }
func BenchmarkLogEntryDecoderDecode_32b(b *testing.B) { benchmarkLogEntryDecoderDecode(b, 32) }

func benchmarkLogEntryDecoderDecode(b *testing.B, sz int) {
	var buf bytes.Buffer
	enc := raft.NewLogEntryEncoder(&buf)
	dec := raft.NewLogEntryDecoder(&buf)

	// Encode a single record and record its size.
	enc.Encode(&raft.LogEntry{Data: make([]byte, sz)})
	b.SetBytes(int64(buf.Len()))

	// Encode all the records on the buffer first.
	buf.Reset()
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(&raft.LogEntry{Data: make([]byte, sz)}); err != nil {
			b.Fatalf("encode: %s", err)
		}
	}
	b.ReportAllocs()

	// Decode from the buffer.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var entry raft.LogEntry
		if err := dec.Decode(&entry); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	runtime.GC()
}

// TestLog wraps the raft.Log to provide helper test functions.
type TestLog struct {
	*raft.Log
}

// NewTestLog returns a new, opened instance of TestLog.
func NewTestLog() *TestLog {
	l := NewUnopenedTestLog()
	if err := l.Open(tempfile()); err != nil {
		log.Fatalf("open: %s", err)
	}
	go func() { l.Clock.Add(l.ElectionTimeout) }()
	if err := l.Initialize(); err != nil {
		log.Fatalf("initialize: %s", err)
	}
	return l
}

// NewUnopenedTestLog returns a new, unopened instance of TestLog.
// The log uses mock clock by default.
func NewUnopenedTestLog() *TestLog {
	l := &TestLog{Log: raft.NewLog()}
	l.Log.FSM = &TestFSM{}
	l.URL, _ = url.Parse("//node")
	l.Log.Clock = raft.NewMockClock()
	l.Log.Rand = seq()
	return l
}

// Close closes the log and removes the underlying data.
func (t *TestLog) Close() error {
	defer os.RemoveAll(t.Path())
	return t.Log.Close()
}

// Ensure that the config can be marshaled and unmarshaled.
func TestConfig_MarshalJSON(t *testing.T) {
	tests := []struct {
		c   *raft.Config
		out string
	}{
		// 0. No nodes.
		{
			c:   &raft.Config{ClusterID: 100},
			out: `{"clusterID":100}`,
		},

		// 1. One node.
		{
			c:   &raft.Config{ClusterID: 100, Nodes: []*raft.Node{&raft.Node{ID: 1, URL: &url.URL{Host: "localhost"}}}},
			out: `{"clusterID":100,"nodes":[{"id":1,"url":"//localhost"}]}`,
		},

		// 1. Node without URL.
		{
			c:   &raft.Config{ClusterID: 100, Nodes: []*raft.Node{&raft.Node{ID: 1, URL: nil}}},
			out: `{"clusterID":100,"nodes":[{"id":1}]}`,
		},
	}
	for i, tt := range tests {
		b, err := json.Marshal(tt.c)
		if err != nil {
			t.Fatalf("%d. unexpected marshaling error: %s", i, err)
		} else if string(b) != tt.out {
			t.Fatalf("%d. unexpected json: %s", i, b)
		}

		var config *raft.Config
		if err := json.Unmarshal(b, &config); err != nil {
			t.Fatalf("%d. unexpected marshaling error: %s", i, err)
		} else if !reflect.DeepEqual(tt.c, config) {
			t.Fatalf("%d. config:\n\nexp: %#v\n\ngot: %#v", i, tt.c, config)
		}
	}
}

// TestFSM represents a fake state machine that simple records all commands.
type TestFSM struct {
	Log      *raft.Log `json:"-"`
	MaxIndex uint64
	Commands [][]byte
}

func (fsm *TestFSM) Apply(entry *raft.LogEntry) error {
	fsm.MaxIndex = entry.Index
	if entry.Type == raft.LogEntryCommand {
		fsm.Commands = append(fsm.Commands, entry.Data)
	}
	return nil
}

func (fsm *TestFSM) Index() (uint64, error) { return fsm.MaxIndex, nil }
func (fsm *TestFSM) Snapshot(w io.Writer) (uint64, error) {
	b, _ := json.Marshal(fsm)
	binary.Write(w, binary.BigEndian, uint64(len(b)))
	_, err := w.Write(b)
	return fsm.MaxIndex, err
}
func (fsm *TestFSM) Restore(r io.Reader) error {
	var sz uint64
	if err := binary.Read(r, binary.BigEndian, &sz); err != nil {
		return err
	}
	buf := make([]byte, sz)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return json.Unmarshal(buf, &fsm)
}

// BufferCloser represents a bytes.Buffer that provides a no-op close.
type BufferCloser struct {
	*bytes.Buffer
}

func (b *BufferCloser) Close() error { return nil }

// seq implements the raft.Log#Rand interface and returns incrementing ints.
func seq() func() int64 {
	var i int64
	var mu sync.Mutex

	return func() int64 {
		mu.Lock()
		defer mu.Unlock()

		i++
		return i
	}
}
