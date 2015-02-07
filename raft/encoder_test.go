package raft_test

import (
	"bytes"
	"io"
	"reflect"
	"runtime"
	"testing"
	"testing/quick"

	"github.com/influxdb/influxdb/raft"
)

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

// Ensure that the encoder can handle write errors during encoding of header.
func TestLogEntryEncoder_Encode_ErrShortWrite_Header(t *testing.T) {
	w := newLimitWriter(23)
	enc := raft.NewLogEntryEncoder(w)
	if err := enc.Encode(&raft.LogEntry{Data: []byte{0, 0, 0, 0}}); err != io.ErrShortWrite {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the encoder can handle write errors during encoding of the data.
func TestLogEntryEncoder_Encode_ErrShortWrite_Data(t *testing.T) {
	w := newLimitWriter(25)
	enc := raft.NewLogEntryEncoder(w)
	if err := enc.Encode(&raft.LogEntry{Data: []byte{0, 0, 0, 0}}); err != io.ErrShortWrite {
		t.Fatalf("unexpected error: %s", err)
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

// Ensure the decoder returns EOF when no more data is available.
func TestLogEntryDecoder_Decode_EOF(t *testing.T) {
	var e raft.LogEntry
	dec := raft.NewLogEntryDecoder(bytes.NewReader([]byte{}))
	if err := dec.Decode(&e); err != io.EOF {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the decoder returns an unexpected EOF when reading partial entries.
func TestLogEntryDecoder_Decode_ErrUnexpectedEOF_Type(t *testing.T) {
	for i, tt := range []struct {
		buf []byte
	}{
		{[]byte{0x10}}, // type flag only
		{[]byte{0x10, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0}},             // partial header
		{[]byte{0x10, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}},          // full header, no data
		{[]byte{0x10, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 4, 5}}, // full header, partial data
	} {
		var e raft.LogEntry
		dec := raft.NewLogEntryDecoder(bytes.NewReader(tt.buf))
		if err := dec.Decode(&e); err != io.ErrUnexpectedEOF {
			t.Errorf("%d. unexpected error: %s", i, err)
		}
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

// limitWriter writes up to n bytes and then returns io.ErrShortWrite.
type limitWriter struct {
	buf bytes.Buffer
	n   int
}

// newLimitWriter returns a new instance of limitWriter.
func newLimitWriter(n int) *limitWriter {
	return &limitWriter{n: n}
}

func (w *limitWriter) Write(p []byte) (n int, err error) {
	if len(p) <= w.n {
		_, _ = w.buf.Write(p)
		w.n -= len(p)
		return len(p), nil
	}
	n = w.n
	w.n = 0
	_, _ = w.buf.Write(p[:n])
	return n, io.ErrShortWrite
}

func TestLimitWriter(t *testing.T) {
	w := newLimitWriter(8)
	if n, err := w.Write([]byte("foo")); err != nil {
		t.Fatalf("unexpected error(0): %s", err)
	} else if n != 3 {
		t.Fatalf("unexpected n(0): %d", n)
	}
	if n, err := w.Write([]byte("bazzz")); err != nil {
		t.Fatalf("unexpected error(1): %s", err)
	} else if n != 5 {
		t.Fatalf("unexpected n(1): %d", n)
	}
	if n, err := w.Write([]byte("x")); err != io.ErrShortWrite {
		t.Fatalf("unexpected error(2): %s", err)
	} else if n != 0 {
		t.Fatalf("unexpected n(2): %d", n)
	}
	if w.buf.String() != "foobazzz" {
		t.Fatalf("unexpected buf: %s", w.buf.String())
	}
}
