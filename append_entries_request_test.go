package raft

import (
	"bytes"
	"testing"
)

// Ensure that we can encode and decode append entries requests.
func TestAppendEntriesRequestEncodeDecode(t *testing.T) {
	var b bytes.Buffer
	e0, _ := newLogEntry(nil, 1, 2, &joinCommand{Name: "localhost:1000"})
	e1, _ := newLogEntry(nil, 1, 2, &joinCommand{Name: "localhost:1001"})
	r0 := newAppendEntriesRequest(1, 2, 3, 4, "ldr", []*LogEntry{e0, e1})
	if _, err := r0.encode(&b); err != nil {
		t.Fatal("AE request encoding error:", err)
	}

	r1 := &AppendEntriesRequest{}
	if _, err := r1.decode(&b); err != nil {
		t.Fatal("AE request decoding error:", err)
	}
	if r1.Term != 1 || r1.PrevLogIndex != 2 || r1.PrevLogTerm != 3 || r1.CommitIndex != 4 || r1.LeaderName != "ldr" || len(r1.Entries) != 2 {
		t.Fatal("Invalid AE data:", r1.Term, r1.PrevLogIndex, r1.PrevLogTerm, r1.CommitIndex, r1.LeaderName, len(r1.Entries))
	}
}

func BenchmarkAppendEntriesRequestEncoding(b *testing.B) {
	req, tmp := createTestAppendEntriesRequest(2000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		req.encode(&buf)
	}
	b.SetBytes(int64(len(tmp)))
}

func BenchmarkAppendEntriesRequestDecoding(b *testing.B) {
	req, buf := createTestAppendEntriesRequest(2000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.decode(bytes.NewReader(buf))
	}
	b.SetBytes(int64(len(buf)))
}

func createTestAppendEntriesRequest(entryCount int) (*AppendEntriesRequest, []byte) {
	entries := make([]*LogEntry, 0)
	for i := 0; i < entryCount; i++ {
		command := &joinCommand{Name: "localhost:1000"}
		entry, _ := newLogEntry(nil, 1, 2, command)
		entries = append(entries, entry)
	}
	req := newAppendEntriesRequest(1, 1, 1, 1, "leader", entries)

	var buf bytes.Buffer
	req.encode(&buf)

	return req, buf.Bytes()
}
