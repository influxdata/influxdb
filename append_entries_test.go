package raft

import (
	"bytes"
	"encoding/json"
	"testing"
)

func BenchmarkAppendEntriesEncoding(b *testing.B) {
	req, tmp := createTestAppendEntriesRequest(2000)
    for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
        json.NewEncoder(&buf).Encode(req)
    }
	b.SetBytes(int64(len(tmp)))
}

func BenchmarkAppendEntriesDecoding(b *testing.B) {
	req, buf := createTestAppendEntriesRequest(2000)
    for i := 0; i < b.N; i++ {
        json.NewDecoder(bytes.NewReader(buf)).Decode(req)
    }
	b.SetBytes(int64(len(buf)))
}

func createTestAppendEntriesRequest(entryCount int) (*AppendEntriesRequest, []byte) {
	entries := make([]*LogEntry, 0)
	for i := 0; i < entryCount; i++ {
		entries = append(entries, newLogEntry(nil, 1, 2, &joinCommand{Name: "localhost:1000"}))
	}
	req := newAppendEntriesRequest(1, "leader", 1, 1, entries, 1)
	buf, _ := json.Marshal(req)
	
	return req, buf
}

