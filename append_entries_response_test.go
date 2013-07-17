package raft

import (
	"bytes"
	"testing"
)

// Ensure that we can encode and decode append entries responses.
func TestAppendEntriesResponseEncodeDecode(t *testing.T) {
	var b bytes.Buffer
	r0 := newAppendEntriesResponse(1, true, 2, 3)
	if _, err := r0.encode(&b); err != nil {
		t.Fatal("AE response encoding error:", err)
	}

	r1 := &AppendEntriesResponse{}
	if _, err := r1.decode(&b); err != nil {
		t.Fatal("AE response decoding error:", err)
	}
	if r1.Term != 1 || r1.Success != true || r1.Index != 2 || r1.CommitIndex != 3 {
		t.Fatal("Invalid AE response data:", r1.Term, r1.Success, r1.Index, r1.CommitIndex)
	}
}

func BenchmarkAppendEntriesResponseEncoding(b *testing.B) {
	req, tmp := createTestAppendEntriesResponse(2000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		req.encode(&buf)
	}
	b.SetBytes(int64(len(tmp)))
}

func BenchmarkAppendEntriesResponseDecoding(b *testing.B) {
	req, buf := createTestAppendEntriesResponse(2000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.decode(bytes.NewReader(buf))
	}
	b.SetBytes(int64(len(buf)))
}

func createTestAppendEntriesResponse(entryCount int) (*AppendEntriesResponse, []byte) {
	resp := newAppendEntriesResponse(1, true, 1, 1)

	var buf bytes.Buffer
	resp.encode(&buf)

	return resp, buf.Bytes()
}
