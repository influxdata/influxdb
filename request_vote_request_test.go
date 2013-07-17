package raft

import (
	"bytes"
	"testing"
)

// Ensure that we can encode and decode request vote requests.
func TestRequestVoteRequestEncodeDecode(t *testing.T) {
	var b bytes.Buffer
	r0 := newRequestVoteRequest(1, "ldr", 2, 3)
	if _, err := r0.encode(&b); err != nil {
		t.Fatal("RV request encoding error:", err)
	}

	r1 := &RequestVoteRequest{}
	if _, err := r1.decode(&b); err != nil {
		t.Fatal("RV request decoding error:", err)
	}
	if r1.Term != 1 || r1.CandidateName != "ldr" || r1.LastLogIndex != 2 || r1.LastLogTerm != 3 {
		t.Fatal("Invalid RV request data:", r1.Term, r1.CandidateName, r1.LastLogIndex, r1.LastLogTerm)
	}
}
