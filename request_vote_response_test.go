package raft

import (
	"bytes"
	"testing"
)

// Ensure that we can encode and decode request vote responses.
func TestRequestVoteResponseEncodeDecode(t *testing.T) {
	var b bytes.Buffer
	r0 := newRequestVoteResponse(1, true)
	if _, err := r0.encode(&b); err != nil {
		t.Fatal("RV response encoding error:", err)
	}

	r1 := &RequestVoteResponse{}
	if _, err := r1.decode(&b); err != nil {
		t.Fatal("RV response decoding error:", err)
	}
	if r1.Term != 1 || r1.VoteGranted != true {
		t.Fatal("Invalid RV response data:", r1.Term, r1.VoteGranted)
	}
}
