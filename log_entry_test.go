package raft

import (
	"bytes"
	"testing"
)

//------------------------------------------------------------------------------
//
// Tests
//
//------------------------------------------------------------------------------

//--------------------------------------
// Encoding
//--------------------------------------

// Ensure that we can encode and decode a log entry.
func TestLogEntryEncodeDecode(t *testing.T) {
	// Create entry.
	e1, err := newLogEntry(nil, 1, 2, &joinCommand{Name: "localhost:1000"})
	if err != nil {
		t.Fatal("Unable to create entry: ", err)
	}

	// Encode the entry to a buffer.
	var buf bytes.Buffer
	if n, err := e1.encode(&buf); n == 0 || err != nil {
		t.Fatal("Unable to encode entry: ", n, err)
	}

	// Decode into a new entry.
	e2 := &LogEntry{}
	if n, err := e2.decode(&buf); n == 0 || err != nil {
		t.Fatal("Unable to decode entry: ", n, err)
	}
	if e2.Index != 1 || e2.Term != 2 {
		t.Fatal("Unexpected log entry encoding:", e2.Index, e2.Term)
	}
	if e2.CommandName != "test:join" || !bytes.Equal(e1.Command, e2.Command) {
		t.Fatal("Unexpected log entry command encoding:", e2.CommandName, len(e1.Command), len(e2.Command))
	}
}
