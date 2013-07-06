package raft

import (
	"encoding/json"
	"reflect"
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

// Ensure that we can encode a log entry to JSON.
func TestLogEntryMarshal(t *testing.T) {
	e := newLogEntry(nil, 1, 2, &joinCommand{Name: "localhost:1000"})
	if b, err := json.Marshal(e); !(string(b) == `{"command":{"name":"localhost:1000"},"index":1,"name":"test:join","term":2}` && err == nil) {
		t.Fatalf("Unexpected log entry marshalling: %v (%v)", string(b), err)
	}
}

// Ensure that we can decode a log entry from JSON.
func TestLogEntryUnmarshal(t *testing.T) {
	e := &LogEntry{}
	b := []byte(`{"command":{"name":"localhost:1000"},"index":1,"name":"test:join","term":2}`)
	if err := json.Unmarshal(b, e); err != nil {
		t.Fatalf("Log entry unmarshalling error: %v", err)
	}
	if !(e.Index == 1 && e.Term == 2 && reflect.DeepEqual(e.Command, &joinCommand{Name: "localhost:1000"})) {
		t.Fatalf("Log entry unmarshaled incorrectly: %v | %v", e, newLogEntry(nil, 1, 2, &joinCommand{Name: "localhost:1000"}))
	}
}
