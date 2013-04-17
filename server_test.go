package raft

import (
	"testing"
)

//------------------------------------------------------------------------------
//
// Tests
//
//------------------------------------------------------------------------------

// Ensure that we can start a single server and append to its log.
func TestServer(t *testing.T) {
	server := newTestServer("1")
	if err := server.Start(); err != nil {
		t.Fatalf("Unable to start server: %v", err)
	}
	server.Stop()
}
