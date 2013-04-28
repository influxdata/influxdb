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
func TestServerSingleNode(t *testing.T) {
	server := newTestServer("1")
	if server.state != Stopped {
		t.Fatalf("Unexpected server state: %v", server.state)
	}

	// Get the server running.
	if err := server.Start(); err != nil {
		t.Fatalf("Unable to start server: %v", err)
	}
	if server.state != Follower {
		t.Fatalf("Unexpected server state: %v", server.state)
	}

	// Join the server to itself.
	if err := server.Join("1"); err != nil {
		t.Fatalf("Unable to join: %v", err)
	}
	if server.state != Leader {
		t.Fatalf("Unexpected server state: %v", server.state)
	}

	// Stop the server.
	server.Stop()
	if server.state != Stopped {
		t.Fatalf("Unexpected server state: %v", server.state)
	}
}

// Ensure that we can start multiple servers and determine a leader.
/*
func TestServerMultiNode(t *testing.T) {
	warn("== begin multi-node ==")
	
	// Initialize the servers.
	names := []string{"1", "2", "3"}
	servers := map[string]*Server{}
	for _, name := range names {
		server := newTestServer(name)
		server.DoHandler = func(server *Server, peer *Peer, command Command) error {
			return servers[peer.name].Do(command)
		}
		if err := server.Start(); err != nil {
			t.Fatalf("Unable to start server[%s]: %v", name, err)
		}
		if err := server.Join("1"); err != nil {
			t.Fatalf("Unable to join server[%s]: %v", name, err)
		}
		servers[name] = server
	}

	// Check that two peers exist on leader.
	leader := servers["1"]
	if leader.MemberCount() != 3 {
		t.Fatalf("Expected member count to be 2, got %v", leader.MemberCount())
	}

	// Stop the servers.
	for _, server := range servers {
		server.Stop()
	}
	warn("== end multi-node ==")
}
*/