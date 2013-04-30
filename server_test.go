package raft

import (
	"testing"
)

//------------------------------------------------------------------------------
//
// Tests
//
//------------------------------------------------------------------------------

//--------------------------------------
// Request Vote
//--------------------------------------

// Ensure that we can request a vote from a server that has not voted.
func TestServerRequestVote(t *testing.T) {
	server := newTestServer("1")
	resp := server.RequestVote(NewRequestVoteRequest(1, "foo", 0, 0))
	if !(resp.Term == 1 && resp.VoteGranted) {
		t.Fatalf("Invalid request vote response: %v/%v", resp.Term, resp.VoteGranted)
	}
}

// Ensure that a vote request is denied if it comes from an old term.
func TestServerRequestVoteDeniedForStaleTerm(t *testing.T) {
	server := newTestServer("1")
	server.state = Leader
	server.currentTerm = 2
	resp := server.RequestVote(NewRequestVoteRequest(1, "foo", 0, 0))
	if !(resp.Term == 2 && !resp.VoteGranted) {
		t.Fatalf("Invalid request vote response: %v/%v", resp.Term, resp.VoteGranted)
	}
	if server.currentTerm != 2 && server.state != Follower {
		t.Fatalf("Server did not update term and demote: %v / %v", server.currentTerm, server.state)
	}
}

// Ensure that a vote request is denied if we've already voted for a different candidate.
func TestServerRequestVoteDeniedIfAlreadyVoted(t *testing.T) {
	server := newTestServer("1")
	server.currentTerm = 2
	resp := server.RequestVote(NewRequestVoteRequest(2, "foo", 0, 0))
	if !(resp.Term == 2 && resp.VoteGranted) {
		t.Fatalf("First vote should not have been denied")
	}
	resp = server.RequestVote(NewRequestVoteRequest(2, "bar", 0, 0))
	if !(resp.Term == 2 && !resp.VoteGranted) {
		t.Fatalf("Second vote should have been denied")
	}
}

// Ensure that a vote request is approved if vote occurs in a new term.
func TestServerRequestVoteApprovedIfAlreadyVotedInOlderTerm(t *testing.T) {
	server := newTestServer("1")
	server.currentTerm = 2
	resp := server.RequestVote(NewRequestVoteRequest(2, "foo", 0, 0))
	if !(resp.Term == 2 && resp.VoteGranted && server.votedFor == "foo") {
		t.Fatalf("First vote should not have been denied")
	}
	resp = server.RequestVote(NewRequestVoteRequest(3, "bar", 0, 0))
	if !(resp.Term == 3 && resp.VoteGranted && server.votedFor == "bar") {
		t.Fatalf("Second vote should have been approved")
	}
}

// Ensure that a vote request is denied if the log is out of date.
func TestServerRequestVoteDenyIfCandidateLogIsBehind(t *testing.T) {
	server := newTestServerWithLog("1",
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}` + "\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}` + "\n" +
		`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}` + "\n")
	server.Start()

	resp := server.RequestVote(NewRequestVoteRequest(1, "foo", 2, 2))
	if !(resp.Term == 1 && !resp.VoteGranted) {
		t.Fatalf("Stale index vote should have been denied")
	}
	resp = server.RequestVote(NewRequestVoteRequest(1, "foo", 3, 1))
	if !(resp.Term == 1 && !resp.VoteGranted) {
		t.Fatalf("Stale term vote should have been denied")
	}
	resp = server.RequestVote(NewRequestVoteRequest(1, "foo", 3, 2))
	if !(resp.Term == 1 && resp.VoteGranted) {
		t.Fatalf("Matching log vote should have been granted")
	}
	resp = server.RequestVote(NewRequestVoteRequest(1, "foo", 4, 3))
	if !(resp.Term == 1 && resp.VoteGranted) {
		t.Fatalf("Ahead-of-log vote should have been granted")
	}
}

//--------------------------------------
// Membership
//--------------------------------------

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
