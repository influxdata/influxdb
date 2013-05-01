package raft

import (
	"reflect"
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
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n"+
			`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n"+
			`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}`+"\n")
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
// Promotion
//--------------------------------------

// Ensure that we can self-promote a server to candidate, obtain votes and become a fearless leader.
func TestServerPromoteSelf(t *testing.T) {
	server := newTestServer("1")
	server.Start()
	if success, err := server.promote(); !(success && err == nil && server.state == Leader) {
		t.Fatalf("Server self-promotion failed: %v (%v)", server.state, err)
	}
}

// Ensure that we can promote a server within a cluster to a leader.
func TestServerPromote(t *testing.T) {
	servers, lookup := newTestCluster([]string{"1", "2", "3"})
	servers.SetRequestVoteHandler(func(server *Server, peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error) {
		return lookup[peer.Name()].RequestVote(req), nil
	})
	leader := servers[0]
	if success, err := leader.promote(); !(success && err == nil && leader.state == Leader) {
		t.Fatalf("Server promotion in cluster failed: %v (%v)", leader.state, err)
	}
}

// Ensure that a server will restart election if not enough votes are obtained before timeout.
func TestServerPromoteDoubleElection(t *testing.T) {
	servers, lookup := newTestCluster([]string{"1", "2", "3"})
	lookup["2"].currentTerm, lookup["2"].votedFor = 1, "2"
	lookup["3"].currentTerm, lookup["3"].votedFor = 1, "3"
	servers.SetRequestVoteHandler(func(server *Server, peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error) {
		return lookup[peer.Name()].RequestVote(req), nil
	})
	leader := servers[0]
	if success, err := leader.promote(); !(success && err == nil && leader.state == Leader && leader.currentTerm == 2) {
		t.Fatalf("Server promotion in cluster failed: %v (%v)", leader.state, err)
	}
	if lookup["2"].votedFor != "1" {
		t.Fatalf("Unexpected vote for server 2: %v", lookup["2"].votedFor)
	}
	if lookup["3"].votedFor != "1" {
		t.Fatalf("Unexpected vote for server 3: %v", lookup["3"].votedFor)
	}
}

//--------------------------------------
// Append Entries
//--------------------------------------

// Ensure we can append entries to a server.
func TestServerAppendEntries(t *testing.T) {
	server := newTestServer("1")
	server.Start()
	defer server.Stop()

	// Append single entry.
	entries := []*LogEntry{NewLogEntry(nil, 1, 1, &TestCommand1{"foo", 10})}
	resp, err := server.AppendEntries(NewAppendEntriesRequest(1, "ldr", 0, 0, entries, 0))
	if !(resp.Term == 1 && resp.Success && err == nil) {
		t.Fatalf("AppendEntries failed: %v/%v : %v", resp.Term, resp.Success, err)
	}
	if index, term := server.log.CommitInfo(); !(index == 0 && term == 0) {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

	// Append multiple entries + commit the last one.
	entries = []*LogEntry{NewLogEntry(nil, 2, 1, &TestCommand1{"bar", 20}), NewLogEntry(nil, 3, 1, &TestCommand1{"baz", 30})}
	resp, err = server.AppendEntries(NewAppendEntriesRequest(1, "ldr", 1, 1, entries, 1))
	if !(resp.Term == 1 && resp.Success && err == nil) {
		t.Fatalf("AppendEntries failed: %v/%v : %v", resp.Term, resp.Success, err)
	}
	if index, term := server.log.CommitInfo(); !(index == 1 && term == 1) {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

	// Send zero entries and commit everything.
	resp, err = server.AppendEntries(NewAppendEntriesRequest(2, "ldr", 3, 1, []*LogEntry{}, 3))
	if !(resp.Term == 2 && resp.Success && err == nil) {
		t.Fatalf("AppendEntries failed: %v/%v : %v", resp.Term, resp.Success, err)
	}
	if index, term := server.log.CommitInfo(); !(index == 3 && term == 1) {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

	server.Stop()
}

// Ensure that entries with stale terms are rejected.
func TestServerAppendEntriesWithStaleTermsAreRejected(t *testing.T) {
	server := newTestServer("1")
	server.Start()
	defer server.Stop()
	server.currentTerm = 2

	// Append single entry.
	entries := []*LogEntry{NewLogEntry(nil, 1, 1, &TestCommand1{"foo", 10})}
	resp, err := server.AppendEntries(NewAppendEntriesRequest(1, "ldr", 0, 0, entries, 0))
	if !(resp.Term == 2 && !resp.Success && err != nil && err.Error() == "raft.Server: Stale request term") {
		t.Fatalf("AppendEntries should have failed: %v/%v : %v", resp.Term, resp.Success, err)
	}
	if index, term := server.log.CommitInfo(); !(index == 0 && term == 0) {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}
}

// Ensure that we reject entries if the commit log is different.
func TestServerAppendEntriesRejectedIfAlreadyCommitted(t *testing.T) {
	server := newTestServer("1")
	server.Start()
	defer server.Stop()

	// Append single entry + commit.
	entries := []*LogEntry{
		NewLogEntry(nil, 1, 1, &TestCommand1{"foo", 10}),
		NewLogEntry(nil, 2, 1, &TestCommand1{"foo", 15}),
	}
	resp, err := server.AppendEntries(NewAppendEntriesRequest(1, "ldr", 0, 0, entries, 2))
	if !(resp.Term == 1 && resp.Success && err == nil) {
		t.Fatalf("AppendEntries failed: %v/%v : %v", resp.Term, resp.Success, err)
	}

	// Append entry again (post-commit).
	entries = []*LogEntry{NewLogEntry(nil, 2, 1, &TestCommand1{"bar", 20})}
	resp, err = server.AppendEntries(NewAppendEntriesRequest(1, "ldr", 2, 1, entries, 1))
	if !(resp.Term == 1 && !resp.Success && err != nil && err.Error() == "raft.Log: Cannot append entry with earlier index in the same term (1:2 <= 1:2)") {
		t.Fatalf("AppendEntries should have failed: %v/%v : %v", resp.Term, resp.Success, err)
	}
}

// Ensure that we uncommitted entries are rolled back if new entries overwrite them.
func TestServerAppendEntriesOverwritesUncommittedEntries(t *testing.T) {
	server := newTestServer("1")
	server.Start()
	defer server.Stop()

	entry1 := NewLogEntry(nil, 1, 1, &TestCommand1{"foo", 10})
	entry2 := NewLogEntry(nil, 2, 1, &TestCommand1{"foo", 15})
	entry3 := NewLogEntry(nil, 2, 2, &TestCommand1{"bar", 20})

	// Append single entry + commit.
	entries := []*LogEntry{entry1, entry2}
	resp, err := server.AppendEntries(NewAppendEntriesRequest(1, "ldr", 0, 0, entries, 1))
	if !(resp.Term == 1 && resp.Success && err == nil && server.log.CommitIndex() == 1 && reflect.DeepEqual(server.log.entries, []*LogEntry{entry1, entry2})) {
		t.Fatalf("AppendEntries failed: %v/%v : %v", resp.Term, resp.Success, err)
	}

	// Append entry that overwrites the second (uncommitted) entry.
	entries = []*LogEntry{entry3}
	resp, err = server.AppendEntries(NewAppendEntriesRequest(2, "ldr", 1, 1, entries, 2))
	if !(resp.Term == 2 && resp.Success && err == nil && server.log.CommitIndex() == 2 && reflect.DeepEqual(server.log.entries, []*LogEntry{entry1, entry3})) {
		t.Fatalf("AppendEntries should have succeeded: %v/%v : %v", resp.Term, resp.Success, err)
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
