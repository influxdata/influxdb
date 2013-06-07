package raft

import (
	"reflect"
	"sync"
	"testing"
	"time"
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
	server := newTestServer("1", &testTransporter{})
	server.Start()
	defer server.Stop()
	resp, err := server.RequestVote(NewRequestVoteRequest(1, "foo", 0, 0))
	if !(resp.Term == 1 && resp.VoteGranted && err == nil) {
		t.Fatalf("Invalid request vote response: %v/%v (%v)", resp.Term, resp.VoteGranted, err)
	}
}

// Ensure that a vote request is denied if it comes from an old term.
func TestServerRequestVoteDeniedForStaleTerm(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.state = Leader
	server.currentTerm = 2
	server.Start()
	defer server.Stop()
	resp, err := server.RequestVote(NewRequestVoteRequest(1, "foo", 0, 0))
	if !(resp.Term == 2 && !resp.VoteGranted && err != nil && err.Error() == "raft.Server: Stale term: 1 < 2") {
		t.Fatalf("Invalid request vote response: %v/%v (%v)", resp.Term, resp.VoteGranted, err)
	}
	if server.currentTerm != 2 && server.state != Follower {
		t.Fatalf("Server did not update term and demote: %v / %v", server.currentTerm, server.state)
	}
}

// Ensure that a vote request is denied if we've already voted for a different candidate.
func TestServerRequestVoteDeniedIfAlreadyVoted(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.currentTerm = 2
	server.Start()
	defer server.Stop()
	resp, err := server.RequestVote(NewRequestVoteRequest(2, "foo", 0, 0))
	if !(resp.Term == 2 && resp.VoteGranted && err == nil) {
		t.Fatalf("First vote should not have been denied (%v)", err)
	}
	resp, err = server.RequestVote(NewRequestVoteRequest(2, "bar", 0, 0))
	if !(resp.Term == 2 && !resp.VoteGranted && err != nil && err.Error() == "raft.Server: Already voted for foo") {
		t.Fatalf("Second vote should have been denied (%v)", err)
	}
}

// Ensure that a vote request is approved if vote occurs in a new term.
func TestServerRequestVoteApprovedIfAlreadyVotedInOlderTerm(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.currentTerm = 2
	server.Start()
	defer server.Stop()
	resp, err := server.RequestVote(NewRequestVoteRequest(2, "foo", 0, 0))
	if !(resp.Term == 2 && resp.VoteGranted && server.VotedFor() == "foo" && err == nil) {
		t.Fatalf("First vote should not have been denied (%v)", err)
	}
	resp, err = server.RequestVote(NewRequestVoteRequest(3, "bar", 0, 0))
	if !(resp.Term == 3 && resp.VoteGranted && server.VotedFor() == "bar" && err == nil) {
		t.Fatalf("Second vote should have been approved (%v)", err)
	}
}

// Ensure that a vote request is denied if the log is out of date.
func TestServerRequestVoteDenyIfCandidateLogIsBehind(t *testing.T) {
	server := newTestServerWithLog("1", &testTransporter{},
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n"+
			`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n"+
			`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}`+"\n")
	if err := server.Start(); err != nil {
		t.Fatalf("Unable to start server: %v", err)
	}
	defer server.Stop()

	resp, err := server.RequestVote(NewRequestVoteRequest(2, "foo", 2, 2))
	if !(resp.Term == 2 && !resp.VoteGranted && err != nil && err.Error() == "raft.Server: Out-of-date log: [3/2] > [2/2]") {
		t.Fatalf("Stale index vote should have been denied [%v/%v] (%v)", resp.Term, resp.VoteGranted, err)
	}
	resp, err = server.RequestVote(NewRequestVoteRequest(2, "foo", 3, 1))
	if !(resp.Term == 2 && !resp.VoteGranted && err != nil && err.Error() == "raft.Server: Out-of-date log: [3/2] > [3/1]") {
		t.Fatalf("Stale term vote should have been denied [%v/%v] (%v)", resp.Term, resp.VoteGranted, err)
	}
	resp, err = server.RequestVote(NewRequestVoteRequest(2, "foo", 3, 2))
	if !(resp.Term == 2 && resp.VoteGranted && err == nil) {
		t.Fatalf("Matching log vote should have been granted (%v)", err)
	}
	resp, err = server.RequestVote(NewRequestVoteRequest(2, "foo", 4, 3))
	if !(resp.Term == 2 && resp.VoteGranted && err == nil) {
		t.Fatalf("Ahead-of-log vote should have been granted (%v)", err)
	}
}

//--------------------------------------
// Promotion
//--------------------------------------

// Ensure that we can self-promote a server to candidate, obtain votes and become a fearless leader.
func TestServerPromoteSelf(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Start()
	defer server.Stop()
	if success, err := server.promote(); !(success && err == nil && server.state == Leader) {
		t.Fatalf("Server self-promotion failed: %v (%v)", server.state, err)
	}
}

// Ensure that we can promote a server within a cluster to a leader.
func TestServerPromote(t *testing.T) {
	lookup := map[string]*Server{}
	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(server *Server, peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error) {
		return lookup[peer.Name()].RequestVote(req)
	}
	transporter.sendAppendEntriesRequestFunc = func(server *Server, peer *Peer, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
		return lookup[peer.Name()].AppendEntries(req)
	}
	servers := newTestCluster([]string{"1", "2", "3"}, transporter, lookup)
	for _, server := range servers {
		defer server.Stop()
	}
	leader := servers[0]
	if success, err := leader.promote(); !(success && err == nil && leader.state == Leader) {
		t.Fatalf("Server promotion in cluster failed: %v (%v)", leader.state, err)
	}
}

// Ensure that a server will restart election if not enough votes are obtained before timeout.
func TestServerPromoteDoubleElection(t *testing.T) {
	lookup := map[string]*Server{}
	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(server *Server, peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error) {
		resp, err := lookup[peer.Name()].RequestVote(req)
		return resp, err
	}
	transporter.sendAppendEntriesRequestFunc = func(server *Server, peer *Peer, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
		resp, err := lookup[peer.Name()].AppendEntries(req)
		return resp, err
	}
	servers := newTestCluster([]string{"1", "2", "3"}, transporter, lookup)
	lookup["2"].currentTerm, lookup["2"].votedFor = 1, "2"
	lookup["3"].currentTerm, lookup["3"].votedFor = 1, "3"
	lookup["2"].electionTimer.Stop()
	lookup["3"].electionTimer.Stop()
	for _, server := range servers {
		defer server.Stop()
	}
	leader := servers[0]
	if success, err := leader.promote(); !(success && err == nil && leader.state == Leader && leader.currentTerm == 2) {
		t.Fatalf("Server promotion in cluster failed: %v (%v)", leader.state, err)
	}
	time.Sleep(50 * time.Millisecond)
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
	server := newTestServer("1", &testTransporter{})
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
}

// Ensure that entries with stale terms are rejected.
func TestServerAppendEntriesWithStaleTermsAreRejected(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
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
	server := newTestServer("1", &testTransporter{})
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
	server := newTestServer("1", &testTransporter{})
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
// Command Execution
//--------------------------------------

// Ensure that a follower cannot execute a command.
func TestServerDenyCommandExecutionWhenFollower(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Start()
	defer server.Stop()
	if err := server.Do(&TestCommand1{"foo", 10}); err != NotLeaderError {
		t.Fatalf("Expected error: %v, got: %v", NotLeaderError, err)
	}
}

//--------------------------------------
// Membership
//--------------------------------------

// Ensure that we can start a single server and append to its log.
func TestServerSingleNode(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	if server.state != Stopped {
		t.Fatalf("Unexpected server state: %v", server.state)
	}

	// Get the server running.
	if err := server.Start(); err != nil {
		t.Fatalf("Unable to start server: %v", err)
	}
	defer server.Stop()
	if server.state != Follower {
		t.Fatalf("Unexpected server state: %v", server.state)
	}

	// Join the server to itself.
	server.Initialize()
	if err := server.Do(&joinCommand{Name:"1"}); err != nil {
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
func TestServerMultiNode(t *testing.T) {
	// Initialize the servers.
	var mutex sync.Mutex
	names := []string{"1", "2", "3"}
	servers := map[string]*Server{}
	for _, server := range servers {
		defer server.Stop()
	}
	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(server *Server, peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error) {
		mutex.Lock()
		s := servers[peer.name]
		mutex.Unlock()
		resp, err := s.RequestVote(req)
		return resp, err
	}
	transporter.sendAppendEntriesRequestFunc = func(server *Server, peer *Peer, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
		mutex.Lock()
		s := servers[peer.name]
		mutex.Unlock()
		resp, err := s.AppendEntries(req)
		return resp, err
	}

	var leader *Server
	for _, name := range names {
		server := newTestServer(name, transporter)
		server.SetElectionTimeout(testElectionTimeout)
		server.SetHeartbeatTimeout(testHeartbeatTimeout)
		if err := server.Start(); err != nil {
			t.Fatalf("Unable to start server[%s]: %v", name, err)
		}
		if name == "1" {
			leader = server
			if err := server.Initialize(); err != nil {
				t.Fatalf("Unable to initialize server[%s]: %v", name, err)
			}
		}
		if err := leader.Do(&joinCommand{Name:name}); err != nil {
			t.Fatalf("Unable to join server[%s]: %v", name, err)
		}

		mutex.Lock()
		servers[name] = server
		mutex.Unlock()
	}
	time.Sleep(100 * time.Millisecond)

	// Check that two peers exist on leader.
	mutex.Lock()
	if leader.MemberCount() != 3 {
		t.Fatalf("Expected member count to be 3, got %v", leader.MemberCount())
	}
	mutex.Unlock()

	// Stop the first server and wait for a re-election.
	time.Sleep(100 * time.Millisecond)
	leader.Stop()
	time.Sleep(100 * time.Millisecond)
	// Check that either server 2 or 3 is the leader now.
	mutex.Lock()
	if servers["2"].State() != Leader && servers["3"].State() != Leader {
		t.Fatalf("Expected leader re-election: 2=%v, 3=%v\n", servers["2"].state, servers["3"].state)
	}
	mutex.Unlock()
}


