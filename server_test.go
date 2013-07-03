package raft

import (
	"reflect"
	"strconv"
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
	server.Initialize()
	server.StartLeader()
	defer server.Stop()
	resp, err := server.RequestVote(NewRequestVoteRequest(1, "foo", 0, 0))
	if !(resp.Term == 1 && resp.VoteGranted && err == nil) {
		t.Fatalf("Invalid request vote response: %v/%v (%v)", resp.Term, resp.VoteGranted, err)
	}
}

// // Ensure that a vote request is denied if it comes from an old term.
func TestServerRequestVoteDeniedForStaleTerm(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartLeader()
	server.currentTerm = 2
	defer server.Stop()
	resp, err := server.RequestVote(NewRequestVoteRequest(1, "foo", 0, 0))
	if !(resp.Term == 2 && !resp.VoteGranted && err != nil && err.Error() == "raft.Server: Stale term: 1 < 2") {
		t.Fatalf("Invalid request vote response: %v/%v (%v)", resp.Term, resp.VoteGranted, err)
	}
	if server.currentTerm != 2 && server.state != Follower {
		t.Fatalf("Server did not update term and demote: %v / %v", server.currentTerm, server.state)
	}
}

// // Ensure that a vote request is denied if we've already voted for a different candidate.
func TestServerRequestVoteDeniedIfAlreadyVoted(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartLeader()
	server.currentTerm = 2
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

// // Ensure that a vote request is approved if vote occurs in a new term.
func TestServerRequestVoteApprovedIfAlreadyVotedInOlderTerm(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartLeader()
	server.currentTerm = 2
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

// // Ensure that a vote request is denied if the log is out of date.
func TestServerRequestVoteDenyIfCandidateLogIsBehind(t *testing.T) {
	server := newTestServerWithLog("1", &testTransporter{},
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n"+
			`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n"+
			`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}`+"\n")
	server.Initialize()
	server.StartLeader()
	server.currentTerm = 2

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

// //--------------------------------------
// // Promotion
// //--------------------------------------

// // Ensure that we can self-promote a server to candidate, obtain votes and become a fearless leader.
func TestServerPromoteSelf(t *testing.T) {
	debugln("---TestServerPromoteSelf---")
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartFollower()
	defer server.Stop()

	time.Sleep(300 * time.Millisecond)

	if server.state != Leader {
		t.Fatalf("Server self-promotion failed: %v", server.state)
	}
}

//Ensure that we can promote a server within a cluster to a leader.
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

	lookup["1"].state = Follower
	lookup["2"].state = Follower
	lookup["3"].state = Follower

	leader := servers[0]

	leader.StartFollower()

	time.Sleep(200 * time.Millisecond)

	if leader.state != Leader {
		t.Fatalf("Server promotion failed: %v", leader.state)
	}
	for _, server := range servers {
		server.Stop()
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

	lookup["1"].state = Follower
	lookup["2"].state = Follower
	lookup["3"].state = Follower

	leader := servers[0]

	leader.StartFollower()
	time.Sleep(400 * time.Millisecond)

	if lookup["2"].votedFor != "1" {
		t.Fatalf("Unexpected vote for server 2: %v", lookup["2"].votedFor)
	}
	if lookup["3"].votedFor != "1" {
		t.Fatalf("Unexpected vote for server 3: %v", lookup["3"].votedFor)
	}

	for _, server := range servers {
		server.Stop()
	}
}

//--------------------------------------
// Append Entries
//--------------------------------------

// Ensure we can append entries to a server.
func TestServerAppendEntries(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartLeader()
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

//Ensure that entries with stale terms are rejected.
func TestServerAppendEntriesWithStaleTermsAreRejected(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartLeader()
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
	server.Initialize()
	server.StartLeader()
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
	server.Initialize()
	server.StartLeader()
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
	server.Initialize()
	server.StartFollower()
	defer server.Stop()
	var err error
	if _, err = server.Do(&TestCommand1{"foo", 10}); err != NotLeaderError {
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

	server.Initialize()

	if server.state != Stopped {
		t.Fatalf("Unexpected server state: %v", server.state)
	}

	server.StartLeader()
	time.Sleep(200 * time.Millisecond)

	// Join the server to itself.
	if _, err := server.Do(&joinCommand{Name: "1"}); err != nil {
		t.Fatalf("Unable to join: %v", err)
	}
	debugln("finish command")

	if server.state != Leader {
		t.Fatalf("Unexpected server state: %v", server.state)
	}

	server.Stop()

	if server.state != Stopped {
		t.Fatalf("Unexpected server state: %v", server.state)
	}
}

// Ensure that we can start multiple servers and determine a leader.
func TestServerMultiNode(t *testing.T) {
	// Initialize the servers.
	var mutex sync.Mutex
	servers := map[string]*Server{}

	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(server *Server, peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error) {
		s := servers[peer.name]
		resp, err := s.RequestVote(req)
		return resp, err
	}
	transporter.sendAppendEntriesRequestFunc = func(server *Server, peer *Peer, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
		s := servers[peer.name]
		resp, err := s.AppendEntries(req)
		return resp, err
	}

	disTransporter := &testTransporter{}
	disTransporter.sendVoteRequestFunc = func(server *Server, peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error) {
		return nil, nil
	}
	disTransporter.sendAppendEntriesRequestFunc = func(server *Server, peer *Peer, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
		return nil, nil
	}

	var names []string

	n := 5

	// add n servers
	for i := 1; i <= n; i++ {
		names = append(names, strconv.Itoa(i))
	}

	var leader *Server
	for _, name := range names {
		server := newTestServer(name, transporter)
		server.Initialize()

		mutex.Lock()
		servers[name] = server
		mutex.Unlock()

		if name == "1" {
			leader = server
			server.SetHeartbeatTimeout(testHeartbeatTimeout)
			server.StartLeader()
			time.Sleep(100 * time.Millisecond)
		} else {
			server.SetElectionTimeout(testElectionTimeout)
			server.SetHeartbeatTimeout(testHeartbeatTimeout)
			server.StartFollower()
		}
		if _, err := leader.Do(&joinCommand{Name: name}); err != nil {
			t.Fatalf("Unable to join server[%s]: %v", name, err)
		}

	}
	time.Sleep(100 * time.Millisecond)

	// Check that two peers exist on leader.
	mutex.Lock()
	if leader.MemberCount() != n {
		t.Fatalf("Expected member count to be %v, got %v", n, leader.MemberCount())
	}
	if servers["2"].State() == Leader || servers["3"].State() == Leader {
		t.Fatalf("Expected leader should be 1: 2=%v, 3=%v\n", servers["2"].state, servers["3"].state)
	}
	mutex.Unlock()

	for i := 0; i < 200000; i++ {
		i++
		debugln("Round ", i)

		num := strconv.Itoa(i%(len(servers)) + 1)
		toStop := servers[num]

		// Stop the first server and wait for a re-election.
		time.Sleep(100 * time.Millisecond)
		debugln("Disconnect ", toStop.Name())
		toStop.SetTransporter(disTransporter)
		time.Sleep(200 * time.Millisecond)
		// Check that either server 2 or 3 is the leader now.
		//mutex.Lock()

		leader := 0

		for key, value := range servers {
			debugln("Play begin")
			if key != num {
				if value.State() == Leader {
					debugln("Found leader")
					for i := 0; i < 10; i++ {
						debugln("[Test] do ", value.Name())
						if _, err := value.Do(&TestCommand2{X: 1}); err != nil {
							break
						}
						debugln("[Test] Done")
					}
					debugln("Leader is ", value.Name(), " Index ", value.log.commitIndex)
				}
				debugln("Not Found leader")
			}
		}
		for {
			for key, value := range servers {
				if key != num {
					if value.State() == Leader {
						leader++
					}
				}
			}

			if leader > 1 {

				t.Fatalf("wrong leader number %v", leader)
			}
			if leader == 0 {
				leader = 0
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if leader == 1 {
				break
			}
		}

		//mutex.Unlock()

		toStop.SetTransporter(transporter)
	}

}
