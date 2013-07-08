package raft

import (
	"fmt"
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
	resp := server.RequestVote(newRequestVoteRequest(1, "foo", 0, 0))
	if resp.Term != 1 || !resp.VoteGranted {
		t.Fatalf("Invalid request vote response: %v/%v", resp.Term, resp.VoteGranted)
	}
}

// // Ensure that a vote request is denied if it comes from an old term.
func TestServerRequestVoteDeniedForStaleTerm(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartLeader()
	server.currentTerm = 2
	defer server.Stop()
	resp := server.RequestVote(newRequestVoteRequest(1, "foo", 0, 0))
	if resp.Term != 2 || resp.VoteGranted {
		t.Fatalf("Invalid request vote response: %v/%v", resp.Term, resp.VoteGranted)
	}
	if server.currentTerm != 2 && server.State() != Follower {
		t.Fatalf("Server did not update term and demote: %v / %v", server.currentTerm, server.State())
	}
}

// // Ensure that a vote request is denied if we've already voted for a different candidate.
func TestServerRequestVoteDeniedIfAlreadyVoted(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartLeader()
	server.currentTerm = 2
	defer server.Stop()
	resp := server.RequestVote(newRequestVoteRequest(2, "foo", 0, 0))
	if resp.Term != 2 || !resp.VoteGranted {
		t.Fatalf("First vote should not have been denied")
	}
	resp = server.RequestVote(newRequestVoteRequest(2, "bar", 0, 0))
	if resp.Term != 2 || resp.VoteGranted {
		t.Fatalf("Second vote should have been denied")
	}
}

// Ensure that a vote request is approved if vote occurs in a new term.
func TestServerRequestVoteApprovedIfAlreadyVotedInOlderTerm(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartLeader()
	server.currentTerm = 2
	defer server.Stop()
	resp := server.RequestVote(newRequestVoteRequest(2, "foo", 0, 0))
	if resp.Term != 2 || !resp.VoteGranted || server.VotedFor() != "foo" {
		t.Fatalf("First vote should not have been denied")
	}
	resp = server.RequestVote(newRequestVoteRequest(3, "bar", 0, 0))

	if resp.Term != 3 || !resp.VoteGranted || server.VotedFor() != "bar" {
		t.Fatalf("Second vote should have been approved")
	}
}

// Ensure that a vote request is denied if the log is out of date.
func TestServerRequestVoteDenyIfCandidateLogIsBehind(t *testing.T) {
	server := newTestServerWithLog("1", &testTransporter{},
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n"+
			`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n"+
			`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}`+"\n")
	server.Initialize()
	server.StartLeader()
	server.currentTerm = 2

	defer server.Stop()

	resp := server.RequestVote(newRequestVoteRequest(2, "foo", 2, 2))
	if resp.Term != 2 || resp.VoteGranted {
		t.Fatalf("Stale index vote should have been denied [%v/%v]", resp.Term, resp.VoteGranted)
	}
	resp = server.RequestVote(newRequestVoteRequest(2, "foo", 3, 1))
	if resp.Term != 2 || resp.VoteGranted {
		t.Fatalf("Stale term vote should have been denied [%v/%v]", resp.Term, resp.VoteGranted)
	}
	resp = server.RequestVote(newRequestVoteRequest(2, "foo", 3, 2))
	if resp.Term != 2 || !resp.VoteGranted {
		t.Fatalf("Matching log vote should have been granted")
	}
	resp = server.RequestVote(newRequestVoteRequest(2, "foo", 4, 3))
	if resp.Term != 2 || !resp.VoteGranted {
		t.Fatalf("Ahead-of-log vote should have been granted")
	}
}

// //--------------------------------------
// // Promotion
// //--------------------------------------

// // Ensure that we can self-promote a server to candidate, obtain votes and become a fearless leader.
func TestServerPromoteSelf(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartFollower()
	defer server.Stop()

	time.Sleep(300 * time.Millisecond)

	if server.State() != Leader {
		t.Fatalf("Server self-promotion failed: %v", server.State())
	}
}

//Ensure that we can promote a server within a cluster to a leader.
func TestServerPromote(t *testing.T) {
	lookup := map[string]*Server{}
	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(server *Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		return lookup[peer.Name()].RequestVote(req)
	}
	transporter.sendAppendEntriesRequestFunc = func(server *Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		return lookup[peer.Name()].AppendEntries(req)
	}
	servers := newTestCluster([]string{"1", "2", "3"}, transporter, lookup)

	servers[0].StartFollower()
	servers[1].StartFollower()
	servers[2].StartFollower()

	time.Sleep(50 * time.Millisecond)

	if servers[0].State() != Leader && servers[1].State() != Leader && servers[2].State() != Leader {
		t.Fatalf("No leader elected: (%s, %s, %s)", servers[0].State(), servers[1].State(), servers[2].State())
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
	entries := []*LogEntry{newLogEntry(nil, 1, 1, &testCommand1{"foo", 10})}
	resp := server.AppendEntries(newAppendEntriesRequest(1, "ldr", 0, 0, entries, 0))
	if resp.Term != 1 || !resp.Success {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}
	if index, term := server.log.commitInfo(); index != 0 || term != 0 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

	// Append multiple entries + commit the last one.
	entries = []*LogEntry{newLogEntry(nil, 2, 1, &testCommand1{"bar", 20}), newLogEntry(nil, 3, 1, &testCommand1{"baz", 30})}
	resp = server.AppendEntries(newAppendEntriesRequest(1, "ldr", 1, 1, entries, 1))
	if resp.Term != 1 || !resp.Success {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}
	if index, term := server.log.commitInfo(); index != 1 || term != 1 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

	// Send zero entries and commit everything.
	resp = server.AppendEntries(newAppendEntriesRequest(2, "ldr", 3, 1, []*LogEntry{}, 3))
	if resp.Term != 2 || !resp.Success {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}
	if index, term := server.log.commitInfo(); index != 3 || term != 1 {
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
	entries := []*LogEntry{newLogEntry(nil, 1, 1, &testCommand1{"foo", 10})}
	resp := server.AppendEntries(newAppendEntriesRequest(1, "ldr", 0, 0, entries, 0))
	if resp.Term != 2 || resp.Success {
		t.Fatalf("AppendEntries should have failed: %v/%v", resp.Term, resp.Success)
	}
	if index, term := server.log.commitInfo(); index != 0 || term != 0 {
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
		newLogEntry(nil, 1, 1, &testCommand1{"foo", 10}),
		newLogEntry(nil, 2, 1, &testCommand1{"foo", 15}),
	}
	resp := server.AppendEntries(newAppendEntriesRequest(1, "ldr", 0, 0, entries, 2))
	if resp.Term != 1 || !resp.Success {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}

	// Append entry again (post-commit).
	entries = []*LogEntry{newLogEntry(nil, 2, 1, &testCommand1{"bar", 20})}
	resp = server.AppendEntries(newAppendEntriesRequest(1, "ldr", 2, 1, entries, 1))
	if resp.Term != 1 || resp.Success {
		t.Fatalf("AppendEntries should have failed: %v/%v", resp.Term, resp.Success)
	}
}

// Ensure that we uncommitted entries are rolled back if new entries overwrite them.
func TestServerAppendEntriesOverwritesUncommittedEntries(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	server.Initialize()
	server.StartLeader()
	defer server.Stop()

	entry1 := newLogEntry(nil, 1, 1, &testCommand1{"foo", 10})
	entry2 := newLogEntry(nil, 2, 1, &testCommand1{"foo", 15})
	entry3 := newLogEntry(nil, 2, 2, &testCommand1{"bar", 20})

	// Append single entry + commit.
	entries := []*LogEntry{entry1, entry2}
	resp := server.AppendEntries(newAppendEntriesRequest(1, "ldr", 0, 0, entries, 1))
	if resp.Term != 1 || !resp.Success || server.log.commitIndex != 1 || !reflect.DeepEqual(server.log.entries, []*LogEntry{entry1, entry2}) {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}

	// Append entry that overwrites the second (uncommitted) entry.
	entries = []*LogEntry{entry3}
	resp = server.AppendEntries(newAppendEntriesRequest(2, "ldr", 1, 1, entries, 2))
	if resp.Term != 2 || !resp.Success || server.log.commitIndex != 2 || !reflect.DeepEqual(server.log.entries, []*LogEntry{entry1, entry3}) {
		t.Fatalf("AppendEntries should have succeeded: %v/%v", resp.Term, resp.Success)
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
	if _, err = server.Do(&testCommand1{"foo", 10}); err != NotLeaderError {
		t.Fatalf("Expected error: %v, got: %v", NotLeaderError, err)
	}
}

//--------------------------------------
// Membership
//--------------------------------------

// Ensure that we can start a single server and append to its log.
func TestServerSingleNode(t *testing.T) {
	server := newTestServer("1", &testTransporter{})
	if server.State() != Stopped {
		t.Fatalf("Unexpected server state: %v", server.State())
	}

	server.Initialize()

	if server.State() != Stopped {
		t.Fatalf("Unexpected server state: %v", server.State())
	}

	server.StartLeader()
	time.Sleep(50 * time.Millisecond)

	// Join the server to itself.
	if _, err := server.Do(&joinCommand{Name: "1"}); err != nil {
		t.Fatalf("Unable to join: %v", err)
	}
	debugln("finish command")

	if server.State() != Leader {
		t.Fatalf("Unexpected server state: %v", server.State())
	}

	server.Stop()

	if server.State() != Stopped {
		t.Fatalf("Unexpected server state: %v", server.State())
	}
}

// Ensure that we can start multiple servers and determine a leader.
func TestServerMultiNode(t *testing.T) {
	// Initialize the servers.
	var mutex sync.RWMutex
	servers := map[string]*Server{}

	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(server *Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		mutex.RLock()
		s := servers[peer.name]
		mutex.RUnlock()
		return s.RequestVote(req)
	}
	transporter.sendAppendEntriesRequestFunc = func(server *Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		mutex.RLock()
		s := servers[peer.name]
		mutex.RUnlock()
		return s.AppendEntries(req)
	}

	disTransporter := &testTransporter{}
	disTransporter.sendVoteRequestFunc = func(server *Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		return nil
	}
	disTransporter.sendAppendEntriesRequestFunc = func(server *Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		return nil
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
			time.Sleep(10 * time.Millisecond)
		}
		if _, err := leader.Do(&joinCommand{Name: name}); err != nil {
			t.Fatalf("Unable to join server[%s]: %v", name, err)
		}

	}
	time.Sleep(100 * time.Millisecond)

	// Check that two peers exist on leader.
	mutex.RLock()
	if leader.MemberCount() != n {
		t.Fatalf("Expected member count to be %v, got %v", n, leader.MemberCount())
	}
	if servers["2"].State() == Leader || servers["3"].State() == Leader {
		t.Fatalf("Expected leader should be 1: 2=%v, 3=%v\n", servers["2"].state, servers["3"].state)
	}
	mutex.RUnlock()

	for i := 0; i < 200000; i++ {
		retry := 0
		fmt.Println("Round ", i)

		num := strconv.Itoa(i%(len(servers)) + 1)
		num_1 := strconv.Itoa((i+3)%(len(servers)) + 1)
		toStop := servers[num]
		toStop_1 := servers[num_1]

		// Stop the first server and wait for a re-election.
		time.Sleep(100 * time.Millisecond)
		debugln("Disconnect ", toStop.Name())
		debugln("disconnect ", num, " ", num_1)
		toStop.SetTransporter(disTransporter)
		toStop_1.SetTransporter(disTransporter)
		time.Sleep(200 * time.Millisecond)
		// Check that either server 2 or 3 is the leader now.
		//mutex.Lock()

		leader := 0

		for key, value := range servers {
			debugln("Play begin")
			if key != num && key != num_1 {
				if value.State() == Leader {
					debugln("Found leader")
					for i := 0; i < 10; i++ {
						debugln("[Test] do ", value.Name())
						if _, err := value.Do(&testCommand2{X: 1}); err != nil {
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
				if key != num && key != num_1 {
					if value.State() == Leader {
						leader++
					}
					debugln(value.Name(), " ", value.currentTerm, " ", value.state)
				}
			}

			if leader > 1 {
				if retry < 300 {
					debugln("retry")
					retry++
					leader = 0
					time.Sleep(100 * time.Millisecond)
					continue
				}
				t.Fatalf("wrong leader number %v", leader)
			}
			if leader == 0 {
				if retry < 300 {
					retry++
					fmt.Println("retry 0")
					leader = 0
					time.Sleep(100 * time.Millisecond)
					continue
				}
				t.Fatalf("wrong leader number %v", leader)
			}
			if leader == 1 {
				break
			}
		}

		//mutex.Unlock()

		toStop.SetTransporter(transporter)
		toStop_1.SetTransporter(transporter)
	}

}
