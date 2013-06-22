package raft

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	Stopped   = "stopped"
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
)

const (
	DefaultHeartbeatTimeout = 50 * time.Millisecond
	DefaultElectionTimeout  = 150 * time.Millisecond
)

//------------------------------------------------------------------------------
//
// Errors
//
//------------------------------------------------------------------------------

var NotLeaderError = errors.New("raft.Server: Not current leader")
var DuplicatePeerError = errors.New("raft.Server: Duplicate peer")

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A server is involved in the consensus protocol and can act as a follower,
// candidate or a leader.
type Server struct {
	name             string
	path             string
	state            string
	transporter      Transporter
	context          interface{}
	currentTerm      uint64
	votedFor         string
	log              *Log
	leader           string
	peers            map[string]*Peer
	mutex            sync.Mutex
	electionTimer    *Timer
	heartbeatTimeout time.Duration
	currentSnapshot  *Snapshot
	lastSnapshot     *Snapshot
	stateMachine     StateMachine
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new server with a log at the given path.
func NewServer(name string, path string, transporter Transporter, context interface{}) (*Server, error) {
	if name == "" {
		return nil, errors.New("raft.Server: Name cannot be blank")
	}
	if transporter == nil {
		panic("raft: Transporter required")
	}

	s := &Server{
		name:             name,
		path:             path,
		transporter:      transporter,
		context:          context,
		state:            Stopped,
		peers:            make(map[string]*Peer),
		log:              NewLog(),
		electionTimer:    NewTimer(DefaultElectionTimeout, DefaultElectionTimeout*2),
		heartbeatTimeout: DefaultHeartbeatTimeout,
	}

	// Setup apply function.
	s.log.ApplyFunc = func(c Command) error {
		err := c.Apply(s)
		return err
	}

	return s, nil
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

//--------------------------------------
// General
//--------------------------------------

// Retrieves the name of the server.
func (s *Server) Name() string {
	return s.name
}

// Retrieves the storage path for the server.
func (s *Server) Path() string {
	return s.path
}

func (s *Server) Leader() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.leader
}

// Retrieves the object that transports requests.
func (s *Server) Transporter() Transporter {
	return s.transporter
}

// Retrieves the context passed into the constructor.
func (s *Server) Context() interface{} {
	return s.context
}

// Retrieves the log path for the server.
func (s *Server) LogPath() string {
	return fmt.Sprintf("%s/log", s.path)
}

// Retrieves the current state of the server.
func (s *Server) State() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.state
}

// Retrieves the name of the candidate this server voted for in this term.
func (s *Server) VotedFor() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.votedFor
}

// Retrieves whether the server's log has no entries.
func (s *Server) IsLogEmpty() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.log.IsEmpty()
}

// A list of all the log entries. This should only be used for debugging purposes.
func (s *Server) LogEntries() []*LogEntry {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.log != nil {
		return s.log.entries
	}
	return nil
}

// A reference to the command name of the last entry.
func (s *Server) LastCommandName() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.log != nil {
		return s.log.LastCommandName()
	}
	return ""
}

//--------------------------------------
// Membership
//--------------------------------------

// Retrieves the number of member servers in the consensus.
func (s *Server) MemberCount() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.peers) + 1
}

// Retrieves the number of servers required to make a quorum.
func (s *Server) QuorumSize() int {
	return (s.MemberCount() / 2) + 1
}

//--------------------------------------
// Election timeout
//--------------------------------------

// Retrieves the election timeout.
func (s *Server) ElectionTimeout() time.Duration {
	return s.electionTimer.MinDuration()
}

// Sets the election timeout.
func (s *Server) SetElectionTimeout(duration time.Duration) {
	s.electionTimer.SetMinDuration(duration)
	s.electionTimer.SetMaxDuration(duration * 2)
}

//--------------------------------------
// Heartbeat timeout
//--------------------------------------

// Retrieves the heartbeat timeout.
func (s *Server) HeartbeatTimeout() time.Duration {
	return s.heartbeatTimeout
}

// Sets the heartbeat timeout.
func (s *Server) SetHeartbeatTimeout(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.heartbeatTimeout = duration
	for _, peer := range s.peers {
		peer.SetHeartbeatTimeout(duration)
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// State
//--------------------------------------

// Starts the server with a log at the given path.
func (s *Server) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Exit if the server is already running.
	if s.Running() {
		return errors.New("raft.Server: Server already running")
	}

	// create snapshot dir if not exist
	os.Mkdir(s.path+"/snapshot", 0700)

	// ## open recovery from the newest snapShot
	//s.LoadSnapshot()

	// Initialize the log and load it up.
	if err := s.log.Open(s.LogPath()); err != nil {
		s.unload()
		return fmt.Errorf("raft.Server: %v", err)
	}

	// Update the term to the last term in the log.
	s.currentTerm = s.log.CurrentTerm()

	// Update the state.
	s.state = Follower
	for _, peer := range s.peers {
		peer.pause()
	}

	// Start the election timeout.
	c := make(chan bool)
	go s.electionTimeoutFunc(c)
	<-c

	return nil
}

// Shuts down the server.
func (s *Server) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.unload()
}

// Unloads the server.
func (s *Server) unload() {
	// Kill the election timer.
	if s.electionTimer != nil {
		s.electionTimer.Stop()
		s.electionTimer = nil
	}

	// Remove peers.
	for _, peer := range s.peers {
		peer.stop()
	}
	s.peers = make(map[string]*Peer)

	// Close the log.
	if s.log != nil {
		s.log.Close()
		s.log = nil
	}

	s.state = Stopped
}

// Checks if the server is currently running.
func (s *Server) Running() bool {
	return s.state != Stopped
}

//--------------------------------------
// Initialization
//--------------------------------------

// Initializes the server to become leader of a new cluster. This function
// will fail if there is an existing log or the server is already a member in
// an existing cluster.
func (s *Server) Initialize() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Exit if the server is not running.
	if !s.Running() {
		return errors.New("raft.Server: Cannot initialize while stopped")
	} else if s.MemberCount() > 1 {
		return errors.New("raft.Server: Cannot initialize; already in membership")
	}

	// Promote to leader.
	s.currentTerm++
	s.state = Leader
	s.leader = s.name
	s.electionTimer.Pause()

	return nil
}

//--------------------------------------
// Commands
//--------------------------------------

// Attempts to execute a command and replicate it. The function will return
// when the command has been successfully committed or an error has occurred.
func (s *Server) Do(command Command) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	err := s.do(command)
	return err
}

// This function is the low-level interface to execute commands. This function
// does not obtain a lock so one must be obtained before executing.
func (s *Server) do(command Command) error {
	if s.state != Leader {
		return NotLeaderError
	}

	// Capture the term that this command is executing within.
	currentTerm := s.currentTerm

	// // TEMP to solve the issue 18
	// for _, peer := range s.peers {
	// 	peer.pause()
	// }

	// Add a new entry to the log.
	entry := s.log.CreateEntry(s.currentTerm, command)
	if err := s.log.AppendEntry(entry); err != nil {
		return err
	}

	// Flush the entries to the peers.
	c := make(chan bool, len(s.peers))
	for _, _peer := range s.peers {
		peer := _peer
		go func() {

			term, success, err := peer.flush(true)

			// Demote if we encounter a higher term.
			if err != nil {

				return
			} else if term > currentTerm {
				s.mutex.Lock()
				s.setCurrentTerm(term)

				if s.electionTimer != nil {
					s.electionTimer.Reset()
				}
				s.mutex.Unlock()

				return
			}

			// If we successfully replicated the log then send a success to the channel.
			if success {
				c <- true
			}
		}()
	}

	// Wait for a quorum to confirm and commit entry.
	responseCount := 1
	committed := false
loop:
	for {
		// If we received enough votes then stop waiting for more votes.
		if responseCount >= s.QuorumSize() {
			committed = true
			// for _, peer := range s.peers {
			// 	peer.resume()
			// }
			break
		}

		// Collect votes from peers.
		select {
		case <-c:
			// Exit if our term has changed.
			if s.currentTerm > currentTerm {
				return fmt.Errorf("raft.Server: Higher term discovered, stepping down: (%v > %v)", s.currentTerm, currentTerm)
			}
			responseCount++
		case <-afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2):
			// for _, peer := range s.peers {
			// 	peer.resume()
			// }
			break loop
		}
	}

	// Commit to log and flush to peers again.
	if committed {
		if err := s.log.SetCommitIndex(entry.Index); err != nil {
			return err
		}
		return s.log.GetEntryError(entry)
	}

	// TODO: This will be removed after the timeout above is changed to a
	// demotion callback.
	return fmt.Errorf("raft: Unable to commit entry: %d", entry.Index)
}

// Appends a log entry from the leader to this server.
func (s *Server) AppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// If the server is stopped then reject it.
	if !s.Running() {
		return NewAppendEntriesResponse(s.currentTerm, false, 0), fmt.Errorf("raft.Server: Server stopped")
	}

	// If the request is coming from an old term then reject it.
	if req.Term < s.currentTerm {
		return NewAppendEntriesResponse(s.currentTerm, false, s.log.CommitIndex()), fmt.Errorf("raft.Server: Stale request term")
	}
	s.setCurrentTerm(req.Term)

	// Update the current leader.
	s.leader = req.LeaderName

	// Reset election timeout.
	if s.electionTimer != nil {
		s.electionTimer.Reset()
	}

	// Reject if log doesn't contain a matching previous entry.
	if err := s.log.Truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		return NewAppendEntriesResponse(s.currentTerm, false, s.log.CommitIndex()), err
	}

	// Append entries to the log.
	if err := s.log.AppendEntries(req.Entries); err != nil {
		return NewAppendEntriesResponse(s.currentTerm, false, s.log.CommitIndex()), err
	}

	// Commit up to the commit index.
	if err := s.log.SetCommitIndex(req.CommitIndex); err != nil {
		return NewAppendEntriesResponse(s.currentTerm, false, s.log.CommitIndex()), err
	}

	return NewAppendEntriesResponse(s.currentTerm, true, s.log.CommitIndex()), nil
}

// Creates an AppendEntries request.
func (s *Server) createAppendEntriesRequest(prevLogIndex uint64) *AppendEntriesRequest {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.createInternalAppendEntriesRequest(prevLogIndex)
}

// Creates an AppendEntries request without a lock.
func (s *Server) createInternalAppendEntriesRequest(prevLogIndex uint64) *AppendEntriesRequest {
	if s.log == nil {
		return nil
	}
	entries, prevLogTerm := s.log.GetEntriesAfter(prevLogIndex)
	req := NewAppendEntriesRequest(s.currentTerm, s.name, prevLogIndex, prevLogTerm, entries, s.log.CommitIndex())
	return req
}

//--------------------------------------
// Promotion
//--------------------------------------

// Promotes the server to a candidate and then requests votes from peers. If
// enough votes are received then the server becomes the leader. If this
// server is elected then true is returned. If another server is elected then
// false is returned.
func (s *Server) promote() (bool, error) {

	for {
		// Start a new election.
		term, lastLogIndex, lastLogTerm, err := s.promoteToCandidate()
		if err != nil {
			return false, err
		}

		// Request votes from each of our peers.
		c := make(chan *RequestVoteResponse, len(s.peers))
		for _, _peer := range s.peers {
			peer := _peer
			go func() {
				req := NewRequestVoteRequest(term, s.name, lastLogIndex, lastLogTerm)
				req.peer = peer
				if resp, _ := s.transporter.SendVoteRequest(s, peer, req); resp != nil {
					resp.peer = peer
					c <- resp
				}
			}()
		}

		// Collect votes until we have a quorum.
		votes := map[string]bool{}
		elected := false
	loop:
		for {
			// Add up all our votes.
			votesGranted := 1
			for _, value := range votes {
				if value {
					votesGranted++
				}
			}
			// If we received enough votes then stop waiting for more votes.
			if votesGranted >= s.QuorumSize() {
				elected = true
				break
			}

			// Collect votes from peers.
			select {
			case resp := <-c:
				if resp != nil {
					// Step down if we discover a higher term.
					if resp.Term > term {
						s.mutex.Lock()
						s.setCurrentTerm(term)
						if s.electionTimer != nil {
							s.electionTimer.Reset()
						}
						s.mutex.Unlock()
						return false, fmt.Errorf("raft.Server: Higher term discovered, stepping down: (%v > %v)", resp.Term, term)
					}
					votes[resp.peer.Name()] = resp.VoteGranted
				}
			case <-afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2):
				break loop
			}
		}

		// If we received enough votes then promote to leader and stop this election.
		if elected && s.promoteToLeader(term, lastLogIndex, lastLogTerm) {
			break
		}

		// If we are no longer in the same term then another server must have been elected.
		s.mutex.Lock()
		if s.currentTerm != term {
			s.mutex.Unlock()
			return false, fmt.Errorf("raft.Server: Term changed during election, stepping down: (%v > %v)", s.currentTerm, term)
		}
		s.mutex.Unlock()
	}

	return true, nil
}

// Promotes the server to a candidate and increases the election term. The
// term and log state are returned for use in the RPCs.
func (s *Server) promoteToCandidate() (uint64, uint64, uint64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Ignore promotion if the server is not a follower.
	if s.state != Follower && s.state != Candidate {
		return 0, 0, 0, fmt.Errorf("raft: Invalid promotion state: %s", s.state)
	}

	// Move server to become a candidate, increase our term & vote for ourself.
	s.state = Candidate
	s.currentTerm++
	s.votedFor = s.name
	s.leader = ""
	// Pause the election timer while we're a candidate.
	s.electionTimer.Pause()
	// Return server state so we can check for it during leader promotion.
	lastLogIndex, lastLogTerm := s.log.CommitInfo()
	return s.currentTerm, lastLogIndex, lastLogTerm, nil
}

// Promotes the server from a candidate to a leader. This can only occur if
// the server is in the state that it assumed when the candidate election
// began. This is because another server may have won the election and caused
// the state to change.
func (s *Server) promoteToLeader(term uint64, lastLogIndex uint64, lastLogTerm uint64) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Ignore promotion if we are not a candidate.
	if s.state != Candidate {
		return false
	}

	// Disallow promotion if the term or log does not match what we currently have.
	logIndex, logTerm := s.log.CommitInfo()
	if s.currentTerm != term || logIndex != lastLogIndex || logTerm != lastLogTerm {
		return false
	}

	// Move server to become a leader and begin peer heartbeats.
	s.state = Leader
	s.leader = s.name
	for _, peer := range s.peers {
		peer.resume()
	}

	return true
}

//--------------------------------------
// Request Vote
//--------------------------------------

// Requests a vote from a server. A vote can be obtained if the vote's term is
// at the server's current term and the server has not made a vote yet. A vote
// can also be obtained if the term is greater than the server's current term.
func (s *Server) RequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// Fail if the server is not running.
	if !s.Running() {
		return NewRequestVoteResponse(s.currentTerm, false), fmt.Errorf("raft.Server: Server is stopped")
	}

	// If the request is coming from an old term then reject it.
	if req.Term < s.currentTerm {
		return NewRequestVoteResponse(s.currentTerm, false), fmt.Errorf("raft.Server: Stale term: %v < %v", req.Term, s.currentTerm)
	}
	s.setCurrentTerm(req.Term)

	// If we've already voted for a different candidate then don't vote for this candidate.
	if s.votedFor != "" && s.votedFor != req.CandidateName {
		return NewRequestVoteResponse(s.currentTerm, false), fmt.Errorf("raft.Server: Already voted for %v", s.votedFor)
	}

	// If the candidate's log is not at least as up-to-date as our committed log then don't vote.
	lastCommitIndex, lastCommitTerm := s.log.CommitInfo()
	if lastCommitIndex > req.LastLogIndex || lastCommitTerm > req.LastLogTerm {
		return NewRequestVoteResponse(s.currentTerm, false), fmt.Errorf("raft.Server: Out-of-date log: [%v/%v] > [%v/%v]", lastCommitIndex, lastCommitTerm, req.LastLogIndex, req.LastLogTerm)
	}

	// If we made it this far then cast a vote and reset our election time out.
	s.votedFor = req.CandidateName
	if s.electionTimer != nil {
		s.electionTimer.Reset()
	}
	return NewRequestVoteResponse(s.currentTerm, true), nil
}

// Updates the current term on the server if the term is greater than the 
// server's current term. When the term is changed then the server's vote is
// cleared and its state is changed to be a follower.
func (s *Server) setCurrentTerm(term uint64) {
	if term > s.currentTerm {
		s.currentTerm = term
		s.votedFor = ""
		s.state = Follower
		for _, peer := range s.peers {
			peer.pause()
		}
	}
}

// Listens to the election timeout and kicks off a new election.
func (s *Server) electionTimeoutFunc(startChannel chan bool) {
	startChannel <- true
	for {
		// Grab the current timer channel.
		s.mutex.Lock()
		var c chan time.Time
		if s.electionTimer != nil {
			c = s.electionTimer.C()
		}
		s.mutex.Unlock()

		// If the channel or timer are gone then exit.
		if c == nil {
			break
		}

		// If an election times out then promote this server. If the channel
		// closes then that means the server has stopped so kill the function.
		if _, ok := <-c; ok {
			s.promote()
		} else {
			break
		}
	}
}

//--------------------------------------
// Membership
//--------------------------------------

// Adds a peer to the server. This should be called by a system's join command
// within the context so that it is within the context of the server lock.
func (s *Server) AddPeer(name string) error {
	// Do not allow peers to be added twice.
	if s.peers[name] != nil {
		return DuplicatePeerError
	}

	// Only add the peer if it doesn't have the same name.
	if s.name != name {
		peer := NewPeer(s, name, s.heartbeatTimeout)
		if s.state == Leader {
			peer.resume()
		}
		s.peers[peer.name] = peer

	}
	return nil
}

// Removes a peer from the server. This should be called by a system's join command
// within the context so that it is within the context of the server lock.
func (s *Server) RemovePeer(name string) error {
	// Ignore removal of the server itself.
	if s.name == name {
		return nil
	}
	// Return error if peer doesn't exist.
	peer := s.peers[name]
	if peer == nil {
		return fmt.Errorf("raft: Peer not found: %s", name)
	}

	// Flush entries to the peer first.
	if s.state == Leader {
		if _, _, err := peer.flush(true); err != nil {
			warn("raft: Unable to notify peer of removal: %v", err)
		}
	}

	// Stop peer and remove it.
	peer.stop()
	delete(s.peers, name)

	return nil
}

//--------------------------------------
// Log compaction
//--------------------------------------

// Creates a snapshot request.
func (s *Server) createSnapshotRequest() *SnapshotRequest {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return NewSnapshotRequest(s.name, s.lastSnapshot)
}

// The background snapshot function
func (s *Server) Snapshot() {
	for {
		s.takeSnapshot()

		// TODO: change this... to something reasonable
		time.Sleep(5000 * time.Millisecond)
	}
}

func (s *Server) takeSnapshot() error {
	//TODO put a snapshot mutex
	if s.currentSnapshot != nil {
		return errors.New("handling snapshot")
	}

	lastIndex, lastTerm := s.log.CommitInfo()

	if lastIndex == 0 || lastTerm == 0 {
		return errors.New("No logs")
	}

	path := s.SnapshotPath(lastIndex, lastTerm)

	state, err := s.stateMachine.Save()

	if err != nil {
		return err
	}

	s.currentSnapshot = &Snapshot{lastIndex, lastTerm, state, path}

	s.saveSnapshot()

	s.log.Compact(lastIndex, lastTerm)

	return nil
}

// Retrieves the log path for the server.
func (s *Server) saveSnapshot() error {

	if s.currentSnapshot == nil {
		return errors.New("no snapshot to save")
	}

	err := s.currentSnapshot.Save()

	if err != nil {
		return err
	}

	tmp := s.lastSnapshot
	s.lastSnapshot = s.currentSnapshot

	// delete the previous snapshot if there is any change
	if tmp != nil && !(tmp.lastIndex == s.lastSnapshot.lastIndex && tmp.lastTerm == s.lastSnapshot.lastTerm) {
		tmp.Remove()
	}
	s.currentSnapshot = nil
	return nil
}

// Retrieves the log path for the server.
func (s *Server) SnapshotPath(lastIndex uint64, lastTerm uint64) string {
	return path.Join(s.path, "snapshot", fmt.Sprintf("%v_%v.ss", lastTerm, lastIndex))
}

func (s *Server) SnapshotRecovery(req *SnapshotRequest) (*SnapshotResponse, error) {
	//
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.stateMachine.Recovery(req.State)

	//update term and index
	s.currentTerm = req.LastTerm
	s.log.UpdateCommitIndex(req.LastIndex)
	snapshotPath := s.SnapshotPath(req.LastIndex, req.LastTerm)
	s.currentSnapshot = &Snapshot{req.LastIndex, req.LastTerm, req.State, snapshotPath}
	s.saveSnapshot()
	s.log.Compact(req.LastIndex, req.LastTerm)

	return NewSnapshotResponse(req.LastTerm, true, req.LastIndex), nil

}

// Load a snapshot at restart
func (s *Server) LoadSnapshot() error {
	dir, err := os.OpenFile(path.Join(s.path, "snapshot"), os.O_RDONLY, 0)
	if err != nil {
		dir.Close()
		panic(err)
	}

	filenames, err := dir.Readdirnames(-1)

	if err != nil {
		dir.Close()
		panic(err)
	}

	dir.Close()
	if len(filenames) == 0 {
		return errors.New("no snapshot")
	}

	// not sure how many snapshot we should keep
	sort.Strings(filenames)
	snapshotPath := path.Join(s.path, "snapshot", filenames[len(filenames)-1])

	// should not file
	file, err := os.OpenFile(snapshotPath, os.O_RDONLY, 0)
	defer file.Close()
	if err != nil {
		panic(err)
	}

	// TODO check checksum first 
	// TODO recovery state machine

	var state []byte
	var checksum, lastIndex, lastTerm uint64

	n, err := fmt.Fscanf(file, "%08x\n%v\n%v", &checksum, &lastIndex, &lastTerm)

	if err != nil {
		return err
	}

	if n != 3 {
		return errors.New("Bad snapshot file")
	}

	state, _ = ioutil.ReadAll(file)

	if err != nil {
		return err
	}

	s.lastSnapshot = &Snapshot{lastIndex, lastTerm, state, snapshotPath}
	err = s.stateMachine.Recovery(state)

	s.log.SetStartTerm(lastTerm)
	s.log.SetStartIndex(lastIndex)
	s.log.UpdateCommitIndex(lastIndex)

	return err
}
