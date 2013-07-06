package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
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
	name        string
	path        string
	state       string
	transporter Transporter
	context     interface{}
	currentTerm uint64

	votedFor   string
	log        *Log
	leader     string
	peers      map[string]*Peer
	mutex      sync.Mutex
	stateMutex sync.Mutex

	electionTimer    *Timer
	heartbeatTimeout time.Duration
	response         chan FlushResponse
	stepDown         chan uint64
	stop             chan bool

	currentSnapshot *Snapshot
	lastSnapshot    *Snapshot
	stateMachine    StateMachine
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new server with a log at the given path.
func NewServer(name string, path string, transporter Transporter, stateMachine StateMachine, context interface{}) (*Server, error) {
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
		stateMachine:     stateMachine,
		context:          context,
		state:            Stopped,
		peers:            make(map[string]*Peer),
		log:              NewLog(),
		stepDown:         make(chan uint64, 1),
		stop:             make(chan bool),
		electionTimer:    NewTimer(DefaultElectionTimeout, DefaultElectionTimeout*2),
		heartbeatTimeout: DefaultHeartbeatTimeout,
	}

	// Setup apply function.
	s.log.ApplyFunc = func(c Command) (interface{}, error) {
		result, err := c.Apply(s)
		return result, err
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

// Retrieves a copy of the peer data.
func (s *Server) Peers() map[string]*Peer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	peers := make(map[string]*Peer)
	for name, peer := range s.peers {
		peers[name] = peer.clone()
	}
	return peers
}

// Retrieves the object that transports requests.
func (s *Server) Transporter() Transporter {
	return s.transporter
}

func (s *Server) SetTransporter(t Transporter) {
	s.transporter = t
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
	return s.state
}

// Retrieves the current term of the server.
func (s *Server) Term() uint64 {
	return s.currentTerm
}

// Retrieves the current committed index of the server.
func (s *Server) CommittedIndex() uint64 {

	return s.log.CommitIndex()

}

// Retrieves the name of the candidate this server voted for in this term.
func (s *Server) VotedFor() string {
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

// Get the state of the server for debugging
func (s *Server) GetState() string {
	return fmt.Sprintf("Name: %s, State: %s, Term: %v, Index: %v ", s.name, s.state, s.currentTerm, s.CommittedIndex())
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

// Start heartbeat when the server promote to leader
func (s *Server) StartHeartbeatTimeout() {
	for _, peer := range s.peers {
		peer.StartHeartbeat()
	}
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
// Initialization
//--------------------------------------

// Starts the server with a log at the given path.
func (s *Server) Initialize() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Exit if the server is already running.
	if s.Running() {
		return errors.New("raft.Server: Server already running")
	}

	// Initialize response channel
	s.response = make(chan FlushResponse, 128)

	// Create snapshot directory if not exist
	os.Mkdir(s.path+"/snapshot", 0700)

	// Initialize the log and load it up.
	if err := s.log.Open(s.LogPath()); err != nil {
		debugln("log error")
		s.unload()
		return fmt.Errorf("raft.Server: %v", err)
	}

	// Update the term to the last term in the log.
	s.currentTerm = s.log.CurrentTerm()

	return nil
}

//                          timeout
//                          ______
//                         |      |
//                         |      |
//                         v      |     recv majority votes
//  --------    timeout    -----------                        -----------
// |Follower| ----------> | Candidate |--------------------> |  Leader   |
//  --------               -----------                        -----------
//     ^              stepDown |                       stepDown      |
//     |_______________________|____________________________________ |
//
// The main Loop for the server
func (s *Server) StartServerLoop(role string) {
	stop := false
	leader := false

	defer debugln("server stopped!")

	for {
		switch role {

		case Follower:
			stop = s.startFollowerLoop()

			if stop {
				return
			}

			role = Candidate

		case Candidate:
			debugln(s.GetState() + "start Candiate")
			stop, leader = s.startCandidateLoop()

			s.votedFor = ""

			if stop {
				return
			}

			if leader {
				role = Leader

			} else {

				role = Follower
			}
			debugln(s.GetState() + "stop Candiate")

		case Leader:
			debugln(s.GetState() + "start Leader")
			stop = s.startLeaderLoop()
			if stop {
				return
			}

			role = Follower
			debugln(s.GetState() + "stop Leader")
		}
	}
}

// Start the sever as a follower
func (s *Server) StartFollower() {
	go s.StartServerLoop(Follower)

}

// Start the sever as a leader
func (s *Server) StartLeader() {
	s.state = Candidate
	s.currentTerm++
	go s.StartServerLoop(Leader)
}

// Shuts down the server.
func (s *Server) Stop() {
	s.mutex.Lock()
	if s.state == Follower {
		s.electionTimer.Stop()
	} else {
		s.mutex.Unlock()
		s.stop <- true
	}
	s.unload()
}

// Unloads the server.
func (s *Server) unload() {
	// Kill the election timer.
	s.state = Stopped

	// wait for all previous flush ends
	time.Sleep(100 * time.Millisecond)

	// Close the log.
	if s.log != nil {
		// still some concurrency issue with stop
		// need lock
		s.log.Close()
	}

}

// Checks if the server is currently running.
func (s *Server) Running() bool {
	return s.state != Stopped
}

// Respond to RPCs from candidates and leaders.
// Convert to candidate if election timeout elapses without
// either:
// 	1.Receiving valid AppendEntries RPC, or
//	2.Granting vote to candidate
func (s *Server) startFollowerLoop() (stop bool) {
	s.state = Follower

	// (1) Timeout: promote and return
	// (2) Stopped: due to receive heartbeat, continue
	for {
		if s.State() == Stopped {
			return true
		}

		if s.electionTimer.Start() {
			return false

		} else {
			s.electionTimer.Ready()
			continue
		}
	}
}

// Increment currentTerm, vote for self
// Reset election timeout
// Send RequestVote RPCs to all other servers, wait for either:
// Votes received from majority of servers: become leader
// AppendEntries RPC received from new leader: step
// down
// Election timeout elapses without election resolution:
// increment term, start new election
// Discover higher term: step down

func (s *Server) startCandidateLoop() (stop bool, leader bool) {

	// the server must be a follower
	if s.state != Follower && s.state != Stopped {
		panic("startCandidateLoop")
	}

	s.state = Candidate
	s.leader = ""
	s.votedFor = s.Name()

	lastLogIndex, lastLogTerm := s.log.LastInfo()

	for {

		// increase term
		s.currentTerm++

		// Request votes from each of our peers.
		c := make(chan *RequestVoteResponse, len(s.peers))
		req := NewRequestVoteRequest(s.currentTerm, s.name, lastLogIndex, lastLogTerm)

		for _, peer := range s.peers {
			go peer.sendVoteRequest(req, c)
		}

		// collectVotes
		elected, timeout, stop := s.startCandidateSelect(c)

		// If we received enough votes then promote to leader and stop this election.
		if elected {
			return false, true
		}

		if timeout {
			debugln(s.Name(), " election timeout, restart")
			// restart promotion
			continue
		}

		if stop {

			return true, false
		}

		return false, false
	}
}

// Initialize nextIndex for each to last log index + 1
// Send initial empty AppendEntries RPCs (heartbeat) to each
// follower; repeat during idle periods to prevent election
// timeouts (§5.2)
// Accept commands from clients, append new entries to local
// log (§5.3)
// Whenever last log index ! nextIndex for a follower, send
// AppendEntries RPC with log entries starting at nextIndex,
// update nextIndex if successful (§5.3)
// If AppendEntries fails because of log inconsistency,
// decrement nextIndex and retry (§5.3)
// Mark entries committed if stored on a majority of servers
// and some entry from current term is stored on a majority of
// servers. Apply newly committed entries to state machine.
// Step down if currentTerm changes (§5.5)
func (s *Server) startLeaderLoop() bool {

	// when the server goes into this loop,
	// the leader may have been stepped down to follower!

	// we cannot assume the the server is a candidate when
	// get into this func

	// The request vote func may let it step down

	// That happens when we receive the majority votes, but
	// another candidate start a new term and has not vote for us
	// after it send vote request, the leader will stepdown before
	// it enter this func

	// Move server to become a leader and begin peer heartbeats.
	s.stateMutex.Lock()

	if s.state == Candidate {
		s.state = Leader
		s.leader = s.name
	} else {

		s.stateMutex.Unlock()
		return false
	}

	s.stateMutex.Unlock()

	logIndex, _ := s.log.LastInfo()

	// after here we let the leader stepdown in the startLeaderSelect loop

	// Update the peers prevLogIndex to leader's lastLogIndex
	// Start heartbeat
	for _, peer := range s.peers {

		debugln("[Leader] Set ", peer.Name(), "Prev to", logIndex)

		peer.prevLogIndex = logIndex
		peer.heartbeatTimer.Ready()
		peer.StartHeartbeat()
	}

	// Begin to collect response from followers
	stop := s.startLeaderSelect()

	{
		for _, peer := range s.peers {
			peer.stop()
		}
	}

	return stop
}

// Votes received from majority of servers: become leader
// Election timeout elapses without election resolution:
// Discover higher term: step down
func (s *Server) startCandidateSelect(c chan *RequestVoteResponse) (bool, bool, bool) {

	// Collect votes until we have a quorum.
	votesGranted := 1

	debugln(s.GetState() + "start Select")
	defer debugln(s.GetState() + "end Select")

	for {

		// If we received enough votes then stop waiting for more votes.
		if votesGranted >= s.QuorumSize() {
			return true, false, false
		}

		// Collect votes from peers.
		select {
		case resp := <-c:
			debugln(s.GetState() + "select recv vote")
			if resp != nil {
				if resp.VoteGranted == true {
					votesGranted++

				} else if resp.Term > s.currentTerm {
					s.stateMutex.Lock()

					// go from internal path
					// we may need to eat the stepdown
					select {
					case <-s.stepDown:

					default:

					}

					s.state = Follower
					s.currentTerm = resp.Term
					debugln(s.GetState() + "select step down")
					s.stateMutex.Unlock()
					return false, false, false
				}
			}

		case <-s.stepDown:
			debugln(s.GetState() + "select step down")
			return false, false, false

		// TODO: do we calculate the overall timeout? or timeout for each vote?
		// Some issue here
		case <-afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2):
			debugln(s.GetState() + "select timeout")
			return false, true, false

		case <-s.stop:
			debugln(s.GetState() + "select stop")
			return false, false, true
		}

	}
}

// Collect response from followers. If more than the
// majority of the followers append a log entry, the
// leader will commit the log entry
func (s *Server) startLeaderSelect() bool {
	count := 1

	for {
		var response FlushResponse

		select {
		case response = <-s.response:

			// count for success response from peers
			if response.success && response.peer != nil {
				count++
			}

		case <-s.stepDown:
			// stepdown to follower

			return false

		case <-s.stop:
			return true
		}

		if response.peer != nil {
			debugln("[CommitCenter] Receive response from ", response.peer.Name(), response.success)
		}

		// At least one entry from the leader's current term must also be stored on
		// a majority of servers
		if count >= s.QuorumSize() {
			// Determine the committed index that a majority has.
			var indices []uint64
			indices = append(indices, s.log.CurrentIndex())
			for _, peer := range s.peers {
				indices = append(indices, peer.prevLogIndex)
			}
			sort.Sort(Uint64Slice(indices))

			// We can commit upto the index which the mojarity
			// of the members have appended.
			commitIndex := indices[s.QuorumSize()-1]
			committedIndex := s.log.CommitIndex()

			if commitIndex > committedIndex {
				debugln(indices)
				debugln(s.GetState(), "[CommitCenter] Going to Commit ", commitIndex)
				s.log.SetCommitIndex(commitIndex)
				debugln("[CommitCenter] Commit ", commitIndex)

				for i := committedIndex; i < commitIndex; i++ {
					select {
					case s.log.entries[i-s.log.startIndex].commit <- true:
						debugln("notify")
						continue

					// we have a buffered commit channel, it should return immediately
					// if we are the leader when the log received
					default:
						debugln("Cannot send commit nofication, log from previous leader")
					}
				}

			}
		}

	}

}

//--------------------------------------
// Commands
//--------------------------------------

// Attempts to execute a command and replicate it. The function will return
// when the command has been successfully committed or an error has occurred.

func (s *Server) Do(command Command) (interface{}, error) {
	// race here
	// chance to append entry when we are not leader
	// after the check, the leader may stepdown
	// but log appended with the newest term
	// which means the command from this follower can be commited
	// which will cause a panic

	s.stateMutex.Lock()
	if s.state != Leader {

		s.stateMutex.Unlock()

		return nil, NotLeaderError
	}

	// we get the term of the server
	// when we are sure the server is leader
	term := s.currentTerm

	s.stateMutex.Unlock()

	entry := s.log.CreateEntry(term, command)

	if err := s.log.AppendEntry(entry); err != nil {
		return nil, err
	}

	s.response <- FlushResponse{term, true, nil, nil}

	// to speed up the response time
	// TODO: think about this carefully
	// fire will speed up response time
	// but will reduce through output

	// for _, peer := range s.peers {
	// 	peer.heartbeatTimer.Fire()
	// }

	debugln("[Do] join!")

	// timeout here
	select {
	case <-entry.commit:
		debugln("[Do] finish!")
		result := entry.result
		entry.result = nil
		return result, nil
	case <-time.After(time.Second):
		debugln("[Do] fail!")
		return nil, errors.New("Command commit fails")
	}
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

	debugln("Peer ", s.Name(), "received heartbeat from ", req.LeaderName,
		" ", req.Term, " ", s.currentTerm, " ", time.Now())

	s.setCurrentTerm(req.Term)

	// Update the current leader.
	s.leader = req.LeaderName

	// Reset election timeout.
	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	// Reject if log doesn't contain a matching previous entry.
	if err := s.log.Truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		return NewAppendEntriesResponse(s.currentTerm, false, s.log.CommitIndex()), err
	}

	debugln("Peer ", s.Name(), "after truncate ")

	// Append entries to the log.
	if err := s.log.AppendEntries(req.Entries); err != nil {
		return NewAppendEntriesResponse(s.currentTerm, false, s.log.CommitIndex()), err
	}

	debugln("Peer ", s.Name(), "commit index ", req.CommitIndex, " from ",
		req.LeaderName)
	// Commit up to the commit index.
	if err := s.log.SetCommitIndex(req.CommitIndex); err != nil {
		return NewAppendEntriesResponse(s.currentTerm, false, s.log.CommitIndex()), err
	}

	debugln("Peer ", s.Name(), "after commit ")

	debugln("Peer ", s.Name(), "reply heartbeat from ", req.LeaderName,
		" ", req.Term, " ", s.currentTerm, " ", time.Now())
	return NewAppendEntriesResponse(s.currentTerm, true, s.log.CommitIndex()), nil
}

// Creates an AppendEntries request. Can return a nil request object if the
// index doesn't exist because of a snapshot.
func (s *Server) createAppendEntriesRequest(prevLogIndex uint64) *AppendEntriesRequest {
	if s.log == nil {
		return nil
	}
	entries, prevLogTerm := s.log.GetEntriesAfter(prevLogIndex)
	if entries != nil {
		return NewAppendEntriesRequest(s.currentTerm, s.name, prevLogIndex, prevLogTerm, entries, s.log.CommitIndex())
	} else {
		return nil
	}
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

	debugln("Peer ", s.Name(), "receive vote request from ", req.CandidateName)
	//debugln("[RequestVote] got the lock")
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
		debugln("already vote for ", s.votedFor, " false to ", req.CandidateName)
		return NewRequestVoteResponse(s.currentTerm, false), fmt.Errorf("raft.Server: Already voted for %v", s.votedFor)
	}

	// If the candidate's log is not at least as up-to-date as
	// our last log then don't vote.
	lastIndex, lastTerm := s.log.LastInfo()
	if lastIndex > req.LastLogIndex || lastTerm > req.LastLogTerm {
		debugln("my log is more up-to-date")
		return NewRequestVoteResponse(s.currentTerm, false), fmt.Errorf("raft.Server: Out-of-date log: [%v/%v] > [%v/%v]", lastIndex, lastTerm, req.LastLogIndex, req.LastLogTerm)
	}

	// If we made it this far then cast a vote and reset our election time out.
	s.votedFor = req.CandidateName

	debugln(s.Name(), "Vote for ", req.CandidateName, "at term", req.Term)

	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	return NewRequestVoteResponse(s.currentTerm, true), nil
}

// Updates the current term on the server if the term is greater than the
// server's current term. When the term is changed then the server's vote is
// cleared and its state is changed to be a follower.
func (s *Server) setCurrentTerm(term uint64) {
	if term > s.currentTerm {
		s.votedFor = ""

		s.stateMutex.Lock()
		if s.state == Leader || s.state == Candidate {
			debugln(s.Name(), " should step down to a follower from ", s.state)

			s.state = Follower

			select {
			case s.stepDown <- term:

			default:
				panic("cannot stepdown")
			}
			debugln(s.Name(), " step down to a follower from ", s.state)
			s.currentTerm = term
			s.stateMutex.Unlock()
			return
		}

		s.stateMutex.Unlock()
		// update term after stop all the peer
		s.currentTerm = term
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
		//debugln("Add peer ", name)
		peer := NewPeer(s, name, s.heartbeatTimeout)
		if s.state == Leader {
			peer.StartHeartbeat()
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
		if _, _, err := peer.flush(); err != nil {
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
		// TODO: change this... to something reasonable
		time.Sleep(60 * time.Second)

		s.takeSnapshot()
	}
}

func (s *Server) takeSnapshot() error {
	//TODO put a snapshot mutex
	debugln("take Snapshot")
	if s.currentSnapshot != nil {
		return errors.New("handling snapshot")
	}

	lastIndex, lastTerm := s.log.CommitInfo()

	if lastIndex == 0 || lastTerm == 0 {
		return errors.New("No logs")
	}

	path := s.SnapshotPath(lastIndex, lastTerm)

	var state []byte
	var err error

	if s.stateMachine != nil {
		state, err = s.stateMachine.Save()

		if err != nil {
			return err
		}

	} else {
		state = []byte{0}
	}

	var peerNames []string

	for _, peer := range s.peers {
		peerNames = append(peerNames, peer.Name())
	}
	peerNames = append(peerNames, s.Name())

	s.currentSnapshot = &Snapshot{lastIndex, lastTerm, peerNames, state, path}

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
	if tmp != nil && !(tmp.LastIndex == s.lastSnapshot.LastIndex && tmp.LastTerm == s.lastSnapshot.LastTerm) {
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

	//recovery the cluster configuration
	for _, peerName := range req.Peers {
		s.AddPeer(peerName)
	}

	//update term and index
	s.currentTerm = req.LastTerm

	s.log.UpdateCommitIndex(req.LastIndex)

	snapshotPath := s.SnapshotPath(req.LastIndex, req.LastTerm)

	s.currentSnapshot = &Snapshot{req.LastIndex, req.LastTerm, req.Peers, req.State, snapshotPath}

	s.saveSnapshot()

	s.log.Compact(req.LastIndex, req.LastTerm)

	return NewSnapshotResponse(req.LastTerm, true, req.LastIndex), nil

}

// Load a snapshot at restart
func (s *Server) LoadSnapshot() error {
	dir, err := os.OpenFile(path.Join(s.path, "snapshot"), os.O_RDONLY, 0)
	if err != nil {

		return err
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

	// should not fail
	file, err := os.OpenFile(snapshotPath, os.O_RDONLY, 0)
	defer file.Close()
	if err != nil {
		panic(err)
	}

	// TODO check checksum first

	var snapshotBytes []byte
	var checksum uint32

	n, err := fmt.Fscanf(file, "%08x\n", &checksum)

	if err != nil {
		return err
	}

	if n != 1 {
		return errors.New("Bad snapshot file")
	}

	snapshotBytes, _ = ioutil.ReadAll(file)
	debugln(string(snapshotBytes))

	// Generate checksum.
	byteChecksum := crc32.ChecksumIEEE(snapshotBytes)

	if uint32(checksum) != byteChecksum {
		debugln(checksum, " ", byteChecksum)
		return errors.New("bad snapshot file")
	}

	err = json.Unmarshal(snapshotBytes, &s.lastSnapshot)

	if err != nil {
		debugln("unmarshal error: ", err)
		return err
	}

	err = s.stateMachine.Recovery(s.lastSnapshot.State)

	if err != nil {
		debugln("recovery error: ", err)
		return err
	}

	for _, peerName := range s.lastSnapshot.Peers {
		s.AddPeer(peerName)
	}

	s.log.SetStartTerm(s.lastSnapshot.LastTerm)
	s.log.SetStartIndex(s.lastSnapshot.LastIndex)
	s.log.UpdateCommitIndex(s.lastSnapshot.LastIndex)

	return err
}
