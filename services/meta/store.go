package meta

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/raft"
)

type store struct {
	id uint64 // local node id

	mu      sync.RWMutex
	closing chan struct{}

	config      *Config
	data        *Data
	raftState   *raftState
	dataChanged chan struct{}
	ready       chan struct{}
	addr        string
	raftln      net.Listener
	path        string
	opened      bool
	peers       []string
	logger      *log.Logger

	// Authentication cache.
	authCache map[string]authUser
}

type authUser struct {
	salt []byte
	hash []byte
}

func newStore(c *Config) *store {
	s := store{
		data: &Data{
			Term:            1,
			Index:           1,
			ClusterID:       1,
			Nodes:           []NodeInfo{},
			Databases:       []DatabaseInfo{},
			Users:           []UserInfo{},
			MaxNodeID:       1,
			MaxShardGroupID: 1,
			MaxShardID:      1,
		},
		ready:       make(chan struct{}),
		closing:     make(chan struct{}),
		dataChanged: make(chan struct{}),
		addr:        c.RaftBindAddress,
		path:        c.Dir,
		config:      c,
	}
	if c.LoggingEnabled {
		s.logger = log.New(os.Stderr, "[metastore] ", log.LstdFlags)
	} else {
		s.logger = log.New(ioutil.Discard, "", 0)
	}

	return &s
}

// open opens and initializes the raft store.
func (s *store) open() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.raftln = ln
	s.addr = ln.Addr().String()

	s.logger.Printf("Using data dir: %v", s.path)

	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Check if store has already been opened.
		if s.opened {
			return ErrStoreOpen
		}
		s.opened = true

		// load our raft peers
		if err := s.loadPeers(); err != nil {
			return err
		}

		// Create the root directory if it doesn't already exist.
		if err := os.MkdirAll(s.path, 0777); err != nil {
			return fmt.Errorf("mkdir all: %s", err)
		}

		// Open the raft store.
		println("s.openRaft start")
		if err := s.openRaft(); err != nil {
			return fmt.Errorf("raft: %s", err)
		}
		println("s.openRaft end")

		// Initialize the store, if necessary.
		println("s.raftState.initialize start")
		if err := s.raftState.initialize(); err != nil {
			return fmt.Errorf("initialize raft: %s", err)
		}
		println("s.raftState.initialize end")

		// Load existing ID, if exists.
		println("s.readID start")
		if err := s.readID(); err != nil {
			return fmt.Errorf("read id: %s", err)
		}
		println("s.readID end")

		return nil
	}(); err != nil {
		println("error ***************")
		return err
	}

	// Join an existing cluster if we needed
	println("s.joinCluster start")
	if err := s.joinCluster(); err != nil {
		return fmt.Errorf("join: %v", err)
	}
	println("s.joinCluster end")

	// If the ID doesn't exist then create a new node.
	if s.id == 0 {
		println("ID doesn't exist, creating new node")
		go s.raftState.initialize()
	} else {
		// TODO: enable node info sync
		// all this does is update the raft peers with the new hostname of this node if it changed
		// based on the ID of this node

		// go s.syncNodeInfo()
		close(s.ready)
	}

	// Wait for a leader to be elected so we know the raft log is loaded
	// and up to date
	println("<-s.ready start")
	//<-s.ready
	println("<-s.ready end")
	if err := s.waitForLeader(0); err != nil {
		return err
	}

	return nil
}

// loadPeers sets the appropriate peers from our persistent storage
func (s *store) loadPeers() error {
	peers, err := readPeersJSON(filepath.Join(s.path, "peers.json"))
	if err != nil {
		return err
	}

	// If we have existing peers, use those.  This will override what's in the
	// config.
	if len(peers) > 0 {
		s.peers = peers

		if _, err := os.Stat(filepath.Join(s.path, "raft.db")); err != nil {
			return err
		}
	}

	return nil
}

func readPeersJSON(path string) ([]string, error) {
	// Read the file
	buf, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	// Check for no peers
	if len(buf) == 0 {
		return nil, nil
	}

	// Decode the peers
	var peers []string
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&peers); err != nil {
		return nil, err
	}

	return peers, nil
}

// IDPath returns the path to the local node ID file.
func (s *store) IDPath() string { return filepath.Join(s.path, "id") }

// readID reads the local node ID from the ID file.
func (s *store) readID() error {
	b, err := ioutil.ReadFile(s.IDPath())
	if os.IsNotExist(err) {
		s.id = 0
		return nil
	} else if err != nil {
		return fmt.Errorf("read file: %s", err)
	}

	id, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return fmt.Errorf("parse id: %s", err)
	}
	s.id = id

	return nil
}

func (s *store) openRaft() error {
	rs := newRaftState(s.config, s.peers)
	rs.ln = s.raftln
	rs.logger = s.logger
	rs.path = s.path
	rs.remoteAddr = s.raftln.Addr()
	if err := rs.open(s); err != nil {
		return err
	}
	s.raftState = rs

	return nil
}

func (s *store) joinCluster() error {

	// No join options, so nothing to do
	if len(s.peers) == 0 {
		return nil
	}

	// We already have a node ID so were already part of a cluster,
	// don't join again so we can use our existing state.
	if s.id != 0 {
		s.logger.Printf("Skipping cluster join: already member of cluster: nodeId=%v raftEnabled=%v peers=%v",
			s.id, raft.PeerContained(s.peers, s.addr), s.peers)
		return nil
	}

	s.logger.Printf("Joining cluster at: %v", s.peers)
	for {
		for _, join := range s.peers {
			// delete me:
			_ = join

			// TODO rework this to use the HTTP endpoint for joining
			//res, err := s.rpc.join(s.RemoteAddr.String(), join)
			//if err != nil {
			//s.logger.Printf("Join node %v failed: %v: retrying...", join, err)
			//continue
			//}

			//s.logger.Printf("Joined remote node %v", join)
			//s.logger.Printf("nodeId=%v raftEnabled=%v peers=%v", res.NodeID, res.RaftEnabled, res.RaftNodes)

			//s.peers = res.RaftNodes
			//s.id = res.NodeID

			//if err := s.writeNodeID(res.NodeID); err != nil {
			//s.logger.Printf("Write node id failed: %v", err)
			//break
			//}

			return nil
		}
		time.Sleep(time.Second)
	}
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.closing)
	return nil
}

func (s *store) snapshot() (*Data, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	spew.Dump(s.data)
	spew.Dump(s.data.Clone())
	return s.data.Clone(), nil
}

// afterIndex returns a channel that will be closed to signal
// the caller when an updated snapshot is available.
func (s *store) afterIndex(index uint64) <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.data.Index {
		// Client needs update so return a closed channel.
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return s.dataChanged
}

// WaitForLeader sleeps until a leader is found or a timeout occurs.
// timeout == 0 means to wait forever.
func (s *store) waitForLeader(timeout time.Duration) error {
	// Begin timeout timer.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Continually check for leader until timeout.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return errors.New("closing")
		case <-timer.C:
			if timeout != 0 {
				return errors.New("timeout")
			}
		case <-ticker.C:
			if s.leader() != "" {
				return nil
			}
		}
	}
}

// isLeader returns true if the store is currently the leader.
func (s *store) isLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil {
		return false
	}
	return s.raftState.raft.State() == raft.Leader
}

// leader returns what the store thinks is the current leader. An empty
// string indicates no leader exists.
func (s *store) leader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil {
		return ""
	}
	return s.raftState.raft.Leader()
}

// index returns the current store index.
func (s *store) index() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Index
}

// apply applies a command to raft.
func (s *store) apply(b []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Apply to raft log.
	f := s.raftState.raft.Apply(b, 0)
	if err := f.Error(); err != nil {
		return err
	}

	// Return response if it's an error.
	// No other non-nil objects should be returned.
	resp := f.Response()
	if err, ok := resp.(error); ok {
		return err
	}

	// resp should either be an error or nil.
	if resp != nil {
		panic(fmt.Sprintf("unexpected response: %#v", resp))
	}

	return nil
}

// RetentionPolicyUpdate represents retention policy fields to be updated.
type RetentionPolicyUpdate struct {
	Name     *string
	Duration *time.Duration
	ReplicaN *int
}

// SetName sets the RetentionPolicyUpdate.Name
func (rpu *RetentionPolicyUpdate) SetName(v string) { rpu.Name = &v }

// SetDuration sets the RetentionPolicyUpdate.Duration
func (rpu *RetentionPolicyUpdate) SetDuration(v time.Duration) { rpu.Duration = &v }

// SetReplicaN sets the RetentionPolicyUpdate.ReplicaN
func (rpu *RetentionPolicyUpdate) SetReplicaN(v int) { rpu.ReplicaN = &v }
