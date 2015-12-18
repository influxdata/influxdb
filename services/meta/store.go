package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type store struct {
	mu          sync.RWMutex
	closing     chan struct{}
	data        *Data
	raftState   *raftState
	dataChanged chan struct{}
	addr        string
	raftln      net.Listener
	path        string
	opened      bool
}

func newStore(c *Config) *store {
	s := store{
		data: &Data{
			Index: 1,
		},
		closing:     make(chan struct{}),
		dataChanged: make(chan struct{}),
		addr:        c.RaftAddr,
		path:        c.Dir,
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
	s.addr = ln.Addr()

	s.Logger.Printf("Using data dir: %v", s.path)

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
		if err := s.openRaft(); err != nil {
			return fmt.Errorf("raft: %s", err)
		}

		// Initialize the store, if necessary.
		if err := s.initialize(); err != nil {
			return fmt.Errorf("initialize raft: %s", err)
		}

		// Load existing ID, if exists.
		if err := s.readID(); err != nil {
			return fmt.Errorf("read id: %s", err)
		}

		return nil
	}(); err != nil {
		return err
	}

	// Begin serving listener.
	s.wg.Add(1)
	go s.serveExecListener()

	s.wg.Add(1)
	go s.serveRPCListener()

	// Join an existing cluster if we needed
	if err := s.joinCluster(); err != nil {
		return fmt.Errorf("join: %v", err)
	}

	// If the ID doesn't exist then create a new node.
	if s.id == 0 {
		go s.init()
	} else {
		go s.syncNodeInfo()
		close(s.ready)
	}

	// Wait for a leader to be elected so we know the raft log is loaded
	// and up to date
	<-s.ready
	if err := s.WaitForLeader(0); err != nil {
		return err
	}

	if s.raftPromotionEnabled {
		s.wg.Add(1)
		s.Logger.Printf("spun up monitoring for %d", s.NodeID())
		go s.monitorPeerHealth()
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
func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.closing)
	return nil
}

func (s *store) snapshot() (*Data, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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

// isLeader returns true if the store is currently the leader.
func (s *store) isLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raft == nil {
		return false
	}
	return s.raft.State() == raft.Leader
}

// leader returns what the store thinks is the current leader. An empty
// string indicates no leader exists.
func (s *store) leader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.raft.Leader()
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
	f := s.raft.Apply(b, 0)
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
