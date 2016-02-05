package meta

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb/services/meta/internal"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
)

// Retention policy settings.
const (
	autoCreateRetentionPolicyName   = "default"
	autoCreateRetentionPolicyPeriod = 0

	// maxAutoCreatedRetentionPolicyReplicaN is the maximum replication factor that will
	// be set for auto-created retention policies.
	maxAutoCreatedRetentionPolicyReplicaN = 3
)

// Raft configuration.
const (
	raftListenerStartupTimeout = time.Second
)

type store struct {
	mu      sync.RWMutex
	closing chan struct{}

	config      *Config
	data        *Data
	raftState   *raftState
	dataChanged chan struct{}
	path        string
	opened      bool
	logger      *log.Logger

	// Authentication cache.
	authCache map[string]authUser

	raftAddr string
	httpAddr string
}

type authUser struct {
	salt []byte
	hash []byte
}

// newStore will create a new metastore with the passed in config
func newStore(c *Config, httpAddr, raftAddr string) *store {
	s := store{
		data: &Data{
			Index: 1,
		},
		closing:     make(chan struct{}),
		dataChanged: make(chan struct{}),
		path:        c.Dir,
		config:      c,
		httpAddr:    httpAddr,
		raftAddr:    raftAddr,
	}
	if c.LoggingEnabled {
		s.logger = log.New(os.Stderr, "[metastore] ", log.LstdFlags)
	} else {
		s.logger = log.New(ioutil.Discard, "", 0)
	}

	return &s
}

// open opens and initializes the raft store.
func (s *store) open(raftln net.Listener) error {
	s.logger.Printf("Using data dir: %v", s.path)

	// See if this server needs to join the raft consensus group
	var initializePeers []string
	if len(s.config.JoinPeers) > 0 {
		c := NewClient(s.config.JoinPeers, s.config.HTTPSEnabled)
		data := c.retryUntilSnapshot(0)
		for _, n := range data.MetaNodes {
			initializePeers = append(initializePeers, n.TCPHost)
		}
		initializePeers = append(initializePeers, s.raftAddr)
	}

	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Check if store has already been opened.
		if s.opened {
			return ErrStoreOpen
		}
		s.opened = true

		// Create the root directory if it doesn't already exist.
		if err := os.MkdirAll(s.path, 0777); err != nil {
			return fmt.Errorf("mkdir all: %s", err)
		}

		// Open the raft store.
		if err := s.openRaft(initializePeers, raftln); err != nil {
			return fmt.Errorf("raft: %s", err)
		}

		return nil
	}(); err != nil {
		return err
	}

	if len(s.config.JoinPeers) > 0 {
		c := NewClient(s.config.JoinPeers, s.config.HTTPSEnabled)
		if err := c.Open(); err != nil {
			return err
		}
		defer c.Close()

		if err := c.JoinMetaServer(s.httpAddr, s.raftAddr); err != nil {
			return err
		}
	}

	// Wait for a leader to be elected so we know the raft log is loaded
	// and up to date
	if err := s.waitForLeader(0); err != nil {
		return err
	}

	// Make sure this server is in the list of metanodes
	peers, err := s.raftState.peers()
	if err != nil {
		return err
	}
	if len(peers) <= 1 {
		// we have to loop here because if the hostname has changed
		// raft will take a little bit to normalize so that this host
		// will be marked as the leader
		for {
			err := s.setMetaNode(s.httpAddr, s.raftAddr)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}

func (s *store) openRaft(initializePeers []string, raftln net.Listener) error {
	rs := newRaftState(s.config, s.raftAddr)
	rs.logger = s.logger
	rs.path = s.path

	if err := rs.open(s, raftln, initializePeers); err != nil {
		return err
	}
	s.raftState = rs

	return nil
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.closing:
		// already closed
		return nil
	default:
		close(s.closing)
		return s.raftState.close()
	}
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

// leaderHTTP returns the HTTP API connection info for the metanode
// that is the raft leader
func (s *store) leaderHTTP() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil {
		return ""
	}
	l := s.raftState.raft.Leader()

	for _, n := range s.data.MetaNodes {
		if n.TCPHost == l {
			return n.Host
		}
	}

	return ""
}

// otherMetaServersHTTP will return the HTTP bind addresses of the other
// meta servers in the cluster
func (s *store) otherMetaServersHTTP() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var a []string
	for _, n := range s.data.MetaNodes {
		if n.TCPHost != s.raftAddr {
			a = append(a, n.Host)
		}
	}
	return a
}

// index returns the current store index.
func (s *store) index() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Index
}

// apply applies a command to raft.
func (s *store) apply(b []byte) error {
	return s.raftState.apply(b)
}

// join adds a new server to the metaservice and raft
func (s *store) join(n *NodeInfo) error {
	if err := s.raftState.addPeer(n.TCPHost); err != nil {
		return err
	}

	return s.createMetaNode(n.Host, n.TCPHost)
}

// leave removes a server from the metaservice and raft
func (s *store) leave(n *NodeInfo) error {
	return s.raftState.removePeer(n.TCPHost)
}

// createMetaNode is used by the join command to create the metanode int
// the metastore
func (s *store) createMetaNode(addr, raftAddr string) error {
	val := &internal.CreateMetaNodeCommand{
		HTTPAddr: proto.String(addr),
		TCPAddr:  proto.String(raftAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}
	t := internal.Command_CreateMetaNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_CreateMetaNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// setMetaNode is used when the raft group has only a single peer. It will
// either create a metanode or update the information for the one metanode
// that is there. It's used because hostnames can change
func (s *store) setMetaNode(addr, raftAddr string) error {
	val := &internal.SetMetaNodeCommand{
		HTTPAddr: proto.String(addr),
		TCPAddr:  proto.String(raftAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}
	t := internal.Command_SetMetaNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_SetMetaNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}
