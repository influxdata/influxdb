package meta

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// Raft configuration.
const (
	raftLogCacheSize      = 512
	raftSnapshotsRetained = 2
	raftTransportMaxPool  = 3
	raftTransportTimeout  = 10 * time.Second
)

// raftState is a consensus strategy that uses a local raft implementation for
// consensus operations.
type raftState struct {
	wg        sync.WaitGroup
	config    *Config
	closing   chan struct{}
	raft      *raft.Raft
	transport *raft.NetworkTransport
	peerStore raft.PeerStore
	raftStore *raftboltdb.BoltStore
	raftLayer *raftLayer
	ln        net.Listener
	addr      string
	logger    *log.Logger
	path      string
}

func newRaftState(c *Config, addr string) *raftState {
	return &raftState{
		config: c,
		addr:   addr,
	}
}

func (r *raftState) open(s *store, ln net.Listener, initializePeers []string) error {
	r.ln = ln
	r.closing = make(chan struct{})

	// Setup raft configuration.
	config := raft.DefaultConfig()
	config.LogOutput = ioutil.Discard

	if r.config.ClusterTracing {
		config.Logger = r.logger
	}
	config.HeartbeatTimeout = time.Duration(r.config.HeartbeatTimeout)
	config.ElectionTimeout = time.Duration(r.config.ElectionTimeout)
	config.LeaderLeaseTimeout = time.Duration(r.config.LeaderLeaseTimeout)
	config.CommitTimeout = time.Duration(r.config.CommitTimeout)
	// Since we actually never call `removePeer` this is safe.
	// If in the future we decide to call remove peer we have to re-evaluate how to handle this
	config.ShutdownOnRemove = false

	// Build raft layer to multiplex listener.
	r.raftLayer = newRaftLayer(r.addr, r.ln)

	// Create a transport layer
	r.transport = raft.NewNetworkTransport(r.raftLayer, 3, 10*time.Second, config.LogOutput)

	// Create peer storage.
	r.peerStore = raft.NewJSONPeers(r.path, r.transport)

	// This server is joining the raft cluster for the first time if initializePeers are passed in
	if len(initializePeers) > 0 {
		if err := r.peerStore.SetPeers(initializePeers); err != nil {
			return err
		}
	}

	peers, err := r.peerStore.Peers()
	if err != nil {
		return err
	}

	// If no peers are set in the config or there is one and we are it, then start as a single server.
	if len(initializePeers) <= 1 {
		config.EnableSingleNode = true

		// Ensure we can always become the leader
		config.DisableBootstrapAfterElect = false

		// Make sure our peer address is here.  This happens with either a single node cluster
		// or a node joining the cluster, as no one else has that information yet.
		if !raft.PeerContained(peers, r.addr) {
			if err := r.peerStore.SetPeers([]string{r.addr}); err != nil {
				return err
			}
		}

		peers = []string{r.addr}
	}

	// If we have multiple nodes in the cluster, make sure our address is in the raft peers or
	// we won't be able to boot into the cluster because the other peers will reject our new hostname.  This
	// is difficult to resolve automatically because we need to have all the raft peers agree on the current members
	// of the cluster before we can change them.
	if len(peers) > 0 && !raft.PeerContained(peers, r.addr) {
		r.logger.Printf("%s is not in the list of raft peers. Please update %v/peers.json on all raft nodes to have the same contents.", r.addr, r.path)
		return fmt.Errorf("peers out of sync: %v not in %v", r.addr, peers)
	}

	// Create the log store and stable store.
	store, err := raftboltdb.NewBoltStore(filepath.Join(r.path, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	r.raftStore = store

	// Create the snapshot store.
	snapshots, err := raft.NewFileSnapshotStore(r.path, raftSnapshotsRetained, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create raft log.
	ra, err := raft.NewRaft(config, (*storeFSM)(s), store, store, snapshots, r.peerStore, r.transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	r.raft = ra

	r.wg.Add(1)
	go r.logLeaderChanges()

	return nil
}

func (r *raftState) logLeaderChanges() {
	defer r.wg.Done()
	// Logs our current state (Node at 1.2.3.4:8088 [Follower])
	r.logger.Printf(r.raft.String())
	for {
		select {
		case <-r.closing:
			return
		case <-r.raft.LeaderCh():
			peers, err := r.peers()
			if err != nil {
				r.logger.Printf("failed to lookup peers: %v", err)
			}
			r.logger.Printf("%v. peers=%v", r.raft.String(), peers)
		}
	}
}

func (r *raftState) close() error {
	if r == nil {
		return nil
	}
	if r.closing != nil {
		close(r.closing)
	}
	r.wg.Wait()

	if r.transport != nil {
		r.transport.Close()
		r.transport = nil
	}

	// Shutdown raft.
	if r.raft != nil {
		if err := r.raft.Shutdown().Error(); err != nil {
			return err
		}
		r.raft = nil
	}

	if r.raftStore != nil {
		r.raftStore.Close()
		r.raftStore = nil
	}

	return nil
}

// apply applies a serialized command to the raft log.
func (r *raftState) apply(b []byte) error {
	// Apply to raft log.
	f := r.raft.Apply(b, 0)
	if err := f.Error(); err != nil {
		return err
	}

	// Return response if it's an error.
	// No other non-nil objects should be returned.
	resp := f.Response()
	if err, ok := resp.(error); ok {
		return err
	}
	if resp != nil {
		panic(fmt.Sprintf("unexpected response: %#v", resp))
	}

	return nil
}

func (r *raftState) lastIndex() uint64 {
	return r.raft.LastIndex()
}

func (r *raftState) snapshot() error {
	future := r.raft.Snapshot()
	return future.Error()
}

// addPeer adds addr to the list of peers in the cluster.
func (r *raftState) addPeer(addr string) error {
	peers, err := r.peerStore.Peers()
	if err != nil {
		return err
	}

	for _, p := range peers {
		if addr == p {
			return nil
		}
	}

	if fut := r.raft.AddPeer(addr); fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

// removePeer removes addr from the list of peers in the cluster.
func (r *raftState) removePeer(addr string) error {
	// Only do this on the leader
	if !r.isLeader() {
		return raft.ErrNotLeader
	}
	if fut := r.raft.RemovePeer(addr); fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

func (r *raftState) peers() ([]string, error) {
	return r.peerStore.Peers()
}

func (r *raftState) leader() string {
	if r.raft == nil {
		return ""
	}

	return r.raft.Leader()
}

func (r *raftState) isLeader() bool {
	if r.raft == nil {
		return false
	}
	return r.raft.State() == raft.Leader
}

// raftLayer wraps the connection so it can be re-used for forwarding.
type raftLayer struct {
	addr   *raftLayerAddr
	ln     net.Listener
	conn   chan net.Conn
	closed chan struct{}
}

type raftLayerAddr struct {
	addr string
}

func (r *raftLayerAddr) Network() string {
	return "tcp"
}

func (r *raftLayerAddr) String() string {
	return r.addr
}

// newRaftLayer returns a new instance of raftLayer.
func newRaftLayer(addr string, ln net.Listener) *raftLayer {
	return &raftLayer{
		addr:   &raftLayerAddr{addr},
		ln:     ln,
		conn:   make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

// Addr returns the local address for the layer.
func (l *raftLayer) Addr() net.Addr {
	return l.addr
}

// Dial creates a new network connection.
func (l *raftLayer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	// Write a marker byte for raft messages.
	_, err = conn.Write([]byte{MuxHeader})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}

// Accept waits for the next connection.
func (l *raftLayer) Accept() (net.Conn, error) { return l.ln.Accept() }

// Close closes the layer.
func (l *raftLayer) Close() error { return l.ln.Close() }
