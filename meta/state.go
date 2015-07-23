package meta

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// raftState abstracts the interaction of the raft consensus layer
// across local or remote nodes.  It is a form of the state design pattern and allows
// the meta.Store to change its behavior with the raft layer at runtime.
type raftState interface {
	open() error
	initialize() error
	leader() string
	isLeader() bool
	sync(index uint64, timeout time.Duration) error
	setPeers(addrs []string) error
	addPeer(addr string) error
	invalidate() error
	close() error
	lastIndex() uint64
	apply(b []byte) error
	snapshot() error
}

// localRaft is a consensus strategy that uses a local raft implementation for
// consensus operations.
type localRaft struct {
	store     *Store
	raft      *raft.Raft
	transport *raft.NetworkTransport
	peerStore raft.PeerStore
	raftStore *raftboltdb.BoltStore
	raftLayer *raftLayer
}

func (r *localRaft) updateMetaData(ms *Data) {
	if ms == nil {
		return
	}

	updated := false
	r.store.mu.RLock()
	if ms.Index > r.store.data.Index {
		updated = true
	}
	r.store.mu.RUnlock()

	if updated {
		r.store.Logger.Printf("Updating metastore to term=%v index=%v", ms.Term, ms.Index)
		r.store.mu.Lock()
		r.store.data = ms
		r.store.mu.Unlock()
	}
}

func (r *localRaft) invalidate() error {
	if r.store.IsLeader() {
		return nil
	}

	ms, err := r.store.rpc.fetchMetaData(false)
	if err != nil {
		return err
	}

	r.updateMetaData(ms)
	return nil
}

func (r *localRaft) open() error {
	s := r.store
	// Setup raft configuration.
	config := raft.DefaultConfig()
	config.Logger = s.Logger
	config.HeartbeatTimeout = s.HeartbeatTimeout
	config.ElectionTimeout = s.ElectionTimeout
	config.LeaderLeaseTimeout = s.LeaderLeaseTimeout
	config.CommitTimeout = s.CommitTimeout

	// If no peers are set in the config then start as a single server.
	config.EnableSingleNode = (len(s.peers) == 0)

	// Ensure our addr is in the peer list
	if config.EnableSingleNode {
		s.peers = append(s.peers, s.Addr.String())
	}

	// Build raft layer to multiplex listener.
	r.raftLayer = newRaftLayer(s.RaftListener, s.Addr)

	// Create a transport layer
	r.transport = raft.NewNetworkTransport(r.raftLayer, 3, 10*time.Second, os.Stderr)

	// Create peer storage.
	r.peerStore = raft.NewJSONPeers(s.path, r.transport)

	// Create the log store and stable store.
	store, err := raftboltdb.NewBoltStore(filepath.Join(s.path, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	r.raftStore = store

	// Create the snapshot store.
	snapshots, err := raft.NewFileSnapshotStore(s.path, raftSnapshotsRetained, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create raft log.
	ra, err := raft.NewRaft(config, (*storeFSM)(s), store, store, snapshots, r.peerStore, r.transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	r.raft = ra

	return nil
}

func (r *localRaft) close() error {
	// Shutdown raft.
	if r.raft != nil {
		r.raft.Shutdown()
		r.raft = nil
	}
	if r.transport != nil {
		r.transport.Close()
		r.transport = nil
	}
	if r.raftStore != nil {
		r.raftStore.Close()
		r.raftStore = nil
	}

	return nil
}

func (r *localRaft) initialize() error {
	s := r.store
	// If we have committed entries then the store is already in the cluster.
	if index, err := r.raftStore.LastIndex(); err != nil {
		return fmt.Errorf("last index: %s", err)
	} else if index > 0 {
		return nil
	}

	// Force set peers.
	if err := r.setPeers(s.peers); err != nil {
		return fmt.Errorf("set raft peers: %s", err)
	}

	return nil
}

// apply applies a serialized command to the raft log.
func (r *localRaft) apply(b []byte) error {
	// Apply to raft log.
	f := r.raft.Apply(b, 0)
	if err := f.Error(); err != nil {
		return err
	}

	// Return response if it's an error.
	// No other non-nil objects should be returned.
	resp := f.Response()
	if err, ok := resp.(error); ok {
		return lookupError(err)
	}
	assert(resp == nil, "unexpected response: %#v", resp)

	return nil
}

func (r *localRaft) lastIndex() uint64 {
	return r.raft.LastIndex()
}

func (r *localRaft) sync(index uint64, timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		// Wait for next tick or timeout.
		select {
		case <-ticker.C:
		case <-timer.C:
			return errors.New("timeout")
		}

		// Compare index against current metadata.
		r.store.mu.Lock()
		ok := (r.store.data.Index >= index)
		r.store.mu.Unlock()

		// Exit if we are at least at the given index.
		if ok {
			return nil
		}
	}
}

func (r *localRaft) snapshot() error {
	future := r.raft.Snapshot()
	return future.Error()
}

// addPeer adds addr to the list of peers in the cluster.
func (r *localRaft) addPeer(addr string) error {
	peers, err := r.peerStore.Peers()
	if err != nil {
		return err
	}

	if len(peers) >= 3 {
		return nil
	}

	if fut := r.raft.AddPeer(addr); fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

// setPeers sets a list of peers in the cluster.
func (r *localRaft) setPeers(addrs []string) error {
	a := make([]string, len(addrs))
	for i, s := range addrs {
		addr, err := net.ResolveTCPAddr("tcp", s)
		if err != nil {
			return fmt.Errorf("cannot resolve addr: %s, err=%s", s, err)
		}
		a[i] = addr.String()
	}
	return r.raft.SetPeers(a).Error()
}

func (r *localRaft) leader() string {
	if r.raft == nil {
		return ""
	}

	return r.raft.Leader()
}

func (r *localRaft) isLeader() bool {
	r.store.mu.RLock()
	defer r.store.mu.RUnlock()
	if r.raft == nil {
		return false
	}
	return r.raft.State() == raft.Leader
}

// remoteRaft is a consensus strategy that uses a remote raft cluster for
// consensus operations.
type remoteRaft struct {
	store *Store
}

func (r *remoteRaft) updateMetaData(ms *Data) {
	if ms == nil {
		return
	}

	updated := false
	r.store.mu.RLock()
	if ms.Index > r.store.data.Index {
		updated = true
	}
	r.store.mu.RUnlock()

	if updated {
		r.store.Logger.Printf("Updating metastore to term=%v index=%v", ms.Term, ms.Index)
		r.store.mu.Lock()
		r.store.data = ms
		r.store.mu.Unlock()
	}
}

func (r *remoteRaft) invalidate() error {
	ms, err := r.store.rpc.fetchMetaData(false)
	if err != nil {
		return err
	}

	r.updateMetaData(ms)
	return nil
}

func (r *remoteRaft) setPeers(addrs []string) error {
	return nil
}

// addPeer adds addr to the list of peers in the cluster.
func (r *remoteRaft) addPeer(addr string) error {
	return fmt.Errorf("cannot add peer using remote raft")
}

func (r *remoteRaft) open() error {
	go func() {
		for {
			select {
			case <-r.store.closing:
				return
			default:
			}

			ms, err := r.store.rpc.fetchMetaData(true)
			if err != nil {
				r.store.Logger.Printf("fetch metastore: %v", err)
				time.Sleep(time.Second)
				continue
			}
			r.updateMetaData(ms)
		}
	}()
	return nil
}

func (r *remoteRaft) close() error {
	return nil
}

// apply applies a serialized command to the raft log.
func (r *remoteRaft) apply(b []byte) error {
	return fmt.Errorf("cannot apply log while in remote raft state")
}

func (r *remoteRaft) initialize() error {
	return nil
}

func (r *remoteRaft) leader() string {
	if len(r.store.peers) == 0 {
		return ""
	}

	return r.store.peers[rand.Intn(len(r.store.peers))]
}

func (r *remoteRaft) isLeader() bool {
	return false
}

func (r *remoteRaft) lastIndex() uint64 {
	return r.store.cachedData().Index
}

func (r *remoteRaft) sync(index uint64, timeout time.Duration) error {
	//FIXME: jwilder: check index and timeout
	return r.store.invalidate()
}

func (r *remoteRaft) snapshot() error {
	return fmt.Errorf("cannot snapshot while in remote raft state")
}
