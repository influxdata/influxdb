package meta

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// raftState abstracts the interaction of the raft consensus layer
// across local or remote nodes.  It is a form of the state design pattern and allows
// the meta.Store to change how its behavior with the raft layer at runtime.
type raftState interface {
	openRaft() error
	initialize() error
	leader() string
	isLeader() bool
	raftEnabled() bool
	sync(index uint64, timeout time.Duration) error
	invalidate() error
	close() error
}

// localRaft is a consensus strategy that uses a local raft implementation fo
// consensus operations.
type localRaft struct {
	store *Store
}

// raftEnable always return true for localRaft
func (r *localRaft) raftEnabled() bool {
	return true
}

func (r *localRaft) invalidate() error {
	time.Sleep(time.Second)
	return nil
}

func (r *localRaft) openRaft() error {
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
	s.raftLayer = newRaftLayer(s.RaftListener, s.Addr)

	// Create a transport layer
	s.transport = raft.NewNetworkTransport(s.raftLayer, 3, 10*time.Second, os.Stderr)

	// Create peer storage.
	s.peerStore = raft.NewJSONPeers(s.path, s.transport)

	// Create the log store and stable store.
	store, err := raftboltdb.NewBoltStore(filepath.Join(s.path, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	s.store = store

	// Create the snapshot store.
	snapshots, err := raft.NewFileSnapshotStore(s.path, raftSnapshotsRetained, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create raft log.
	ra, err := raft.NewRaft(config, (*storeFSM)(s), store, store, snapshots, s.peerStore, s.transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	return nil
}

func (r *localRaft) close() error {
	s := r.store
	// Shutdown raft.
	if s.raft != nil {
		s.raft.Shutdown()
		s.raft = nil
	}
	if s.transport != nil {
		s.transport.Close()
		s.transport = nil
	}
	if s.store != nil {
		s.store.Close()
		s.store = nil
	}

	return nil
}

func (r *localRaft) initialize() error {
	s := r.store
	// If we have committed entries then the store is already in the cluster.
	if index, err := s.store.LastIndex(); err != nil {
		return fmt.Errorf("last index: %s", err)
	} else if index > 0 {
		return nil
	}

	// Force set peers.
	if err := s.SetPeers(s.peers); err != nil {
		return fmt.Errorf("set raft peers: %s", err)
	}

	return nil
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

func (r *localRaft) leader() string {
	if r.store.raft == nil {
		return ""
	}

	return r.store.raft.Leader()
}

func (r *localRaft) isLeader() bool {
	r.store.mu.RLock()
	defer r.store.mu.RUnlock()
	return r.store.raft.State() == raft.Leader
}

// remoteRaft is a consensus strategy that uses a remote raft cluster for
// consensus operations.
type remoteRaft struct {
	store *Store
}

func (r *remoteRaft) raftEnabled() bool {
	return false
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

func (r *remoteRaft) openRaft() error {
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

func (r *remoteRaft) sync(index uint64, timeout time.Duration) error {
	//FIXME: jwilder: check index and timeout
	return r.store.invalidate()
}
