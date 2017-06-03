package multiraft

import (
	"sync"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-mdb"
)

// Log represents a combined cluster- and group- level log.
type Log struct {
	mu     sync.Mutex
	opened bool

	// Cluster-level raft log. This is only used if the current node is part
	// of the cluster-level Raft consensus group.
	cluster struct {
		raft      *raft.Raft
		store     *raftmdb.MDBStore
		snapshots *raft.FileSnapshotStore
		peers     *raft.JSONPeers
	}

	// Group-level raft log.
	group struct {
		raft      *raft.Raft
		store     *raftmdb.MDBStore
		snapshots *raft.FileSnapshotStore
		peers     *raft.JSONPeers
	}

	// Current cluster configuration, lookups by group id and node id.
	groups map[int]*Group // groups by group id.
	nodes  map[int]*Node  // nodes by node id.

	// Application-level FSM.
	FSM raft.FSM
}

// NewLog returns a new, unopened instance of Log.
func NewLog() *Log {
	return &Log{}
}

// Open initializes a multiraft log from a file path.
func (l *Log) Open(path string) (*Log, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Do not allow the log to be reopened.
	if l.opened {
		return errors.New("log already open")
	}

	// Make base directory, if not exists.
	if err := os.MkdirAll(path, 0700); err != nil {
		l.close()
		return nil, err
	}

	// Open the cluster log.
	if err := l.openClusterLog(filepath.Join(path, "cluster")); err != nil {
		_ = l.close()
		return nil, fmt.Errorf("cluster log: %s", err)
	}

	// Open the group log.
	if err := l.openGroupLog(filepath.Join(path, "group")); err != nil {
		_ = l.close()
		return nil, fmt.Errorf("group log: %s", err)
	}

	// Mark as open.
	l.opened = true

	return nil
}

// opens the cluster-level Raft log.
func (l *Log) openClusterLog(path string) error {
	// TODO(benbjohnson):
	// Check if this node is actually a part of the cluster-level consensus group.

	// Create a transport.
	t, err := raft.NewTCPTransport(addr, 3, 10*time.Second, nil)
	s.raftTransport = trans

	// Create a log store.
	s, err := raftmdb.NewMDBStoreWithSize(path, 8*(1<<30))
	if err != nil {
		return err
	}
	l.cluster.store = s

	// Create the snapshot store
	ss, err := raft.NewFileSnapshotStore(path, 2, nil)
	if err != nil {
		return err
	}
	l.cluster.snapshots = ss

	// Create a peer store.
	raft.NewJSONPeers(path, trans)

	// Open the raft log.
	r, err = raft.NewRaft(raft.DefaultConfig(), clusterFSM(l), s, s, snaps, peerStore, trans)
	if err != nil {
		_ = s.Close()
	}
	l.cluster.raft = r

	return nil
}

// opens the group-level Raft log.
func (l *Log) openGroupLog(path string) error {
	// TODO(benbjohnson): Do the same as the cluster log but for the group.
	return nil
}

// Close closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.close()
}

func (l *Log) close() error {
	// TODO(benbjohnson): Close cluster log.
	// TODO(benbjohnson): Close cluster store.
	// TODO(benbjohnson): Close cluster snapshot store.

	// TODO(benbjohnson): Close group log.
	// TODO(benbjohnson): Close group store.
	// TODO(benbjohnson): Close group snapshot store.

	// Mark as closed.
	l.opened = false

	return nil
}

// Group returns a group by id.
func (l *Log) Group(id int) *Group {
	l.Lock()
	defer l.Unlock()
	return l.groups[id]
}

// AddGroup adds a group to the cluster.
func (l *Log) AddGroup() (*Group, error) {
	// TODO(benbjohnson): Apply the command to the cluster level raft log.
	return nil
}

// RemoveGroup removes a group from the cluster.
func (l *Log) RemoveGroup(id int) error {
	// TODO(benbjohnson): Apply the command to the cluster level raft log.
	return nil
}

// Node returns a node by id.
func (l *Log) Node(id int) *Node {
	l.Lock()
	defer l.Unlock()
	return l.node[id]
}

// TODO(benbjohnson): Add commit log stream reader when node becomes group leader.

// logFSM wraps the Log type to implement the raft.FSM interface without
// exposing it to the external API. This FSM is used to propagate state from
// the cluster to the group.
type logFSM Log

func (l *logFSM) Apply(*Log) interface{} {
	// TODO(benbjohnson):
	//
	//   If this node is currently the leader then apply to group log.
	//   We need to figure out a way to ensure that cluster changes do not get
	//   missed in the event of a group-level election.
	//
	return nil
}

func (l *logFSM) Snapshot() (raft.FSMSnapshot, error) {
	// TODO(benbjohnson):
	//
	//   Discard cluster data since it is replicated to the group log.
	//
	return nil, nil
}

func (l *logFSM) Restore(io.ReadCloser) error {
	// TODO(benbjohnson):
	//
	//   We don't need to persist cluster data except for logs that are pending
	//   replication to the group.
	//
	return nil
}

// Group represents a collection of nodes.
// Each group maintains its own independent Raft log.
type Group struct {
	id    int
	nodes map[int]*Node
}

// ID returns the group identifier.
func (g *Group) ID() int { return g.id }

// Node returns a member node.
func (g *Group) Node(id int) { return g.nodes[id] }

// Node represents a member of a group.
type Node struct {
	id  int
	url url.URL
}

// ID returns the node identifier.
func (n *Node) ID() int { return n.id }
