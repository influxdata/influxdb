package meta

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/influxdb/influxdb/meta/internal"
)

const (
	// DefaultHeartbeatTimeout is the default heartbeat timeout for the store.
	DefaultHeartbeatTimeout = 1000 * time.Millisecond

	// DefaultElectionTimeout is the default election timeout for the store.
	DefaultElectionTimeout = 1000 * time.Millisecond

	// DefaultLeaderLeaseTimeout is the default leader lease for the store.
	DefaultLeaderLeaseTimeout = 500 * time.Millisecond

	// DefaultCommitTimeout is the default commit timeout for the store.
	DefaultCommitTimeout = 50 * time.Millisecond
)

// Raft configuration.
const (
	raftLogCacheSize      = 512
	raftSnapshotsRetained = 2
	raftTransportMaxPool  = 3
	raftTransportTimeout  = 10 * time.Second
)

// Store represents a raft-backed metastore.
type Store struct {
	mu        sync.RWMutex
	path      string
	data      *Data
	raft      *raft.Raft
	peers     raft.PeerStore
	transport *raft.NetworkTransport
	store     *raftboltdb.BoltStore

	// The amount of time before a follower starts a new election.
	HeartbeatTimeout time.Duration

	// The amount of time before a candidate starts a new election.
	ElectionTimeout time.Duration

	// The amount of time without communication to the cluster before a
	// leader steps down to a follower state.
	LeaderLeaseTimeout time.Duration

	// The amount of time without an apply before sending a heartbeat.
	CommitTimeout time.Duration

	Logger *log.Logger
}

// NewStore returns a new instance of Store.
func NewStore() *Store {
	return &Store{
		data:               &Data{},
		HeartbeatTimeout:   DefaultHeartbeatTimeout,
		ElectionTimeout:    DefaultElectionTimeout,
		LeaderLeaseTimeout: DefaultLeaderLeaseTimeout,
		CommitTimeout:      DefaultCommitTimeout,
		Logger:             log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Path returns the root path when open.
// Returns an empty string when the store is closed.
func (s *Store) Path() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.path
}

// opened returns true if the store is in an open state.
func (s *Store) opened() bool { return s.path != "" }

// Open opens and initializes the raft store.
func (s *Store) Open(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if store has already been opened.
	if s.opened() {
		return errors.New("raft store already open")
	}
	s.path = path

	if err := func() error {
		// Create the root directory if it doesn't already exist.
		if err := os.MkdirAll(path, 0777); err != nil {
			return fmt.Errorf("mkdir all: %s", err)
		}

		// Setup raft configuration.
		config := raft.DefaultConfig()
		config.Logger = s.Logger
		config.EnableSingleNode = true
		config.HeartbeatTimeout = s.HeartbeatTimeout
		config.ElectionTimeout = s.ElectionTimeout
		config.LeaderLeaseTimeout = s.LeaderLeaseTimeout
		config.CommitTimeout = s.CommitTimeout

		// Create a transport layer
		advertise, err := net.ResolveTCPAddr("tcp", "localhost:9000")
		if err != nil {
			return fmt.Errorf("resolve tcp addr: %s", err)
		}
		transport, err := raft.NewTCPTransport(":9000", advertise,
			raftTransportMaxPool, raftTransportTimeout, os.Stderr)
		if err != nil {
			return fmt.Errorf("new tcp transport: %s", err)
		}
		s.transport = transport

		// Create peer storage.
		s.peers = raft.NewJSONPeers(path, transport)

		// Create the log store and stable store.
		store, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
		if err != nil {
			return fmt.Errorf("new bolt store: %s", err)
		}
		s.store = store

		// Create the snapshot store.
		snapshots, err := raft.NewFileSnapshotStore(path, raftSnapshotsRetained, os.Stderr)
		if err != nil {
			return fmt.Errorf("file snapshot store: %s", err)
		}

		// Create raft log.
		r, err := raft.NewRaft(config, (*StoreFSM)(s), store, store, snapshots, s.peers, s.transport)
		if err != nil {
			return fmt.Errorf("new raft: %s", err)
		}
		s.raft = r

		return nil
	}(); err != nil {
		s.close()
		return err
	}

	return nil
}

// Close closes the store and shuts down the node in the cluster.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.close()
}

func (s *Store) close() error {
	// Check if store has already been closed.
	if !s.opened() {
		return errors.New("raft store already closed")
	}
	s.path = ""

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

// SetData sets the root meta data.
func (s *Store) SetData(data *Data) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
}

// LeaderCh returns a channel that notifies on leadership change.
// Panics when the store has not been opened yet.
func (s *Store) LeaderCh() <-chan bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.raft == nil {
		panic("cannot retrieve leadership channel when closed")
	}

	return s.raft.LeaderCh()
}

// CreateNode creates a new node in the store.
func (s *Store) CreateNode(host string) (*NodeInfo, error) {
	if err := s.exec(
		internal.Command_CreateNodeCommand,
		internal.E_CreateNodeCommand_Command,
		&internal.CreateNodeCommand{
			Host: proto.String(host),
		},
	); err != nil {
		return nil, err
	}
	return s.NodeByHost(host)
}

// NodeByHost returns a node by hostname.
func (s *Store) NodeByHost(host string) (*NodeInfo, error) {
	n := s.data.NodeByHost(host)

	// FIX(benbjohnson): Invalidate cache if not found and check again.

	return n, nil
}

// CreateContinuousQuery(query string) (*ContinuousQueryInfo, error)
// DropContinuousQuery(query string) error

// Node(id uint64) (*NodeInfo, error)
// NodeByHost(host string) (*NodeInfo, error)
// CreateNode(host string) (*NodeInfo, error)
// DeleteNode(id uint64) error

// Database(name string) (*DatabaseInfo, error)
// CreateDatabase(name string) (*DatabaseInfo, error)
// CreateDatabaseIfNotExists(name string) (*DatabaseInfo, error)
// DropDatabase(name string) error

// RetentionPolicy(database, name string) (*RetentionPolicyInfo, error)
// CreateRetentionPolicy(database string, rp *RetentionPolicyInfo) (*RetentionPolicyInfo, error)
// CreateRetentionPolicyIfNotExists(database string, rp *RetentionPolicyInfo) (*RetentionPolicyInfo, error)
// SetDefaultRetentionPolicy(database, name string) error
// UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) (*RetentionPolicyInfo, error)
// DeleteRetentionPolicy(database, name string) error

// ShardGroup(database, policy string, timestamp time.Time) (*ShardGroupInfo, error)
// CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*ShardGroupInfo, error)
// DeleteShardGroup(database, policy string, shardID uint64) error

// User(username string) (*UserInfo, error)
// CreateUser(username, password string, admin bool) (*UserInfo, error)
// UpdateUser(username, password string) (*UserInfo, error)
// DeleteUser(username string) error
// SetPrivilege(p influxql.Privilege, username string, dbname string) error

func (s *Store) exec(typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	// Create command.
	cmd := &internal.Command{Type: &typ}
	if err := proto.SetExtension(cmd, desc, value); err != nil {
		return fmt.Errorf("set extension: %s", err)
	}

	// Marshal to a byte slice.
	b, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal proto: %s", err)
	}

	// Apply to raft log.
	f := s.raft.Apply(b, 0)
	if err := f.Error(); err != nil {
		return err
	}

	if resp := f.Response(); resp != nil {
		panic(fmt.Sprintf("unexpected response: %#v", resp))
	}

	return nil
}

// StoreFSM represents the finite state machine used by Store to interact with Raft.
type StoreFSM Store

func (fsm *StoreFSM) Apply(l *raft.Log) interface{} {
	var cmd internal.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		panic(fmt.Errorf("cannot marshal command: %x", l.Data))
	}

	switch cmd.GetType() {
	case internal.Command_CreateContinuousQueryCommand:
		return fsm.applyCreateContinuousQueryCommand(&cmd)
	case internal.Command_DropContinuousQueryCommand:
		return fsm.applyDropContinuousQueryCommand(&cmd)
	case internal.Command_CreateNodeCommand:
		return fsm.applyCreateNodeCommand(&cmd)
	case internal.Command_DeleteNodeCommand:
		return fsm.applyDeleteNodeCommand(&cmd)
	case internal.Command_CreateDatabaseCommand:
		return fsm.applyCreateDatabaseCommand(&cmd)
	case internal.Command_DropDatabaseCommand:
		return fsm.applyDropDatabaseCommand(&cmd)
	case internal.Command_CreateDatabaseIfNotExistsCommand:
		return fsm.applyCreateDatabaseIfNotExistsCommand(&cmd)
	case internal.Command_CreateRetentionPolicyCommand:
		return fsm.applyCreateRetentionPolicyCommand(&cmd)
	case internal.Command_CreateRetentionPolicyIfNotExistsCommand:
		return fsm.applyCreateRetentionPolicyIfNotExistsCommand(&cmd)
	case internal.Command_DeleteRetentionPolicyCommand:
		return fsm.applyDeleteRetentionPolicyCommand(&cmd)
	case internal.Command_SetDefaultRetentionPolicyCommand:
		return fsm.applySetDefaultRetentionPolicyCommand(&cmd)
	case internal.Command_UpdateRetentionPolicyCommand:
		return fsm.applyUpdateRetentionPolicyCommand(&cmd)
	case internal.Command_CreateShardGroupIfNotExistsCommand:
		return fsm.applyCreateShardGroupIfNotExistsCommand(&cmd)
	case internal.Command_CreateUserCommand:
		return fsm.applyCreateUserCommand(&cmd)
	case internal.Command_DeleteUserCommand:
		return fsm.applyDeleteUserCommand(&cmd)
	case internal.Command_UpdateUserCommand:
		return fsm.applyUpdateUserCommand(&cmd)
	case internal.Command_SetPrivilegeCommand:
		return fsm.applySetPrivilegeCommand(&cmd)
	case internal.Command_DeleteShardGroupCommand:
		return fsm.applyDeleteShardGroupCommand(&cmd)
	default:
		panic(fmt.Errorf("cannot apply command: %x", l.Data))
	}
}

func (fsm *StoreFSM) applyCreateContinuousQueryCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyDropContinuousQueryCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyCreateNodeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateNodeCommand_Command)
	v := ext.(*internal.CreateNodeCommand)

	// Copy data and update.
	data, err := fsm.data.CreateNode(v.GetHost())
	if err != nil {
		return err
	}
	(*Store)(fsm).SetData(data)

	return nil
}

func (fsm *StoreFSM) applyDeleteNodeCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyCreateDatabaseCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyDropDatabaseCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyCreateDatabaseIfNotExistsCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyCreateRetentionPolicyCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyCreateRetentionPolicyIfNotExistsCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyDeleteRetentionPolicyCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applySetDefaultRetentionPolicyCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyUpdateRetentionPolicyCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyCreateShardGroupIfNotExistsCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyCreateUserCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyDeleteUserCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyUpdateUserCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applySetPrivilegeCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) applyDeleteShardGroupCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *StoreFSM) Snapshot() (raft.FSMSnapshot, error) {
	s := (*Store)(fsm)
	s.mu.Lock()
	defer s.mu.Unlock()

	return &StoreFSMSnapshot{Data: (*Store)(fsm).data}, nil
}

func (fsm *StoreFSM) Restore(r io.ReadCloser) error {
	// Read all bytes.
	// b, err := ioutil.ReadAll(r)
	// if err != nil {
	// 	return err
	// }

	// TODO: Decode metadata.
	// TODO: Set metadata on store.

	return nil
}

type StoreFSMSnapshot struct {
	Data *Data
}

func (s *StoreFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// TODO: Encode data.
	// TODO: sink.Write(p)
	// TODO: sink.Close()
	panic("not implemented yet")
}

// Release is invoked when we are finished with the snapshot
func (s *StoreFSMSnapshot) Release() {}

// RetentionPolicyUpdate represents retention policy fields to be updated.
type RetentionPolicyUpdate struct {
	Name     *string
	Duration *time.Duration
	ReplicaN *uint32
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
