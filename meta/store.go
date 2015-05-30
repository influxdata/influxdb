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
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta/internal"
	"golang.org/x/crypto/bcrypt"
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
	mu     sync.RWMutex
	path   string
	opened bool

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
func NewStore(path string) *Store {
	return &Store{
		path:               path,
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
func (s *Store) Path() string { return s.path }

// Open opens and initializes the raft store.
func (s *Store) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if store has already been opened.
	if s.opened {
		return ErrStoreOpen
	}
	s.opened = true

	if err := func() error {
		// Create the root directory if it doesn't already exist.
		if err := os.MkdirAll(s.path, 0777); err != nil {
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
		s.peers = raft.NewJSONPeers(s.path, transport)

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
		r, err := raft.NewRaft(config, (*storeFSM)(s), store, store, snapshots, s.peers, s.transport)
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
	if !s.opened {
		return ErrStoreClosed
	}
	s.opened = false

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

// LeaderCh returns a channel that notifies on leadership change.
// Panics when the store has not been opened yet.
func (s *Store) LeaderCh() <-chan bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	assert(s.raft != nil, "cannot retrieve leadership channel when closed")
	return s.raft.LeaderCh()
}

// Node returns a node by id.
func (s *Store) Node(id uint64) (ni *NodeInfo, err error) {
	err = s.read(func(data *Data) error {
		ni = data.Node(id)
		if ni == nil {
			return errInvalidate
		}
		return nil
	})
	return
}

// NodeByHost returns a node by hostname.
func (s *Store) NodeByHost(host string) (ni *NodeInfo, err error) {
	err = s.read(func(data *Data) error {
		ni = data.NodeByHost(host)
		if ni == nil {
			return errInvalidate
		}
		return nil
	})
	return
}

// Nodes returns a list of all nodes.
func (s *Store) Nodes() (a []NodeInfo, err error) {
	err = s.read(func(data *Data) error {
		a = data.Nodes
		return nil
	})
	return
}

// CreateNode creates a new node in the store.
func (s *Store) CreateNode(host string) (*NodeInfo, error) {
	if err := s.exec(internal.Command_CreateNodeCommand, internal.E_CreateNodeCommand_Command,
		&internal.CreateNodeCommand{
			Host: proto.String(host),
		},
	); err != nil {
		return nil, err
	}
	return s.NodeByHost(host)
}

// DeleteNode removes a node from the metastore by id.
func (s *Store) DeleteNode(id uint64) error {
	return s.exec(internal.Command_DeleteNodeCommand, internal.E_DeleteNodeCommand_Command,
		&internal.DeleteNodeCommand{
			ID: proto.Uint64(id),
		},
	)
}

// Database returns a database by name.
func (s *Store) Database(name string) (di *DatabaseInfo, err error) {
	err = s.read(func(data *Data) error {
		di = data.Database(name)
		if di == nil {
			return errInvalidate
		}
		return nil
	})
	return
}

// Databases returns a list of all databases.
func (s *Store) Databases() (dis []DatabaseInfo, err error) {
	err = s.read(func(data *Data) error {
		dis = data.Databases
		return nil
	})
	return
}

// CreateDatabase creates a new database in the store.
func (s *Store) CreateDatabase(name string) (*DatabaseInfo, error) {
	if err := s.exec(internal.Command_CreateDatabaseCommand, internal.E_CreateDatabaseCommand_Command,
		&internal.CreateDatabaseCommand{
			Name: proto.String(name),
		},
	); err != nil {
		return nil, err
	}
	return s.Database(name)
}

// CreateDatabaseIfNotExists creates a new database in the store if it doesn't already exist.
func (s *Store) CreateDatabaseIfNotExists(name string) (*DatabaseInfo, error) {
	// Try to find database locally first.
	if di, err := s.Database(name); err != nil {
		return nil, err
	} else if di != nil {
		return di, nil
	}

	// Attempt to create database.
	if di, err := s.CreateDatabase(name); err == ErrDatabaseExists {
		return s.Database(name)
	} else {
		return di, err
	}
}

// DropDatabase removes a database from the metastore by name.
func (s *Store) DropDatabase(name string) error {
	return s.exec(internal.Command_DropDatabaseCommand, internal.E_DropDatabaseCommand_Command,
		&internal.DropDatabaseCommand{
			Name: proto.String(name),
		},
	)
}

// RetentionPolicy returns a retention policy for a database by name.
func (s *Store) RetentionPolicy(database, name string) (rpi *RetentionPolicyInfo, err error) {
	err = s.read(func(data *Data) error {
		rpi, err = data.RetentionPolicy(database, name)
		if err != nil {
			return err
		} else if rpi == nil {
			return errInvalidate
		}
		return nil
	})
	return
}

// DefaultRetentionPolicy returns the default retention policy for a database.
func (s *Store) DefaultRetentionPolicy(database string) (rpi *RetentionPolicyInfo, err error) {
	err = s.read(func(data *Data) error {
		di := data.Database(database)
		if di == nil {
			return ErrDatabaseNotFound
		}

		for i := range di.RetentionPolicies {
			if di.RetentionPolicies[i].Name == di.DefaultRetentionPolicy {
				rpi = &di.RetentionPolicies[i]
				return nil
			}
		}
		return errInvalidate
	})
	return
}

// RetentionPolicies returns a list of all retention policies for a database.
func (s *Store) RetentionPolicies(database string) (a []RetentionPolicyInfo, err error) {
	err = s.read(func(data *Data) error {
		di := data.Database(database)
		if di != nil {
			return ErrDatabaseNotFound
		}
		a = di.RetentionPolicies
		return nil
	})
	return
}

// CreateRetentionPolicy creates a new retention policy for a database.
func (s *Store) CreateRetentionPolicy(database string, rpi *RetentionPolicyInfo) (*RetentionPolicyInfo, error) {
	if err := s.exec(internal.Command_CreateRetentionPolicyCommand, internal.E_CreateRetentionPolicyCommand_Command,
		&internal.CreateRetentionPolicyCommand{
			Database:        proto.String(database),
			RetentionPolicy: rpi.protobuf(),
		},
	); err != nil {
		return nil, err
	}

	return s.RetentionPolicy(database, rpi.Name)
}

// CreateRetentionPolicyIfNotExists creates a new policy in the store if it doesn't already exist.
func (s *Store) CreateRetentionPolicyIfNotExists(database string, rpi *RetentionPolicyInfo) (*RetentionPolicyInfo, error) {
	// Try to find policy locally first.
	if rpi, err := s.RetentionPolicy(database, rpi.Name); err != nil {
		return nil, err
	} else if rpi != nil {
		return rpi, nil
	}

	// Attempt to create policy.
	if other, err := s.CreateRetentionPolicy(database, rpi); err == ErrRetentionPolicyExists {
		return s.RetentionPolicy(database, rpi.Name)
	} else {
		return other, err
	}
}

// SetDefaultRetentionPolicy sets the default retention policy for a database.
func (s *Store) SetDefaultRetentionPolicy(database, name string) error {
	return s.exec(internal.Command_SetDefaultRetentionPolicyCommand, internal.E_SetDefaultRetentionPolicyCommand_Command,
		&internal.SetDefaultRetentionPolicyCommand{
			Database: proto.String(database),
			Name:     proto.String(name),
		},
	)
}

// UpdateRetentionPolicy updates an existing retention policy.
func (s *Store) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) error {
	var newName *string
	if rpu.Name != nil {
		newName = rpu.Name
	}

	var duration *int64
	if rpu.Duration != nil {
		value := int64(*rpu.Duration)
		duration = &value
	}

	var replicaN *uint32
	if rpu.Duration != nil {
		value := uint32(*rpu.ReplicaN)
		replicaN = &value
	}

	return s.exec(internal.Command_UpdateRetentionPolicyCommand, internal.E_UpdateRetentionPolicyCommand_Command,
		&internal.UpdateRetentionPolicyCommand{
			Database: proto.String(database),
			Name:     proto.String(name),
			NewName:  newName,
			Duration: duration,
			ReplicaN: replicaN,
		},
	)
}

// DropRetentionPolicy removes a policy from a database by name.
func (s *Store) DropRetentionPolicy(database, name string) error {
	return s.exec(internal.Command_DropRetentionPolicyCommand, internal.E_DropRetentionPolicyCommand_Command,
		&internal.DropRetentionPolicyCommand{
			Database: proto.String(database),
			Name:     proto.String(name),
		},
	)
}

// FIX: CreateRetentionPolicyIfNotExists(database string, rp *RetentionPolicyInfo) (*RetentionPolicyInfo, error)

// CreateShardGroup creates a new shard group in a retention policy for a given time.
func (s *Store) CreateShardGroup(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	if err := s.exec(internal.Command_CreateShardGroupCommand, internal.E_CreateShardGroupCommand_Command,
		&internal.CreateShardGroupCommand{
			Database:  proto.String(database),
			Policy:    proto.String(policy),
			Timestamp: proto.Int64(timestamp.UnixNano()),
		},
	); err != nil {
		return nil, err
	}

	return s.ShardGroupByTimestamp(database, policy, timestamp)
}

// CreateShardGroupIfNotExists creates a new shard group if one doesn't already exist.
func (s *Store) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	// Try to find shard group locally first.
	if sgi, err := s.ShardGroupByTimestamp(database, policy, timestamp); err != nil {
		return nil, err
	} else if sgi != nil {
		return sgi, nil
	}

	// Attempt to create database.
	if sgi, err := s.CreateShardGroup(database, policy, timestamp); err == ErrShardGroupExists {
		return s.ShardGroupByTimestamp(database, policy, timestamp)
	} else {
		return sgi, err
	}
}

// DeleteShardGroup removes an existing shard group from a policy by ID.
func (s *Store) DeleteShardGroup(database, policy string, id uint64) error {
	return s.exec(internal.Command_DeleteShardGroupCommand, internal.E_DeleteShardGroupCommand_Command,
		&internal.DeleteShardGroupCommand{
			Database:     proto.String(database),
			Policy:       proto.String(policy),
			ShardGroupID: proto.Uint64(id),
		},
	)
}

// ShardGroups returns a list of all shard groups for a policy by timestamp.
func (s *Store) ShardGroups(database, policy string) (a []ShardGroupInfo, err error) {
	err = s.read(func(data *Data) error {
		a, err = data.ShardGroups(database, policy)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// ShardGroupByTimestamp returns a shard group for a policy by timestamp.
func (s *Store) ShardGroupByTimestamp(database, policy string, timestamp time.Time) (sgi *ShardGroupInfo, err error) {
	err = s.read(func(data *Data) error {
		sgi, err = data.ShardGroupByTimestamp(database, policy, timestamp)
		if err != nil {
			return err
		} else if sgi == nil {
			return errInvalidate
		}
		return nil
	})
	return
}

// CreateContinuousQuery creates a new continuous query on the store.
func (s *Store) CreateContinuousQuery(database, name, query string) error {
	return s.exec(internal.Command_CreateContinuousQueryCommand, internal.E_CreateContinuousQueryCommand_Command,
		&internal.CreateContinuousQueryCommand{
			Database: proto.String(database),
			Name:     proto.String(name),
			Query:    proto.String(query),
		},
	)
}

// DropContinuousQuery removes a continuous query from the store.
func (s *Store) DropContinuousQuery(database, name string) error {
	return s.exec(internal.Command_DropContinuousQueryCommand, internal.E_DropContinuousQueryCommand_Command,
		&internal.DropContinuousQueryCommand{
			Database: proto.String(database),
			Name:     proto.String(name),
		},
	)
}

// User returns a user by name.
func (s *Store) User(name string) (ui *UserInfo, err error) {
	err = s.read(func(data *Data) error {
		ui = data.User(name)
		if ui == nil {
			return errInvalidate
		}
		return nil
	})
	return
}

// Users returns a list of all users.
func (s *Store) Users() (a []UserInfo, err error) {
	err = s.read(func(data *Data) error {
		a = data.Users
		return nil
	})
	return
}

// AdminUserExists returns true if an admin user exists on the system.
func (s *Store) AdminUserExists() (exists bool, err error) {
	err = s.read(func(data *Data) error {
		for i := range data.Users {
			if data.Users[i].Admin {
				exists = true
				break
			}
		}
		return nil
	})
	return
}

// Authenticate retrieves a user with a matching username and password.
func (s *Store) Authenticate(username, password string) (ui *UserInfo, err error) {
	err = s.read(func(data *Data) error {
		// Find user.
		u := data.User(username)
		if u == nil {
			return ErrUserNotFound
		}

		// Compare password with user hash.
		if err := bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte(password)); err != nil {
			return err
		}

		ui = u
		return nil
	})
	return
}

// CreateUser creates a new user in the store.
func (s *Store) CreateUser(name, password string, admin bool) (*UserInfo, error) {
	// Hash the password before serializing it.
	hash, err := HashPassword(password)
	if err != nil {
		return nil, err
	}

	// Serialize command and send it to the leader.
	if err := s.exec(internal.Command_CreateUserCommand, internal.E_CreateUserCommand_Command,
		&internal.CreateUserCommand{
			Name:  proto.String(name),
			Hash:  proto.String(string(hash)),
			Admin: proto.Bool(admin),
		},
	); err != nil {
		return nil, err
	}
	return s.User(name)
}

// DropUser removes a user from the metastore by name.
func (s *Store) DropUser(name string) error {
	return s.exec(internal.Command_DropUserCommand, internal.E_DropUserCommand_Command,
		&internal.DropUserCommand{
			Name: proto.String(name),
		},
	)
}

// UpdateUser updates an existing user in the store.
func (s *Store) UpdateUser(name, password string) error {
	// Hash the password before serializing it.
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}

	// Serialize command and send it to the leader.
	return s.exec(internal.Command_UpdateUserCommand, internal.E_UpdateUserCommand_Command,
		&internal.UpdateUserCommand{
			Name: proto.String(name),
			Hash: proto.String(string(hash)),
		},
	)
}

// SetPrivilege sets a privilege for a user on a database.
func (s *Store) SetPrivilege(username, database string, p influxql.Privilege) error {
	return s.exec(internal.Command_SetPrivilegeCommand, internal.E_SetPrivilegeCommand_Command,
		&internal.SetPrivilegeCommand{
			Username:  proto.String(username),
			Database:  proto.String(database),
			Privilege: proto.Int32(int32(p)),
		},
	)
}

// read executes a function with the current metadata.
// If an error is returned then the cache is invalidated and retried.
//
// The error returned by the retry is passed through to the original caller
// unless the error is errInvalidate. A nil error is passed through when
// errInvalidate is returned.
func (s *Store) read(fn func(*Data) error) error {
	// First use the cached metadata.
	s.mu.RLock()
	data := s.data
	s.mu.RUnlock()

	// Execute fn against cached data.
	// Return immediately if there was no error.
	if err := fn(data); err == nil {
		return nil
	}

	// If an error occurred then invalidate cache and retry.
	if err := s.invalidate(); err != nil {
		return err
	}

	// Re-read the metadata.
	s.mu.RLock()
	data = s.data
	s.mu.RUnlock()

	// Passthrough error unless it is a cache invalidation.
	if err := fn(data); err != nil && err != errInvalidate {
		return err
	}

	return nil
}

// errInvalidate is returned to read() when the cache should be invalidated
// but an error should not be passed through to the caller.
var errInvalidate = errors.New("invalidate cache")

func (s *Store) invalidate() error {
	return nil // FIX(benbjohnson): Reload cache from the leader.
}

func (s *Store) exec(typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	// Create command.
	cmd := &internal.Command{Type: &typ}
	err := proto.SetExtension(cmd, desc, value)
	assert(err == nil, "proto.SetExtension: %s", err)

	// Marshal to a byte slice.
	b, err := proto.Marshal(cmd)
	assert(err == nil, "proto.Marshal: %s", err)

	// Apply to raft log.
	f := s.raft.Apply(b, 0)
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

// storeFSM represents the finite state machine used by Store to interact with Raft.
type storeFSM Store

func (fsm *storeFSM) Apply(l *raft.Log) interface{} {
	var cmd internal.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		panic(fmt.Errorf("cannot marshal command: %x", l.Data))
	}

	// Lock the store.
	s := (*Store)(fsm)
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.GetType() {
	case internal.Command_CreateNodeCommand:
		return fsm.applyCreateNodeCommand(&cmd)
	case internal.Command_DeleteNodeCommand:
		return fsm.applyDeleteNodeCommand(&cmd)
	case internal.Command_CreateDatabaseCommand:
		return fsm.applyCreateDatabaseCommand(&cmd)
	case internal.Command_DropDatabaseCommand:
		return fsm.applyDropDatabaseCommand(&cmd)
	case internal.Command_CreateRetentionPolicyCommand:
		return fsm.applyCreateRetentionPolicyCommand(&cmd)
	case internal.Command_DropRetentionPolicyCommand:
		return fsm.applyDropRetentionPolicyCommand(&cmd)
	case internal.Command_SetDefaultRetentionPolicyCommand:
		return fsm.applySetDefaultRetentionPolicyCommand(&cmd)
	case internal.Command_UpdateRetentionPolicyCommand:
		return fsm.applyUpdateRetentionPolicyCommand(&cmd)
	case internal.Command_CreateShardGroupCommand:
		return fsm.applyCreateShardGroupCommand(&cmd)
	case internal.Command_DeleteShardGroupCommand:
		return fsm.applyDeleteShardGroupCommand(&cmd)
	case internal.Command_CreateContinuousQueryCommand:
		return fsm.applyCreateContinuousQueryCommand(&cmd)
	case internal.Command_DropContinuousQueryCommand:
		return fsm.applyDropContinuousQueryCommand(&cmd)
	case internal.Command_CreateUserCommand:
		return fsm.applyCreateUserCommand(&cmd)
	case internal.Command_DropUserCommand:
		return fsm.applyDropUserCommand(&cmd)
	case internal.Command_UpdateUserCommand:
		return fsm.applyUpdateUserCommand(&cmd)
	case internal.Command_SetPrivilegeCommand:
		return fsm.applySetPrivilegeCommand(&cmd)
	default:
		panic(fmt.Errorf("cannot apply command: %x", l.Data))
	}
}

func (fsm *storeFSM) applyCreateNodeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateNodeCommand_Command)
	v := ext.(*internal.CreateNodeCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateNode(v.GetHost()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDeleteNodeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DeleteNodeCommand_Command)
	v := ext.(*internal.DeleteNodeCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DeleteNode(v.GetID()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateDatabaseCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateDatabaseCommand_Command)
	v := ext.(*internal.CreateDatabaseCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateDatabase(v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDropDatabaseCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DropDatabaseCommand_Command)
	v := ext.(*internal.DropDatabaseCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DropDatabase(v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateRetentionPolicyCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateRetentionPolicyCommand_Command)
	v := ext.(*internal.CreateRetentionPolicyCommand)
	pb := v.GetRetentionPolicy()

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateRetentionPolicy(v.GetDatabase(),
		&RetentionPolicyInfo{
			Name:               pb.GetName(),
			ReplicaN:           int(pb.GetReplicaN()),
			Duration:           time.Duration(pb.GetDuration()),
			ShardGroupDuration: time.Duration(pb.GetShardGroupDuration()),
		}); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDropRetentionPolicyCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DropRetentionPolicyCommand_Command)
	v := ext.(*internal.DropRetentionPolicyCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DropRetentionPolicy(v.GetDatabase(), v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applySetDefaultRetentionPolicyCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_SetDefaultRetentionPolicyCommand_Command)
	v := ext.(*internal.SetDefaultRetentionPolicyCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.SetDefaultRetentionPolicy(v.GetDatabase(), v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyUpdateRetentionPolicyCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_UpdateRetentionPolicyCommand_Command)
	v := ext.(*internal.UpdateRetentionPolicyCommand)

	// Create update object.
	rpu := RetentionPolicyUpdate{Name: v.NewName}
	if v.Duration != nil {
		value := time.Duration(v.GetDuration())
		rpu.Duration = &value
	}
	if v.ReplicaN != nil {
		value := int(v.GetReplicaN())
		rpu.ReplicaN = &value
	}

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.UpdateRetentionPolicy(v.GetDatabase(), v.GetName(), &rpu); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateShardGroupCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateShardGroupCommand_Command)
	v := ext.(*internal.CreateShardGroupCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateShardGroup(v.GetDatabase(), v.GetPolicy(), time.Unix(0, v.GetTimestamp())); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDeleteShardGroupCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DeleteShardGroupCommand_Command)
	v := ext.(*internal.DeleteShardGroupCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DeleteShardGroup(v.GetDatabase(), v.GetPolicy(), v.GetShardGroupID()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateContinuousQueryCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateContinuousQueryCommand_Command)
	v := ext.(*internal.CreateContinuousQueryCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateContinuousQuery(v.GetDatabase(), v.GetName(), v.GetQuery()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDropContinuousQueryCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DropContinuousQueryCommand_Command)
	v := ext.(*internal.DropContinuousQueryCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DropContinuousQuery(v.GetDatabase(), v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateUserCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateUserCommand_Command)
	v := ext.(*internal.CreateUserCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateUser(v.GetName(), v.GetHash(), v.GetAdmin()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDropUserCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DropUserCommand_Command)
	v := ext.(*internal.DropUserCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DropUser(v.GetName()); err != nil {
		return err
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) applyUpdateUserCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_UpdateUserCommand_Command)
	v := ext.(*internal.UpdateUserCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.UpdateUser(v.GetName(), v.GetHash()); err != nil {
		return err
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) applySetPrivilegeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_SetPrivilegeCommand_Command)
	v := ext.(*internal.SetPrivilegeCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.SetPrivilege(v.GetUsername(), v.GetDatabase(), influxql.Privilege(v.GetPrivilege())); err != nil {
		return err
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) Snapshot() (raft.FSMSnapshot, error) {
	s := (*Store)(fsm)
	s.mu.Lock()
	defer s.mu.Unlock()

	return &storeFSMSnapshot{Data: (*Store)(fsm).data}, nil
}

func (fsm *storeFSM) Restore(r io.ReadCloser) error {
	// Read all bytes.
	// b, err := ioutil.ReadAll(r)
	// if err != nil {
	// 	return err
	// }

	// TODO: Decode metadata.
	// TODO: Set metadata on store.

	return nil
}

type storeFSMSnapshot struct {
	Data *Data
}

func (s *storeFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// TODO: Encode data.
	// TODO: sink.Write(p)
	// TODO: sink.Close()
	panic("not implemented yet")
}

// Release is invoked when we are finished with the snapshot
func (s *storeFSMSnapshot) Release() {}

// RetentionPolicyUpdate represents retention policy fields to be updated.
type RetentionPolicyUpdate struct {
	Name     *string
	Duration *time.Duration
	ReplicaN *int
}

func (rpu *RetentionPolicyUpdate) SetName(v string)            { rpu.Name = &v }
func (rpu *RetentionPolicyUpdate) SetDuration(v time.Duration) { rpu.Duration = &v }
func (rpu *RetentionPolicyUpdate) SetReplicaN(v int)           { rpu.ReplicaN = &v }

// BcryptCost is the cost associated with generating password with Bcrypt.
// This setting is lowered during testing to improve test suite performance.
var BcryptCost = 10

// HashPassword generates a cryptographically secure hash for password.
// Returns an error if the password is invalid or a hash cannot be generated.
func HashPassword(password string) ([]byte, error) {
	// The second arg is the cost of the hashing, higher is slower but makes
	// it harder to brute force, since it will be really slow and impractical
	return bcrypt.GenerateFromPassword([]byte(password), BcryptCost)
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
