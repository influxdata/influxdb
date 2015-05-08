package meta

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/influxdb/influxdb/meta/internal"
)

// Raft configuration.
const (
	raftLogCacheSize      = 512
	raftSnapshotsRetained = 2
	raftTransportMaxPool  = 3
	raftTransportTimeout  = 10 * time.Second
)

// Service represents a raft-backed implementation of Store.
type Service struct {
	data      *Data
	raft      *raft.Raft
	peers     raft.PeerStore
	transport *raft.NetworkTransport
	store     *raftboltdb.BoltStore

	Logger *log.Logger
}

// NewService returns a new instance of Service.
func NewService(cache *Cache) *Service {
	return &Service{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Open opens and initializes the service and its raft log.
func (s *Service) Open(path string) error {
	if err := func() error {
		// Setup raft configuration.
		config := raft.DefaultConfig()
		config.Logger = s.Logger
		// config.EnableSingleNode = true

		// Create a transport layer
		advertise, err := net.ResolveTCPAddr("tcp", "localhost:9000")
		if err != nil {
			return fmt.Errorf("resolve tcp addr: %s", err)
		}
		trans, err := raft.NewTCPTransport(":9000", advertise,
			raftTransportMaxPool, raftTransportTimeout, os.Stderr)
		if err != nil {
			return fmt.Errorf("new tcp transport: %s", err)
		}
		s.transport = trans

		// Create peer storage.
		s.peers = raft.NewJSONPeers(path, trans)

		// Create the log store and stable store.
		store, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
		if err != nil {
			return fmt.Errorf("new bolt store: %s", err)
		}
		s.store = store

		// Create cache for the underlying store.
		cache, err := raft.NewLogCache(raftLogCacheSize, store)
		if err != nil {
			return fmt.Errorf("new log cache: %s", err)
		}

		// Create the snapshot store.
		snapshots, err := raft.NewFileSnapshotStore(path, raftSnapshotsRetained, os.Stderr)
		if err != nil {
			return fmt.Errorf("file snapshot store: %s", err)
		}

		// Create raft log.
		r, err := raft.NewRaft(config, (*ServiceFSM)(s), cache, store, snapshots, s.peers, trans)
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

func (s *Service) Close() error { panic("not yet implemented") }

func (s *Service) close() error {
	// TODO: Close raft.
	// TODO: Close store.
	panic("not implemented yet")
}

// Data returns the current metadata
func (s *Service) Data() *Data { return s.data }

// ServiceHandler represents an HTTP interface to the service.
type ServiceHandler struct {
	Service interface {
		Exec([]byte) error
	}
}

// ServeHTTP serves HTTP requests for the handler.
func (h *ServiceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/commands":
		if r.Method == "POST" {
			h.postCommand(w, r)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

// postCommand reads a command from the body and executes it against the service.
func (h *ServiceHandler) postCommand(w http.ResponseWriter, r *http.Request) {
	// Read request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Execute command.
	if err := h.Service.Exec(body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// ServiceFSM represents the finite state machine used by Service to interact with Raft.
type ServiceFSM Service

func (fsm *ServiceFSM) Apply(l *raft.Log) interface{} {
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

func (fsm *ServiceFSM) applyCreateContinuousQueryCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyDropContinuousQueryCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyCreateNodeCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyDeleteNodeCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyCreateDatabaseCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyDropDatabaseCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyCreateDatabaseIfNotExistsCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyCreateRetentionPolicyCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyCreateRetentionPolicyIfNotExistsCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyDeleteRetentionPolicyCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applySetDefaultRetentionPolicyCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyUpdateRetentionPolicyCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyCreateShardGroupIfNotExistsCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyCreateUserCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyDeleteUserCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyUpdateUserCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applySetPrivilegeCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) applyDeleteShardGroupCommand(cmd *internal.Command) interface{} {
	panic("not yet implemented")
}

func (fsm *ServiceFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &ServiceFSMSnapshot{Data: (*Service)(fsm).Data()}, nil
}

func (fsm *ServiceFSM) Restore(r io.ReadCloser) error {
	// Read all bytes.
	// b, err := ioutil.ReadAll(r)
	// if err != nil {
	// 	return err
	// }

	// TODO: Decode metadata.
	// TODO: Set metadata on service.

	return nil
}

type ServiceFSMSnapshot struct {
	Data *Data
}

func (s *ServiceFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// TODO: Encode data.
	// TODO: sink.Write(p)
	// TODO: sink.Close()
	panic("not implemented yet")
}

// Release is invoked when we are finished with the snapshot
func (s *ServiceFSMSnapshot) Release() {}
