package influxdb

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/influxdb/influxdb/messaging"
	"github.com/influxdb/influxdb/protocol"
)

const (
	// DefaultRootPassword is the password initially set for the root user.
	// It is also used when reseting the root user's password.
	DefaultRootPassword = "root"

	// DefaultShardSpaceName is the name of a databases's default shard space.
	DefaultShardSpaceName = "default"

	// DefaultSplitN represents the number of partitions a shard is split into.
	DefaultSplitN = 1

	// DefaultReplicaN represents the number of replicas data is written to.
	DefaultReplicaN = 1

	// DefaultShardDuration is the time period held by a shard.
	DefaultShardDuration = 7 * (24 * time.Hour)

	// DefaultShardRetention is the length of time before a shard is dropped.
	DefaultShardRetention = time.Duration(0)
)

const (
	createDatabaseMessageType          = messaging.MessageType(0x00)
	deleteDatabaseMessageType          = messaging.MessageType(0x01)
	createShardSpaceMessageType        = messaging.MessageType(0x02)
	deleteShardSpaceMessageType        = messaging.MessageType(0x03)
	createClusterAdminMessageType      = messaging.MessageType(0x04)
	deleteClusterAdminMessageType      = messaging.MessageType(0x05)
	clusterAdminSetPasswordMessageType = messaging.MessageType(0x06)
	createDBUserMessageType            = messaging.MessageType(0x07)
	deleteDBUserMessageType            = messaging.MessageType(0x08)
	dbUserSetPasswordMessageType       = messaging.MessageType(0x09)
)

// Server represents a collection of metadata and raw metric data.
type Server struct {
	mu   sync.RWMutex
	path string
	done chan struct{} // goroutine close notification

	client MessagingClient  // broker client
	index  uint64           // highest broadcast index seen
	errors map[uint64]error // message errors

	databases map[string]*Database
	admins    map[string]*ClusterAdmin
}

// NewServer returns a new instance of Server.
// The server requires a client to the messaging broker to be passed in.
func NewServer(client MessagingClient) *Server {
	assert(client != nil, "messaging client required")
	return &Server{
		client:    client,
		databases: make(map[string]*Database),
		admins:    make(map[string]*ClusterAdmin),
		errors:    make(map[uint64]error),
	}
}

// Path returns the path used when opening the server.
// Returns an empty string when the server is closed.
func (s *Server) Path() string { return s.path }

// Open initializes the server from a given path.
func (s *Server) Open(path string) error {
	// Ensure the server isn't already open and there's a path provided.
	if s.opened() {
		return ErrServerOpen
	} else if path == "" {
		return ErrPathRequired
	}

	// Set the server path.
	s.path = path

	// Start goroutine to read messages from the broker.
	s.done = make(chan struct{}, 0)
	go s.processor(s.done)

	return nil
}

// opened returns true when the server is open.
func (s *Server) opened() bool { return s.path != "" }

// Close shuts down the server.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.opened() {
		return ErrServerClosed
	}

	// Close notification.
	close(s.done)
	s.done = nil

	// Remove path.
	s.path = ""

	return nil
}

// broadcast encodes a message as JSON and send it to the broker's broadcast topic.
// This function waits until the message has been processed by the server.
// Returns the broker log index of the message or an error.
func (s *Server) broadcast(typ messaging.MessageType, c interface{}) (uint64, error) {
	// Encode the command.
	data, err := json.Marshal(c)
	if err != nil {
		return 0, err
	}

	// Publish the message.
	m := &messaging.Message{
		Type:    typ,
		TopicID: messaging.BroadcastTopicID,
		Data:    data,
	}
	index, err := s.client.Publish(m)
	if err != nil {
		return 0, err
	}

	// Wait for the server to receive the message.
	err = s.sync(index)

	return index, err
}

// sync blocks until a given index (or a higher index) has been seen.
// Returns any error associated with the command.
func (s *Server) sync(index uint64) error {
	for {
		// Check if index has occurred. If so, retrieve the error and return.
		s.mu.RLock()
		if s.index >= index {
			err, ok := s.errors[index]
			if ok {
				delete(s.errors, index)
			}
			s.mu.RUnlock()
			return err
		}
		s.mu.RUnlock()

		// Otherwise wait momentarily and check again.
		time.Sleep(1 * time.Millisecond)
	}
}

// Database creates a new database.
func (s *Server) Database(name string) *Database {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.databases[name]
}

// Databases returns a list of all databases, sorted by name.
func (s *Server) Databases() []*Database {
	s.mu.Lock()
	defer s.mu.Unlock()
	var a databases
	for _, db := range s.databases {
		a = append(a, db)
	}
	sort.Sort(a)
	return a
}

// CreateDatabase creates a new database.
func (s *Server) CreateDatabase(name string) error {
	c := &createDatabaseCommand{Name: name}
	_, err := s.broadcast(createDatabaseMessageType, c)
	return err
}

func (s *Server) applyCreateDatabase(m *messaging.Message) error {
	var c createDatabaseCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.databases[c.Name] != nil {
		return ErrDatabaseExists
	}

	// Create database entry.
	db := newDatabase(s)
	db.name = c.Name
	s.databases[c.Name] = db
	return nil
}

type createDatabaseCommand struct {
	Name string `json:"name"`
}

// DeleteDatabase deletes an existing database.
func (s *Server) DeleteDatabase(name string) error {
	c := &deleteDatabaseCommand{Name: name}
	_, err := s.broadcast(deleteDatabaseMessageType, c)
	return err
}

func (s *Server) applyDeleteDatabase(m *messaging.Message) error {
	var c deleteDatabaseCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.databases[c.Name] == nil {
		return ErrDatabaseNotFound
	}

	// Delete the database entry.
	delete(s.databases, c.Name)
	return nil
}

type deleteDatabaseCommand struct {
	Name string `json:"name"`
}

// ClusterAdmin returns an admin by name.
// Returns nil if the admin does not exist.
func (s *Server) ClusterAdmin(name string) *ClusterAdmin {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admins[name]
}

// ClusterAdmins returns a list of all cluster admins, sorted by name.
func (s *Server) ClusterAdmins() []*ClusterAdmin {
	s.mu.Lock()
	defer s.mu.Unlock()
	var a clusterAdmins
	for _, u := range s.admins {
		a = append(a, u)
	}
	sort.Sort(a)
	return a
}

// CreateClusterAdmin creates a cluster admin on the server.
func (s *Server) CreateClusterAdmin(username, password string) error {
	c := &createClusterAdminCommand{Username: username, Password: password}
	_, err := s.broadcast(createClusterAdminMessageType, c)
	return err
}

func (s *Server) applyCreateClusterAdmin(m *messaging.Message) error {
	var c createClusterAdminCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate admin.
	if c.Username == "" {
		return ErrUsernameRequired
	} else if s.admins[c.Username] != nil {
		return ErrClusterAdminExists
	}

	// Generate the hash of the password.
	hash, err := HashPassword(c.Password)
	if err != nil {
		return err
	}

	// Create the cluster admin.
	s.admins[c.Username] = &ClusterAdmin{
		CommonUser: CommonUser{
			Name: c.Username,
			Hash: string(hash),
		},
	}

	return nil
}

type createClusterAdminCommand struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// DeleteClusterAdmin removes a cluster admin from the server.
func (s *Server) DeleteClusterAdmin(username string) error {
	c := &deleteClusterAdminCommand{Username: username}
	_, err := s.broadcast(deleteClusterAdminMessageType, c)
	return err
}

func (s *Server) applyDeleteClusterAdmin(m *messaging.Message) error {
	var c deleteClusterAdminCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate admin.
	if c.Username == "" {
		return ErrUsernameRequired
	} else if s.admins[c.Username] == nil {
		return ErrClusterAdminNotFound
	}

	// Delete the cluster admin.
	delete(s.admins, c.Username)
	return nil
}

type deleteClusterAdminCommand struct {
	Username string `json:"username"`
}

func (s *Server) applyDBUserSetPassword(m *messaging.Message) error {
	var c dbUserSetPasswordCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	return db.changePassword(c.Username, c.Password)
}

func (s *Server) applyCreateDBUser(m *messaging.Message) error {
	var c createDBUserCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	return db.applyCreateUser(c.Username, c.Password, c.Permissions)
}

type createDBUserCommand struct {
	Database    string   `json:"database"`
	Username    string   `json:"username"`
	Password    string   `json:"password"`
	Permissions []string `json:"permissions"`
}

func (s *Server) applyDeleteDBUser(m *messaging.Message) error {
	var c deleteDBUserCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	return db.applyDeleteUser(c.Username)
}

type deleteDBUserCommand struct {
	Database string `json:"database"`
	Username string `json:"username"`
}

type dbUserSetPasswordCommand struct {
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Server) applyCreateShardSpace(m *messaging.Message) error {
	var c createShardSpaceCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	return db.applyCreateShardSpace(c.Name, c.Regex, c.Retention, c.Duration, c.ReplicaN, c.SplitN)
}

type createShardSpaceCommand struct {
	Database  string        `json:"database"`
	Name      string        `json:"name"`
	Regex     string        `json:"regex"`
	Retention time.Duration `json:"retention"`
	Duration  time.Duration `json:"duration"`
	ReplicaN  uint32        `json:"replicaN"`
	SplitN    uint32        `json:"splitN"`
}

func (s *Server) applyDeleteShardSpace(m *messaging.Message) error {
	var c deleteShardSpaceCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	return db.applyDeleteShardSpace(c.Name)
}

type deleteShardSpaceCommand struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

// WriteSeries writes series data to the broker.
func (s *Server) WriteSeries(u *ClusterAdmin, database string, series *protocol.Series) error {
	// TODO:
	return nil
}

// processor runs in a separate goroutine and processes all incoming broker messages.
func (s *Server) processor(done chan struct{}) {
	client := s.client
	for {
		// Read incoming message.
		var m *messaging.Message
		select {
		case <-done:
			return
		case m = <-client.C():
		}

		// Process message.
		var err error
		switch m.Type {
		case createDatabaseMessageType:
			err = s.applyCreateDatabase(m)
		case deleteDatabaseMessageType:
			err = s.applyDeleteDatabase(m)
		case createClusterAdminMessageType:
			err = s.applyCreateClusterAdmin(m)
		case deleteClusterAdminMessageType:
			err = s.applyDeleteClusterAdmin(m)
		case createDBUserMessageType:
			err = s.applyCreateDBUser(m)
		case deleteDBUserMessageType:
			err = s.applyDeleteDBUser(m)
		case dbUserSetPasswordMessageType:
			err = s.applyDBUserSetPassword(m)
		case createShardSpaceMessageType:
			err = s.applyCreateShardSpace(m)
		case deleteShardSpaceMessageType:
			err = s.applyDeleteShardSpace(m)
		}

		// Sync high water mark and errors for broadcast topic.
		if m.TopicID == messaging.BroadcastTopicID {
			s.mu.Lock()
			s.index = m.Index
			if err != nil {
				s.errors[m.Index] = err
			}
			s.mu.Unlock()
		}
	}
}

// MessagingClient represents the client used to receive messages from brokers.
type MessagingClient interface {
	// Publishes a message to the broker.
	Publish(m *messaging.Message) (index uint64, err error)

	// The streaming channel for all subscribed messages.
	C() <-chan *messaging.Message
}

// mustMarshal encodes a value to JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid marshal will cause corruption and a panic is appropriate.
func mustMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal: " + err.Error())
	}
	return b
}

// mustUnmarshal decodes a value from JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid unmarshal will cause corruption and a panic is appropriate.
func mustUnmarshal(b []byte, v interface{}) {
	if err := json.Unmarshal(b, v); err != nil {
		panic("unmarshal: " + err.Error())
	}
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
