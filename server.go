package influxdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/messaging"
)

const (
	// DefaultRootPassword is the password initially set for the root user.
	// It is also used when reseting the root user's password.
	DefaultRootPassword = "root"

	// DefaultRetentionPolicyName is the name of a databases's default shard space.
	DefaultRetentionPolicyName = "default"

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
	// broadcast messages
	createDatabaseMessageType            = messaging.MessageType(0x00)
	deleteDatabaseMessageType            = messaging.MessageType(0x01)
	createRetentionPolicyMessageType     = messaging.MessageType(0x02)
	deleteRetentionPolicyMessageType     = messaging.MessageType(0x03)
	createClusterAdminMessageType        = messaging.MessageType(0x04)
	deleteClusterAdminMessageType        = messaging.MessageType(0x05)
	clusterAdminSetPasswordMessageType   = messaging.MessageType(0x06)
	createDBUserMessageType              = messaging.MessageType(0x07)
	deleteDBUserMessageType              = messaging.MessageType(0x08)
	dbUserSetPasswordMessageType         = messaging.MessageType(0x09)
	createShardIfNotExistsMessageType    = messaging.MessageType(0x0a)
	setDefaultRetentionPolicyMessageType = messaging.MessageType(0x0b)

	// per-topic messages
	writeSeriesMessageType = messaging.MessageType(0x80)
)

// Server represents a collection of metadata and raw metric data.
type Server struct {
	mu   sync.RWMutex
	path string
	done chan struct{} // goroutine close notification

	client MessagingClient  // broker client
	index  uint64           // highest broadcast index seen
	errors map[uint64]error // message errors

	meta *metastore // metadata store

	databases map[string]*Database     // databases by name
	admins    map[string]*ClusterAdmin // admins by name
}

// NewServer returns a new instance of Server.
// The server requires a client to the messaging broker to be passed in.
func NewServer(client MessagingClient) *Server {
	assert(client != nil, "messaging client required")
	return &Server{
		client:    client,
		meta:      &metastore{},
		databases: make(map[string]*Database),
		admins:    make(map[string]*ClusterAdmin),
		errors:    make(map[uint64]error),
	}
}

// Path returns the path used when opening the server.
// Returns an empty string when the server is closed.
func (s *Server) Path() string { return s.path }

// shardPath returns the path for a shard.
func (s *Server) shardPath(id uint64) string {
	if s.path == "" {
		return ""
	}
	return filepath.Join(s.path, "shards", strconv.FormatUint(id, 10))
}

// Open initializes the server from a given path.
func (s *Server) Open(path string) error {
	// Ensure the server isn't already open and there's a path provided.
	if s.opened() {
		return ErrServerOpen
	} else if path == "" {
		return ErrPathRequired
	}

	// Create required directories.
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, "shards"), 0700); err != nil {
		return err
	}

	// Open metadata store.
	if err := s.meta.open(filepath.Join(path, "meta")); err != nil {
		return fmt.Errorf("meta: %s", err)
	}

	// Load state from metastore.
	if err := s.load(); err != nil {
		return fmt.Errorf("load: %s", err)
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

	// Close metastore.
	_ = s.meta.close()

	// Remove path.
	s.path = ""

	return nil
}

// load reads the state of the server from the metastore.
func (s *Server) load() error {
	return s.meta.view(func(tx *metatx) error {
		// Load databases.
		s.databases = make(map[string]*Database)
		for _, db := range tx.databases() {
			db.server = s
			s.databases[db.name] = db
		}

		// Load cluster admins.
		s.admins = make(map[string]*ClusterAdmin)
		for _, u := range tx.clusterAdmins() {
			s.admins[u.Name] = u
		}

		return nil
	})
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
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.databases[c.Name] != nil {
		return ErrDatabaseExists
	}

	// Create database entry.
	db := newDatabase(s)
	db.name = c.Name

	// Persist to metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	// Add to databases on server.
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
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.databases[c.Name] == nil {
		return ErrDatabaseNotFound
	}

	// Remove from metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.deleteDatabase(c.Name)
	})

	// Delete the database entry.
	delete(s.databases, c.Name)
	return nil
}

type deleteDatabaseCommand struct {
	Name string `json:"name"`
}

func (s *Server) applyCreateShardIfNotExists(m *messaging.Message) error {
	var c createShardIfNotExistsSpaceCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	// Check if a matching shard already exists.
	if err, ok := db.applyCreateShardIfNotExists(m.Index, c.Space, c.Timestamp); err != nil {
		return err
	} else if !ok {
		return nil
	}

	// Persist to metastore if a shard was created.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

type createShardIfNotExistsSpaceCommand struct {
	Database  string    `json:"name"`
	Space     string    `json:"space"`
	Timestamp time.Time `json:"timestamp"`
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
	mustUnmarshalJSON(m.Data, &c)

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
	u := &ClusterAdmin{
		CommonUser: CommonUser{
			Name: c.Username,
			Hash: string(hash),
		},
	}

	// Persist to metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveClusterAdmin(u)
	})

	s.admins[u.Name] = u
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
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate admin.
	if c.Username == "" {
		return ErrUsernameRequired
	} else if s.admins[c.Username] == nil {
		return ErrClusterAdminNotFound
	}

	// Remove from metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.deleteClusterAdmin(c.Username)
	})

	// Delete the cluster admin.
	delete(s.admins, c.Username)
	return nil
}

type deleteClusterAdminCommand struct {
	Username string `json:"username"`
}

func (s *Server) applyDBUserSetPassword(m *messaging.Message) error {
	var c dbUserSetPasswordCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	// Update password change in database.
	if err := db.applyChangePassword(c.Username, c.Password); err != nil {
		return err
	}

	// Persist to metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

type dbUserSetPasswordCommand struct {
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Server) applyCreateDBUser(m *messaging.Message) error {
	var c createDBUserCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	if err := db.applyCreateUser(c.Username, c.Password, c.Permissions); err != nil {
		return err
	}

	// Persist to metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

type createDBUserCommand struct {
	Database    string   `json:"database"`
	Username    string   `json:"username"`
	Password    string   `json:"password"`
	Permissions []string `json:"permissions"`
}

func (s *Server) applyDeleteDBUser(m *messaging.Message) error {
	var c deleteDBUserCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	// Remove user from database.
	if err := db.applyDeleteUser(c.Username); err != nil {
		return err
	}

	// Persist to metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

type deleteDBUserCommand struct {
	Database string `json:"database"`
	Username string `json:"username"`
}

func (s *Server) applyCreateRetentionPolicy(m *messaging.Message) error {
	var c createRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	if err := db.applyCreateRetentionPolicy(c.Name, c.Duration, c.ReplicaN, c.SplitN); err != nil {
		return err
	}

	// Persist to metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

type createRetentionPolicyCommand struct {
	Database string        `json:"database"`
	Name     string        `json:"name"`
	Duration time.Duration `json:"duration"`
	ReplicaN uint32        `json:"replicaN"`
	SplitN   uint32        `json:"splitN"`
}

func (s *Server) applyDeleteRetentionPolicy(m *messaging.Message) error {
	var c deleteRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	// Remove shard space from database.
	if err := db.applyDeleteRetentionPolicy(c.Name); err != nil {
		return err
	}

	// Persist to metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

type deleteRetentionPolicyCommand struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

func (s *Server) applySetDefaultRetentionPolicy(m *messaging.Message) error {
	var c setDefaultRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	if err := db.applySetDefaultRetentionPolicy(c.Name); err != nil {
		return err
	}

	// Persist to metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

type setDefaultRetentionPolicyCommand struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

/* TEMPORARILY REMOVED FOR PROTOBUFS.
func (s *Server) applyWriteSeries(m *messaging.Message) error {
	req := &protocol.WriteSeriesRequest{}
	if err := proto.Unmarshal(m.Data, req); err != nil {
		panic("unmarshal request: " + err.Error())
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[req.GetDatabase()]
	if db == nil {
		return ErrDatabaseNotFound
	}

	if err := db.applyWriteSeries(id, t, values); err != nil {
		return err
	}

	return nil
}
*/

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
		case createRetentionPolicyMessageType:
			err = s.applyCreateRetentionPolicy(m)
		case deleteRetentionPolicyMessageType:
			err = s.applyDeleteRetentionPolicy(m)
		case createShardIfNotExistsMessageType:
			err = s.applyCreateShardIfNotExists(m)
		case setDefaultRetentionPolicyMessageType:
			err = s.applySetDefaultRetentionPolicy(m)
		case writeSeriesMessageType:
			/* TEMPORARILY REMOVED FOR PROTOBUFS.
			err = s.applyWriteSeries(m)
			*/
		}

		// Sync high water mark and errors.
		s.mu.Lock()
		s.index = m.Index
		if err != nil {
			s.errors[m.Index] = err
		}
		s.mu.Unlock()
	}
}

// MessagingClient represents the client used to receive messages from brokers.
type MessagingClient interface {
	// Publishes a message to the broker.
	Publish(m *messaging.Message) (index uint64, err error)

	// The streaming channel for all subscribed messages.
	C() <-chan *messaging.Message
}

// metastore represents the low-level data store for metadata.
type metastore struct {
	db *bolt.DB
}

// open initializes the metastore.
func (m *metastore) open(path string) error {
	// Open the bolt-backed database.
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	m.db = db

	// Initialize the metastore.
	if err := m.init(); err != nil {
		return err
	}

	return nil
}

// close closes the store.
func (m *metastore) close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// init initializes the metastore to ensure all top-level buckets are created.
func (m *metastore) init() error {
	return m.db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("Databases"))
		_, _ = tx.CreateBucketIfNotExists([]byte("Series"))
		_, _ = tx.CreateBucketIfNotExists([]byte("ClusterAdmins"))
		return nil
	})
}

// view executes a function in the context of a read-only transaction.
func (m *metastore) view(fn func(*metatx) error) error {
	return m.db.View(func(tx *bolt.Tx) error { return fn(&metatx{tx}) })
}

// update executes a function in the context of a read-write transaction.
func (m *metastore) update(fn func(*metatx) error) error {
	return m.db.Update(func(tx *bolt.Tx) error { return fn(&metatx{tx}) })
}

// mustUpdate executes a function in the context of a read-write transaction.
// Panics if update returns an error.
func (m *metastore) mustUpdate(fn func(*metatx) error) {
	if err := m.update(fn); err != nil {
		panic("metastore update: " + err.Error())
	}
}

// metatx represents a metastore transaction.
type metatx struct {
	*bolt.Tx
}

// database returns a database from the metastore by name.
func (tx *metatx) database(name string) (db *Database) {
	if v := tx.Bucket([]byte("Databases")).Get([]byte(name)); v != nil {
		mustUnmarshalJSON(v, &db)
	}
	return
}

// databases returns a list of all databases from the metastore.
func (tx *metatx) databases() (a []*Database) {
	c := tx.Bucket([]byte("Databases")).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		db := newDatabase(nil)
		mustUnmarshalJSON(v, &db)
		a = append(a, db)
	}
	return
}

// saveDatabase persists a database to the metastore.
func (tx *metatx) saveDatabase(db *Database) error {
	return tx.Bucket([]byte("Databases")).Put([]byte(db.name), mustMarshalJSON(db))
}

// deleteDatabase removes database from the metastore.
func (tx *metatx) deleteDatabase(name string) error {
	return tx.Bucket([]byte("Databases")).Delete([]byte(name))
}

// series returns a series by database and name.
func (tx *metatx) series(database, name string) (s *Series) {
	b := tx.Bucket([]byte("Series")).Bucket([]byte(database))
	if b == nil {
		return nil
	}
	if v := b.Get([]byte(name)); v != nil {
		mustUnmarshalJSON(v, &s)
	}
	return
}

// saveSeries persists a series to the metastore.
func (tx *metatx) saveSeries(database string, s *Series) error {
	b, err := tx.Bucket([]byte("Series")).CreateBucketIfNotExists([]byte(database))
	if err != nil {
		return err
	}
	return b.Put([]byte(s.Name), mustMarshalJSON(s))
}

// deleteSeries removes a series from the metastore.
func (tx *metatx) deleteSeries(database, name string) error {
	b := tx.Bucket([]byte("Series")).Bucket([]byte(database))
	if b == nil {
		return nil
	}
	return b.Delete([]byte(name))
}

// clusterAdmin returns a cluster admin from the metastore by name.
func (tx *metatx) clusterAdmin(name string) (u *ClusterAdmin) {
	if v := tx.Bucket([]byte("ClusterAdmins")).Get([]byte(name)); v != nil {
		mustUnmarshalJSON(v, &u)
	}
	return
}

// clusterAdmins returns a list of all cluster admins from the metastore.
func (tx *metatx) clusterAdmins() (a []*ClusterAdmin) {
	c := tx.Bucket([]byte("ClusterAdmins")).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		u := &ClusterAdmin{}
		mustUnmarshalJSON(v, &u)
		a = append(a, u)
	}
	return
}

// saveClusterAdmin persists a cluster admin to the metastore.
func (tx *metatx) saveClusterAdmin(u *ClusterAdmin) error {
	return tx.Bucket([]byte("ClusterAdmins")).Put([]byte(u.Name), mustMarshalJSON(u))
}

// deleteClusterAdmin removes the cluster admin from the metastore.
func (tx *metatx) deleteClusterAdmin(name string) error {
	return tx.Bucket([]byte("ClusterAdmins")).Delete([]byte(name))
}

// ContinuousQuery represents a query that exists on the server and processes
// each incoming event.
type ContinuousQuery struct {
	ID    uint32
	Query string
	// TODO: ParsedQuery *parser.SelectQuery
}

// mustMarshal encodes a value to JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid marshal will cause corruption and a panic is appropriate.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal: " + err.Error())
	}
	return b
}

// mustUnmarshalJSON decodes a value from JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid unmarshal will cause corruption and a panic is appropriate.
func mustUnmarshalJSON(b []byte, v interface{}) {
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
