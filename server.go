package influxdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"code.google.com/p/go.crypto/bcrypt"
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
	createDatabaseMessageType = messaging.MessageType(0x00)
	deleteDatabaseMessageType = messaging.MessageType(0x01)

	createRetentionPolicyMessageType     = messaging.MessageType(0x10)
	updateRetentionPolicyMessageType     = messaging.MessageType(0x11)
	deleteRetentionPolicyMessageType     = messaging.MessageType(0x12)
	setDefaultRetentionPolicyMessageType = messaging.MessageType(0x13)

	createUserMessageType = messaging.MessageType(0x20)
	updateUserMessageType = messaging.MessageType(0x21)
	deleteUserMessageType = messaging.MessageType(0x22)

	createShardIfNotExistsMessageType = messaging.MessageType(0x30)

	createSeriesIfNotExistsMessageType = messaging.MessageType(0x40)

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

	databases        map[string]*database // databases by name
	databasesByShard map[uint64]*database // databases by shard id
	users            map[string]*User     // user by name
}

// NewServer returns a new instance of Server.
// The server requires a client to the messaging broker to be passed in.
func NewServer(client MessagingClient) *Server {
	assert(client != nil, "messaging client required")
	return &Server{
		client:           client,
		meta:             &metastore{},
		databases:        make(map[string]*database),
		databasesByShard: make(map[uint64]*database),
		users:            make(map[string]*User),
		errors:           make(map[uint64]error),
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
		s.databases = make(map[string]*database)
		for _, db := range tx.databases() {
			s.databases[db.name] = db

			for sh := range db.shards {
				s.databasesByShard[sh] = db
			}
		}

		// Load users.
		s.users = make(map[string]*User)
		for _, u := range tx.users() {
			s.users[u.Name] = u
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

// DatabaseExists returns true if a database exists.
func (s *Server) DatabaseExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.databases[name] != nil
}

// Databases returns a sorted list of all database names.
func (s *Server) Databases() (a []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, db := range s.databases {
		a = append(a, db.name)
	}
	sort.Strings(a)
	return
}

// CreateDatabase creates a new database.
func (s *Server) CreateDatabase(name string) error {
	c := &createDatabaseCommand{Name: name}
	_, err := s.broadcast(createDatabaseMessageType, c)
	return err
}

func (s *Server) applyCreateDatabase(m *messaging.Message) (err error) {
	var c createDatabaseCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.databases[c.Name] != nil {
		return ErrDatabaseExists
	}

	// Create database entry.
	db := newDatabase()
	db.name = c.Name

	// Persist to metastore.
	err = s.meta.mustUpdate(func(tx *metatx) error { return tx.saveDatabase(db) })

	// Add to databases on server.
	s.databases[c.Name] = db

	return
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

func (s *Server) applyDeleteDatabase(m *messaging.Message) (err error) {
	var c deleteDatabaseCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.databases[c.Name] == nil {
		return ErrDatabaseNotFound
	}

	// Remove from metastore.
	err = s.meta.mustUpdate(func(tx *metatx) error { return tx.deleteDatabase(c.Name) })

	// Delete the database entry.
	delete(s.databases, c.Name)
	return
}

type deleteDatabaseCommand struct {
	Name string `json:"name"`
}

// shardByTimestamp returns a shard that owns a given timestamp for a database.
func (s *Server) shardByTimestamp(database, policy string, id uint32, timestamp time.Time) (*Shard, error) {
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound
	}
	return db.shardByTimestamp(policy, id, timestamp)
}

// Shards returns a list of all shards for a database.
// Returns an error if the database doesn't exist.
func (s *Server) Shards(database string) ([]*Shard, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Lookup database.
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound
	}

	// Retrieve shards from database.
	shards := make([]*Shard, 0, len(db.shards))
	for _, shard := range db.shards {
		shards = append(shards, shard)
	}
	return shards, nil
}

// shardsByTimestamp returns all shards that own a given timestamp for a database.
func (s *Server) shardsByTimestamp(database, policy string, timestamp time.Time) ([]*Shard, error) {
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound
	}
	return db.shardsByTimestamp(policy, timestamp)
}

// CreateShardsIfNotExist creates all the shards for a retention policy for the interval a timestamp falls into.
// Note that multiple shards can be created for each bucket of time.
func (s *Server) CreateShardsIfNotExists(database, policy string, timestamp time.Time) error {
	c := &createShardIfNotExistsCommand{Database: database, Policy: policy, Timestamp: timestamp}
	_, err := s.broadcast(createShardIfNotExistsMessageType, c)
	return err
}

// createShardIfNotExists returns the shard for a given retention policy, series, and timestamp.
// If it doesn't exist, it will create all shards for the given timestamp
func (s *Server) createShardIfNotExists(database, policy string, id uint32, timestamp time.Time) (*Shard, error) {
	// Check if shard exists first.
	sh, err := s.shardByTimestamp(database, policy, id, timestamp)
	if err != nil {
		return nil, err
	} else if sh != nil {
		return sh, nil
	}

	// If the shard doesn't exist then create it.
	if err := s.CreateShardsIfNotExists(database, policy, timestamp); err != nil {
		return nil, err
	}

	// Lookup the shard again.
	return s.shardByTimestamp(database, policy, id, timestamp)
}

func (s *Server) applyCreateShardIfNotExists(m *messaging.Message) (err error) {
	var c createShardIfNotExistsCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	// Validate retention policy.
	rp := db.policies[c.Policy]
	if rp == nil {
		return ErrRetentionPolicyNotFound
	}

	// If we can match to an existing shard date range then just ignore request.
	for _, sh := range rp.Shards {
		if timeBetweenInclusive(c.Timestamp, sh.StartTime, sh.EndTime) {
			return nil
		}
	}

	// If no shards match then create a new one.
	sh := newShard()
	sh.ID = m.Index
	sh.StartTime = c.Timestamp.Truncate(rp.Duration).UTC()
	sh.EndTime = sh.StartTime.Add(rp.Duration).UTC()

	// Open shard.
	if err := sh.open(s.shardPath(sh.ID)); err != nil {
		panic("unable to open shard: " + err.Error())
	}

	// Persist to metastore if a shard was created.
	if err = s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	}); err != nil {
		_ = sh.close()
		return
	}

	// Add to lookups.
	s.databasesByShard[sh.ID] = db
	db.shards[sh.ID] = sh
	rp.Shards = append(rp.Shards, sh)

	return
}

type createShardIfNotExistsCommand struct {
	Database  string    `json:"name"`
	Policy    string    `json:"policy"`
	Timestamp time.Time `json:"timestamp"`
}

// User returns a user by username
// Returns nil if the user does not exist.
func (s *Server) User(name string) *User {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.users[name]
}

// Users returns a list of all users, sorted by name.
func (s *Server) Users() (a []*User) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.users {
		a = append(a, u)
	}
	sort.Sort(users(a))
	return a
}

// CreateUser creates a user on the server.
func (s *Server) CreateUser(username, password string, admin bool) error {
	c := &createUserCommand{Username: username, Password: password, Admin: admin}
	_, err := s.broadcast(createUserMessageType, c)
	return err
}

func (s *Server) applyCreateUser(m *messaging.Message) (err error) {
	var c createUserCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate user.
	if c.Username == "" {
		return ErrUsernameRequired
	} else if s.users[c.Username] != nil {
		return ErrUserExists
	}

	// Generate the hash of the password.
	hash, err := HashPassword(c.Password)
	if err != nil {
		return err
	}

	// Create the user.
	u := &User{
		Name:  c.Username,
		Hash:  string(hash),
		Admin: c.Admin,
	}

	// Persist to metastore.
	err = s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveUser(u)
	})

	s.users[u.Name] = u
	return
}

type createUserCommand struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Admin    bool   `json:"admin,omitempty"`
}

// UpdateUser updates an existing user on the server.
func (s *Server) UpdateUser(username, password string) error {
	c := &updateUserCommand{Username: username, Password: password}
	_, err := s.broadcast(updateUserMessageType, c)
	return err
}

func (s *Server) applyUpdateUser(m *messaging.Message) (err error) {
	var c updateUserCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate command.
	u := s.users[c.Username]
	if u == nil {
		return ErrUserNotFound
	}

	// Update the user's password, if set.
	if c.Password != "" {
		hash, err := HashPassword(c.Password)
		if err != nil {
			return err
		}
		u.Hash = string(hash)
	}

	// Persist to metastore.
	return s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveUser(u)
	})
}

type updateUserCommand struct {
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

// DeleteUser removes a user from the server.
func (s *Server) DeleteUser(username string) error {
	c := &deleteUserCommand{Username: username}
	_, err := s.broadcast(deleteUserMessageType, c)
	return err
}

func (s *Server) applyDeleteUser(m *messaging.Message) error {
	var c deleteUserCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate user.
	if c.Username == "" {
		return ErrUsernameRequired
	} else if s.users[c.Username] == nil {
		return ErrUserNotFound
	}

	// Remove from metastore.
	s.meta.mustUpdate(func(tx *metatx) error {
		return tx.deleteUser(c.Username)
	})

	// Delete the user.
	delete(s.users, c.Username)
	return nil
}

type deleteUserCommand struct {
	Username string `json:"username"`
}

// RetentionPolicy returns a retention policy by name.
// Returns an error if the database doesn't exist.
func (s *Server) RetentionPolicy(database, name string) (*RetentionPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Lookup database.
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound
	}

	return db.policies[name], nil
}

// DefaultRetentionPolicy returns the default retention policy for a database.
// Returns an error if the database doesn't exist.
func (s *Server) DefaultRetentionPolicy(database string) (*RetentionPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Lookup database.
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound
	}

	return db.policies[db.defaultRetentionPolicy], nil
}

// RetentionPolicies returns a list of retention polocies for a database.
// Returns an error if the database doesn't exist.
func (s *Server) RetentionPolicies(database string) ([]*RetentionPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Lookup database.
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound
	}

	// Retrieve the policies.
	a := make([]*RetentionPolicy, 0, len(db.policies))
	for _, p := range db.policies {
		a = append(a, p)
	}
	return a, nil
}

// CreateRetentionPolicy creates a retention policy for a database.
func (s *Server) CreateRetentionPolicy(database string, rp *RetentionPolicy) error {
	c := &createRetentionPolicyCommand{
		Database: database,
		Name:     rp.Name,
		Duration: rp.Duration,
		ReplicaN: rp.ReplicaN,
		SplitN:   rp.SplitN,
	}
	_, err := s.broadcast(createRetentionPolicyMessageType, c)
	return err
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
	} else if c.Name == "" {
		return ErrRetentionPolicyNameRequired
	} else if db.policies[c.Name] != nil {
		return ErrRetentionPolicyExists
	}

	// Add policy to the database.
	db.policies[c.Name] = &RetentionPolicy{
		Name:     c.Name,
		Duration: c.Duration,
		ReplicaN: c.ReplicaN,
		SplitN:   c.SplitN,
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

// UpdateRetentionPolicy updates an existing retention policy on a database.
func (s *Server) UpdateRetentionPolicy(database, name string, rp *RetentionPolicy) error {
	c := &updateRetentionPolicyCommand{Database: database, Name: name, NewName: rp.Name}
	_, err := s.broadcast(updateRetentionPolicyMessageType, c)
	return err
}

type updateRetentionPolicyCommand struct {
	Database string `json:"database"`
	Name     string `json:"name"`
	NewName  string `json:"newName"`
}

func (s *Server) applyUpdateRetentionPolicy(m *messaging.Message) (err error) {
	var c updateRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate command.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	} else if c.Name == "" {
		return ErrRetentionPolicyNameRequired
	}

	// Retrieve the policy.
	p := db.policies[c.Name]
	if db.policies[c.Name] == nil {
		return ErrRetentionPolicyNotFound
	}

	// Update the policy name, if not blank.
	if c.NewName != c.Name && c.NewName != "" {
		delete(db.policies, p.Name)
		p.Name = c.NewName
		db.policies[p.Name] = p
	}

	// Persist to metastore.
	err = s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return
}

// DeleteRetentionPolicy removes a retention policy from a database.
func (s *Server) DeleteRetentionPolicy(database, name string) error {
	c := &deleteRetentionPolicyCommand{Database: database, Name: name}
	_, err := s.broadcast(deleteRetentionPolicyMessageType, c)
	return err
}

func (s *Server) applyDeleteRetentionPolicy(m *messaging.Message) (err error) {
	var c deleteRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	} else if c.Name == "" {
		return ErrRetentionPolicyNameRequired
	} else if db.policies[c.Name] == nil {
		return ErrRetentionPolicyNotFound
	}

	// Remove retention policy.
	delete(db.policies, c.Name)

	// Persist to metastore.
	err = s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return
}

type deleteRetentionPolicyCommand struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

// SetDefaultRetentionPolicy sets the default policy to write data into and query from on a database.
func (s *Server) SetDefaultRetentionPolicy(database, name string) error {
	c := &setDefaultRetentionPolicyCommand{Database: database, Name: name}
	_, err := s.broadcast(setDefaultRetentionPolicyMessageType, c)
	return err
}

func (s *Server) applySetDefaultRetentionPolicy(m *messaging.Message) (err error) {
	var c setDefaultRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate command.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	} else if db.policies[c.Name] == nil {
		return ErrRetentionPolicyNotFound
	}

	// Update default policy.
	db.defaultRetentionPolicy = c.Name

	// Persist to metastore.
	err = s.meta.mustUpdate(func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return
}

type setDefaultRetentionPolicyCommand struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

func (s *Server) applyCreateSeriesIfNotExists(m *messaging.Message) error {
	var c createSeriesIfNotExistsCommand
	mustUnmarshalJSON(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate command.
	db := s.databases[c.Database]
	if db == nil {
		return ErrDatabaseNotFound
	}

	return s.meta.mustUpdate(func(tx *metatx) error {
		return tx.createSeriesIfNotExists(db.name, c.Name, c.Tags)
	})
}

type createSeriesIfNotExistsCommand struct {
	Database string            `json:"database"`
	Name     string            `json:"name"`
	Tags     map[string]string `json:"tags"`
}

// WriteSeries writes series data to the database.
func (s *Server) WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error {
	// Find the id for the series and tagset
	id, err := s.createSeriesIfNotExists(database, name, tags)
	if err != nil {
		return err
	}

	// Now write it into the shard.
	sh, err := s.createShardIfNotExists(database, retentionPolicy, id, timestamp)
	if err != nil {
		return fmt.Errorf("create shard(%s/%d): %s", retentionPolicy, timestamp.Format(time.RFC3339Nano), err)
	}

	// Encode point to a byte slice.
	data, err := marshalPoint(id, timestamp, values)
	if err != nil {
		return err
	}

	// Publish "write series" message on shard's topic to broker.
	m := &messaging.Message{
		Type:    writeSeriesMessageType,
		TopicID: sh.ID,
		Data:    data,
	}

	_, err = s.client.Publish(m)
	return err
}

func (s *Server) applyWriteSeries(m *messaging.Message) error {
	s.mu.RLock()

	// Retrieve the database.
	db := s.databasesByShard[m.TopicID]
	if db == nil {
		s.mu.RUnlock()
		return ErrDatabaseNotFound
	}

	// Retrieve the shard.
	sh := db.shards[m.TopicID]
	if sh == nil {
		s.mu.RUnlock()
		return ErrShardNotFound
	}
	s.mu.RUnlock()

	// TODO: enable some way to specify if the data should be overwritten
	overwrite := true

	// Write to shard.
	return sh.writeSeries(overwrite, m.Data)
}

// seriesID returns the unique id of a series and tagset and a bool indicating if it was found
func (s *Server) seriesID(database, name string, tags map[string]string) (id uint32) {
	s.meta.view(func(tx *metatx) error {
		id, _ = tx.seriesID(database, name, tags)
		return nil
	})
	return
}

func (s *Server) createSeriesIfNotExists(database, name string, tags map[string]string) (uint32, error) {
	// Try to find series locally first.
	if id := s.seriesID(database, name, tags); id != 0 {
		return id, nil
	}

	// If it doesn't exist then create a message and broadcast.
	c := &createSeriesIfNotExistsCommand{Database: database, Name: name, Tags: tags}
	_, err := s.broadcast(createSeriesIfNotExistsMessageType, c)
	if err != nil {
		return 0, err
	}

	// Lookup series again.
	id := s.seriesID(database, name, tags)
	if id == 0 {
		return 0, ErrSeriesNotFound
	}
	return id, nil
}

func (s *Server) Measurements(database string) (a Measurements) {
	s.meta.view(func(tx *metatx) error {
		a = tx.measurements(database)
		return nil
	})
	return
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
		case writeSeriesMessageType:
			err = s.applyWriteSeries(m)
		case createDatabaseMessageType:
			err = s.applyCreateDatabase(m)
		case deleteDatabaseMessageType:
			err = s.applyDeleteDatabase(m)
		case createUserMessageType:
			err = s.applyCreateUser(m)
		case updateUserMessageType:
			err = s.applyUpdateUser(m)
		case deleteUserMessageType:
			err = s.applyDeleteUser(m)
		case createRetentionPolicyMessageType:
			err = s.applyCreateRetentionPolicy(m)
		case updateRetentionPolicyMessageType:
			err = s.applyUpdateRetentionPolicy(m)
		case deleteRetentionPolicyMessageType:
			err = s.applyDeleteRetentionPolicy(m)
		case createShardIfNotExistsMessageType:
			err = s.applyCreateShardIfNotExists(m)
		case setDefaultRetentionPolicyMessageType:
			err = s.applySetDefaultRetentionPolicy(m)
		case createSeriesIfNotExistsMessageType:
			err = s.applyCreateSeriesIfNotExists(m)
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

// database represents a collection of retention policies.
type database struct {
	name string

	policies map[string]*RetentionPolicy // retention policies by name
	shards   map[uint64]*Shard           // shards by id

	defaultRetentionPolicy string
}

// newDatabase returns an instance of database.
func newDatabase() *database {
	return &database{
		policies: make(map[string]*RetentionPolicy),
		shards:   make(map[uint64]*Shard),
	}
}

// shardByTimestamp returns a shard that owns a given timestamp.
func (db *database) shardByTimestamp(policy string, id uint32, timestamp time.Time) (*Shard, error) {
	p := db.policies[policy]
	if p == nil {
		return nil, ErrRetentionPolicyNotFound
	}
	return p.shardByTimestamp(id, timestamp), nil
}

// shardsByTimestamp returns all shards that own a given timestamp.
func (db *database) shardsByTimestamp(policy string, timestamp time.Time) ([]*Shard, error) {
	p := db.policies[policy]
	if p == nil {
		return nil, ErrRetentionPolicyNotFound
	}
	return p.shardsByTimestamp(timestamp), nil
}

// timeBetweenInclusive returns true if t is between min and max, inclusive.
func timeBetweenInclusive(t, min, max time.Time) bool {
	return (t.Equal(min) || t.After(min)) && (t.Equal(max) || t.Before(max))
}

// MarshalJSON encodes a database into a JSON-encoded byte slice.
func (db *database) MarshalJSON() ([]byte, error) {
	// Copy over properties to intermediate type.
	var o databaseJSON
	o.Name = db.name
	o.DefaultRetentionPolicy = db.defaultRetentionPolicy
	for _, rp := range db.policies {
		o.Policies = append(o.Policies, rp)
	}
	for _, s := range db.shards {
		o.Shards = append(o.Shards, s)
	}
	return json.Marshal(&o)
}

// UnmarshalJSON decodes a JSON-encoded byte slice to a database.
func (db *database) UnmarshalJSON(data []byte) error {
	// Decode into intermediate type.
	var o databaseJSON
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Copy over properties from intermediate type.
	db.name = o.Name
	db.defaultRetentionPolicy = o.DefaultRetentionPolicy

	// Copy shard policies.
	db.policies = make(map[string]*RetentionPolicy)
	for _, rp := range o.Policies {
		db.policies[rp.Name] = rp
	}

	// Copy shards.
	db.shards = make(map[uint64]*Shard)
	for _, s := range o.Shards {
		db.shards[s.ID] = s
	}

	return nil
}

// databaseJSON represents the JSON-serialization format for a database.
type databaseJSON struct {
	Name                   string             `json:"name,omitempty"`
	DefaultRetentionPolicy string             `json:"defaultRetentionPolicy,omitempty"`
	Policies               []*RetentionPolicy `json:"policies,omitempty"`
	Shards                 []*Shard           `json:"shards,omitempty"`
}

// Measurement represents a collection of time series in a database
type Measurement struct {
	Name   string    `json:"name,omitempty"`
	Series []*Series `json:"series,omitempty"`
	Fields []*Fields `json:"fields,omitempty"`
}

type Measurements []*Measurement

func (m Measurement) String() string { return string(mustMarshalJSON(m)) }

// Field represents a series field.
type Field struct {
	ID   uint8     `json:"id,omitempty"`
	Name string    `json:"name,omitempty"`
	Type FieldType `json:"field"`
}

type FieldType int

const (
	Int64 FieldType = iota
	Float64
	String
	Boolean
	Binary
)

// Fields represents a list of fields.
type Fields []*Field

// Series belong to a Measurement and represent unique time series in a database
type Series struct {
	ID   uint32
	Tags map[string]string
}

// RetentionPolicy represents a policy for creating new shards in a database and how long they're kept around for.
type RetentionPolicy struct {
	// Unique name within database. Required.
	Name string

	// Length of time to keep data around
	Duration time.Duration

	ReplicaN uint32
	SplitN   uint32

	Shards []*Shard
}

// NewRetentionPolicy returns a new instance of RetentionPolicy with defaults set.
func NewRetentionPolicy(name string) *RetentionPolicy {
	return &RetentionPolicy{
		Name:     name,
		ReplicaN: DefaultReplicaN,
		SplitN:   DefaultSplitN,
		Duration: DefaultShardRetention,
	}
}

// shardByTimestamp returns the shard in the space that owns a given timestamp for a given series id.
// Returns nil if the shard does not exist.
func (rp *RetentionPolicy) shardByTimestamp(id uint32, timestamp time.Time) *Shard {
	shards := rp.shardsByTimestamp(timestamp)
	if len(shards) > 0 {
		return shards[int(id)%len(shards)]
	}
	return nil
}

func (rp *RetentionPolicy) shardsByTimestamp(timestamp time.Time) []*Shard {
	shards := make([]*Shard, 0, rp.SplitN)
	for _, s := range rp.Shards {
		if timeBetweenInclusive(timestamp, s.StartTime, s.EndTime) {
			shards = append(shards, s)
		}
	}
	return shards
}

// MarshalJSON encodes a retention policy to a JSON-encoded byte slice.
func (rp *RetentionPolicy) MarshalJSON() ([]byte, error) {
	return json.Marshal(&retentionPolicyJSON{
		Name:     rp.Name,
		Duration: rp.Duration,
		ReplicaN: rp.ReplicaN,
		SplitN:   rp.SplitN,
	})
}

// UnmarshalJSON decodes a JSON-encoded byte slice to a retention policy.
func (rp *RetentionPolicy) UnmarshalJSON(data []byte) error {
	// Decode into intermediate type.
	var o retentionPolicyJSON
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Copy over properties from intermediate type.
	rp.Name = o.Name
	rp.ReplicaN = o.ReplicaN
	rp.SplitN = o.SplitN
	rp.Duration = o.Duration
	rp.Shards = o.Shards

	return nil
}

// retentionPolicyJSON represents an intermediate struct for JSON marshaling.
type retentionPolicyJSON struct {
	Name     string        `json:"name"`
	ReplicaN uint32        `json:"replicaN,omitempty"`
	SplitN   uint32        `json:"splitN,omitempty"`
	Duration time.Duration `json:"duration,omitempty"`
	Shards   []*Shard      `json:"shards,omitempty"`
}

// RetentionPolicies represents a list of shard policies.
type RetentionPolicies []*RetentionPolicy

// Shards returns a list of all shards for all policies.
func (rps RetentionPolicies) Shards() []*Shard {
	var shards []*Shard
	for _, rp := range rps {
		shards = append(shards, rp.Shards...)
	}
	return shards
}

// BcryptCost is the cost associated with generating password with Bcrypt.
// This setting is lowered during testing to improve test suite performance.
var BcryptCost = 10

// User represents a user account on the system.
// It can be given read/write permissions to individual databases.
type User struct {
	Name  string `json:"name"`
	Hash  string `json:"hash"`
	Admin bool   `json:"admin,omitempty"`
}

// Authenticate returns nil if the password matches the user's password.
// Returns an error if the password was incorrect.
func (u *User) Authenticate(password string) error {
	return bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte(password))
}

// users represents a list of users, sortable by name.
type users []*User

func (p users) Len() int           { return len(p) }
func (p users) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p users) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Matcher struct {
	IsRegex bool
	Name    string
}

func (m *Matcher) Matches(name string) bool {
	if m.IsRegex {
		matches, _ := regexp.MatchString(m.Name, name)
		return matches
	}
	return m.Name == name
}

// HashPassword generates a cryptographically secure hash for password.
// Returns an error if the password is invalid or a hash cannot be generated.
func HashPassword(password string) ([]byte, error) {
	// The second arg is the cost of the hashing, higher is slower but makes
	// it harder to brute force, since it will be really slow and impractical
	return bcrypt.GenerateFromPassword([]byte(password), BcryptCost)
}

// ContinuousQuery represents a query that exists on the server and processes
// each incoming event.
type ContinuousQuery struct {
	ID    uint32
	Query string
	// TODO: ParsedQuery *parser.SelectQuery
}
