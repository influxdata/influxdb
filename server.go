package influxdb

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/messaging"
	"github.com/influxdb/influxdb/protocol"
)

const (
	createDatabaseMessageType = messaging.MessageType(0x00)
	deleteDatabaseMessageType = messaging.MessageType(0x01)

	createDBUserMessageType   = messaging.MessageType(0x11)
	deleteDBUserMessageType   = messaging.MessageType(0x12)
	changePasswordMessageType = messaging.MessageType(0x13)
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
}

// NewServer returns a new instance of Server.
// The server requires a client to the messaging broker to be passed in.
func NewServer(client MessagingClient) *Server {
	assert(client != nil, "messaging client required")
	return &Server{
		client:    client,
		databases: make(map[string]*Database),
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

// CreateDatabase creates a new database.
func (s *Server) Database(name string) *Database {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.databases[name]
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

func (s *Server) applyCreateDBUser(m *messaging.Message) error {
	var c createDBUserCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate user.
	if c.Username == "" {
		return ErrUsernameRequired
	} else if !isValidName(c.Username) {
		return ErrInvalidUsername
	}

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	} else if db.User(c.Username) != nil {
		return ErrUserExists
	}

	// Generate the hash of the password.
	hash, err := HashPassword(c.Password)
	if err != nil {
		return err
	}

	// Setup matchers.
	rmatcher := []*Matcher{{true, ".*"}}
	wmatcher := []*Matcher{{true, ".*"}}
	if len(c.Permissions) == 2 {
		rmatcher[0].Name = c.Permissions[0]
		wmatcher[0].Name = c.Permissions[1]
	}

	// Create the user.
	u := &DBUser{
		CommonUser: CommonUser{
			Name: c.Username,
			Hash: string(hash),
		},
		DB:       c.Database,
		ReadFrom: rmatcher,
		WriteTo:  wmatcher,
		IsAdmin:  false,
	}
	return db.saveUser(u)
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

	// Validate command.
	if c.Username == "" {
		return ErrUsernameRequired
	}

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	return db.deleteUser(c.Username)
}

type deleteDBUserCommand struct {
	Database string `json:"database"`
	Username string `json:"username"`
}

func (s *Server) applyChangePassword(m *messaging.Message) error {
	var c changePasswordCommand
	mustUnmarshal(m.Data, &c)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate user.
	if c.Username == "" {
		return ErrUsernameRequired
	}

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound
	}

	return db.changePassword(c.Username, c.Password)
}

type changePasswordCommand struct {
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
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
		case createDBUserMessageType:
			err = s.applyCreateDBUser(m)
		case deleteDBUserMessageType:
			err = s.applyDeleteDBUser(m)
		case changePasswordMessageType:
			err = s.applyChangePassword(m)
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

// Database represents a collection of shard spaces.
type Database struct {
	mu     sync.RWMutex
	server *Server
	name   string
	users  map[string]*DBUser
	spaces map[string]*ShardSpace
}

// newDatabase returns an instance of Database associated with a server.
func newDatabase(s *Server) *Database {
	return &Database{
		server: s,
		users:  make(map[string]*DBUser),
		spaces: make(map[string]*ShardSpace),
	}
}

// Name returns the database name.
func (db *Database) Name() string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.name
}

// User returns a database user by name.
func (db *Database) User(name string) *DBUser {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.users[name]
}

// CreateUser creates a user in the database.
func (db *Database) CreateUser(username, password string, permissions []string) error {
	// TODO: Authorization.

	c := &createDBUserCommand{
		Database:    db.Name(),
		Username:    username,
		Password:    password,
		Permissions: permissions,
	}
	_, err := db.server.broadcast(createDBUserMessageType, c)
	return err
}

func (db *Database) saveUser(u *DBUser) error {
	db.mu.Lock()
	db.users[u.Name] = u
	db.mu.Unlock()
	return nil
}

// DeleteUser removes a user from the database.
func (db *Database) DeleteUser(username string) error {
	c := &deleteDBUserCommand{
		Database: db.Name(),
		Username: username,
	}
	_, err := db.server.broadcast(deleteDBUserMessageType, c)
	return err
}

func (db *Database) deleteUser(username string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if user exists.
	if db.users[username] == nil {
		return ErrUserNotFound
	}

	// Remove user.
	delete(db.users, username)
	return nil
}

// ChangePassword changes the password for a user in the database.
func (db *Database) ChangePassword(username, newPassword string) error {
	c := &changePasswordCommand{
		Database: db.Name(),
		Username: username,
		Password: newPassword,
	}
	_, err := db.server.broadcast(changePasswordMessageType, c)
	return err
}

func (db *Database) changePassword(username, newPassword string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check that user exists.
	u := db.users[username]
	if u == nil {
		return ErrUserNotFound
	}

	// Generate the hash of the password.
	hash, err := HashPassword(newPassword)
	if err != nil {
		return err
	}

	// Update user password hash.
	u.Hash = string(hash)

	return nil
}

// ShardSpace represents a policy for creating new shards in a database.
type ShardSpace struct {
	// Unique name within database. Required.
	Name string `json:"name"`

	// Name of the database the space belongs to. Required.
	Database string `json:"database"`

	// Expression used to match against series. Optional. Defaults to /.*/.
	Regex string `json:"regex"`

	// Optional, if not set it, it will default to the storage directory.
	RetentionPolicy string `json:"retentionPolicy"`

	ShardDuration     string `json:"shardDuration"`
	ReplicationFactor uint32 `json:"replicationFactor"`
	Split             uint32 `json:"split"`

	// TODO: shards []*ShardData
	re *regexp.Regexp // compiled regex
}

// NewShardSpace returns a new instance of ShardSpace with defaults set.
func NewShardSpace(database, name string) *ShardSpace {
	re, _ := regexp.Compile(".*")
	return &ShardSpace{
		Database:          database,
		Name:              name,
		Split:             DefaultSplit,
		ReplicationFactor: DefaultReplicationFactor,
		Regex:             "/.*/",
		RetentionPolicy:   "inf",
		ShardDuration:     "7d",

		// shards: make([]*ShardData, 0),
		re: re,
	}
}

// Match returns true if the string matches the space's regular expression.
func (s *ShardSpace) Match(str string) bool {
	//if s.re == nil {
	//	s.re, _ = s.re(s.Regex)
	//}
	return s.re.MatchString(str)
}

func (s *ShardSpace) ParsedRetentionPeriod() time.Duration {
	if s.RetentionPolicy == "" {
		return DefaultRetentionPolicyDuration
	} else if s.RetentionPolicy == "inf" {
		return time.Duration(0)
	}
	d, _ := parseTimeDuration(s.RetentionPolicy)
	return time.Duration(d)
}

func (s *ShardSpace) ParsedShardDuration() time.Duration {
	if s.ShardDuration != "" {
		d, _ := parseTimeDuration(s.ShardDuration)
		return time.Duration(d)
	}
	return DefaultShardDuration
}

// ContinuousQuery represents a query that exists on the server and processes
// each incoming event.
type ContinuousQuery struct {
	ID    uint32
	Query string
	// TODO: ParsedQuery *parser.SelectQuery
}

// compiles a regular expression. Removes leading and ending slashes.
func compileRegex(s string) (*regexp.Regexp, error) {
	if strings.HasPrefix(s, "/") {
		if strings.HasSuffix(s, "/i") {
			s = fmt.Sprintf("(?i)%s", s[1:len(s)-2])
		} else {
			s = s[1 : len(s)-1]
		}
	}
	return regexp.Compile(s)
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
