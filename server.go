package influxdb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/messaging"
	"github.com/influxdb/influxdb/protocol"
)

// Server represents a collection of metadata and raw metric data.
type Server struct {
	mu        sync.RWMutex
	path      string
	client    MessagingClient
	databases map[string]*Database

	done chan struct{}
}

// NewServer returns a new instance of Server.
// The server requires a client to the messaging broker to be passed in.
func NewServer(client MessagingClient) *Server {
	assert(client != nil, "messaging client required")
	return &Server{
		client:    client,
		databases: make(map[string]*Database),
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
	defer s.mu.Lock()

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

// publish encodes a message as JSON and send it to the broker.
// This function does not wait for the message to be processed.
// Returns the broker log index of the message or an error.
func (s *Server) publish(typ *messaging.MessageType, c interface{}) (uint64, error) {
	// Encode the command.
	b, err := json.Marshal(c)
	if err != nil {
		return 0, err
	}

	// Publish the message.
	return s.client.Publish(typ, b)
}

// publishSync encodes a message as JSON and send it to the broker.
// This function will wait until the message is processed before returning.
// Returns an error if the message could not be successfully published.
func (s *Server) publishSync(typ *messaging.MessageType, c interface{}) error {
	// Publish the message.
	index, err := s.publish(typ, c)
	if err != nil {
		return err
	}

	// Wait for the message to be processed.
	if err := s.client.wait(index); err != nil {
		return err
	}

	return nil
}

// CreateDatabase creates a new database.
func (s *Server) CreateDatabase(name string) error {
	s.mu.Lock()
	if s.databases[name] != nil {
		s.mu.Unlock()
		return ErrDatabaseExists
	}
	s.mu.Unlock()
	return s.publishSync(createDatabaseMessageType, &createDatabaseCommand{Name: name})
}

func (s *Server) applyCreateDatabase(c *createDatabaseCommand) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.databases[c.Name] != nil {
		return
	}

	// Create database entry.
	s.databases[c.Name] = &Database{Name: c.Name}
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
		switch m.Type {
		case createDatabaseMessageType:
			var c createDatabaseCommand
			mustUnmarshal(m.Data, &c)
			b.applyCreateDatabase(c.ID, c.Name)
		}
	}
}

const (
	createDatabaseMessageType = messaging.MessageType(0x00)
	deleteDatabaseMessageType = messaging.MessageType(0x01)
)

// createDatabaseCommand creates a new database.
type createDatabaseCommand struct {
	Name string `json:"name"`
}

// MessagingClient represents the client used to receive messages from brokers.
type MessagingClient interface {
	C() <-chan *messaging.Message
}

// Database represents a collection of shard spaces.
type Database struct {
	mu          sync.RWMutex
	name        string
	shardSpaces map[string]*ShardSpace
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
