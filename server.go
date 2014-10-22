package influxdb

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/influxdb/influxdb/messaging"
)

// Server represents a collection of metadata and raw metric data.
type Server struct {
	mu        sync.RWMutex
	client    MessagingClient
	databases map[string]*Database
}

// NewServer returns a new instance of Server.
// The server requires a client to the messaging broker to be passed in.
func NewServer(client MessagingClient) *Server {
	assert(client != nil, "messaging client required")
	return &Server{
		client:    client,
		databases: make(map[String]*Database),
	}
}

// Open initializes the server from a given path.
func (s *Server) Open(path string) error {
	// TODO: Open metadata.
	return nil
}

// Close shuts down the server.
func (s *Server) Close() error {
	// TODO: Close metadata.
	return nil
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
		Split:             DEFAULT_SPLIT,
		ReplicationFactor: DEFAULT_REPLICATION_FACTOR,
		Regex:             "/.*/",
		RetentionPolicy:   "inf",
		ShardDuration:     "7d",

		shards: make([]*ShardData, 0),
		re:     re,
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
	d, _ := common.ParseTimeDuration(s.RetentionPolicy)
	return time.Duration(d)
}

func (s *ShardSpace) ParsedShardDuration() time.Duration {
	if s.ShardDuration != "" {
		d, _ := common.ParseTimeDuration(s.ShardDuration)
		return time.Duration(d)
	}
	return DefaultShardDuration
}

func (s *ShardSpace) UpdateFromSpace(space *ShardSpace) error {
	r, err := s.compileRegex(space.Regex)
	if err != nil {
		return err
	}
	s.Regex = space.Regex
	s.compiledRegex = r
	s.RetentionPolicy = space.RetentionPolicy
	s.ShardDuration = space.ShardDuration
	s.ReplicationFactor = space.ReplicationFactor
	s.Split = space.Split
	return nil
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

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}
