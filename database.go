package influxdb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

// Database represents a collection of shard spaces.
type Database struct {
	mu     sync.RWMutex
	server *Server
	name   string

	users  map[string]*DBUser     // database users by name
	spaces map[string]*ShardSpace // shard spaces by name
	shards map[uint32]*shard      // shards by id
}

// newDatabase returns an instance of Database associated with a server.
func newDatabase(s *Server) *Database {
	return &Database{
		server: s,
		users:  make(map[string]*DBUser),
		spaces: make(map[string]*ShardSpace),
		shards: make(map[uint32]*shard),
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

// User returns a list of all database users.
func (db *Database) Users() []*DBUser {
	db.mu.Lock()
	defer db.mu.Unlock()
	var a dbUsers
	for _, u := range db.users {
		a = append(a, u)
	}
	sort.Sort(a)
	return a
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

func (db *Database) applyCreateUser(username, password string, permissions []string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate user.
	if username == "" {
		return ErrUsernameRequired
	} else if !isValidName(username) {
		return ErrInvalidUsername
	} else if db.users[username] != nil {
		return ErrUserExists
	}

	// Generate the hash of the password.
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}

	// Setup matchers.
	rmatcher := []*Matcher{{true, ".*"}}
	wmatcher := []*Matcher{{true, ".*"}}
	if len(permissions) == 2 {
		rmatcher[0].Name = permissions[0]
		wmatcher[0].Name = permissions[1]
	}

	// Create the user.
	db.users[username] = &DBUser{
		CommonUser: CommonUser{
			Name: username,
			Hash: string(hash),
		},
		DB:       db.name,
		ReadFrom: rmatcher,
		WriteTo:  wmatcher,
		IsAdmin:  false,
	}

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

func (db *Database) applyDeleteUser(username string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate user.
	if username == "" {
		return ErrUsernameRequired
	} else if db.users[username] == nil {
		return ErrUserNotFound
	}

	// Remove user.
	delete(db.users, username)
	return nil
}

// ChangePassword changes the password for a user in the database
func (db *Database) ChangePassword(username, newPassword string) error {
	c := &dbUserSetPasswordCommand{
		Database: db.Name(),
		Username: username,
		Password: newPassword,
	}
	_, err := db.server.broadcast(dbUserSetPasswordMessageType, c)
	return err
}

func (db *Database) applyChangePassword(username, newPassword string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate user.
	u := db.users[username]
	if username == "" {
		return ErrUsernameRequired
	} else if u == nil {
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

// ShardSpace returns a shard space by name.
func (db *Database) ShardSpace(name string) *ShardSpace {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.spaces[name]
}

// CreateShardSpace creates a shard space in the database.
func (db *Database) CreateShardSpace(ss *ShardSpace) error {
	c := &createShardSpaceCommand{
		Database:  db.Name(),
		Name:      ss.Name,
		Retention: ss.Retention,
		Duration:  ss.Duration,
		ReplicaN:  ss.ReplicaN,
		SplitN:    ss.SplitN,
	}
	if ss.Regex != nil {
		c.Regex = ss.Regex.String()
	}
	_, err := db.server.broadcast(createShardSpaceMessageType, c)
	return err
}

func (db *Database) applyCreateShardSpace(name, regex string, retention, duration time.Duration, replicaN, splitN uint32) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate shard space.
	if name == "" {
		return ErrShardSpaceNameRequired
	} else if db.spaces[name] != nil {
		return ErrShardSpaceExists
	}

	// Compile regex.
	re := regexp.MustCompile(regex)

	// Add space to the database.
	db.spaces[name] = &ShardSpace{
		Name:      name,
		Regex:     re,
		Retention: retention,
		Duration:  duration,
		ReplicaN:  replicaN,
		SplitN:    splitN,
	}

	return nil
}

// DeleteShardSpace removes a shard space from the database.
func (db *Database) DeleteShardSpace(name string) error {
	c := &deleteShardSpaceCommand{Database: db.Name(), Name: name}
	_, err := db.server.broadcast(deleteShardSpaceMessageType, c)
	return err
}

func (db *Database) applyDeleteShardSpace(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate shard space.
	if name == "" {
		return ErrShardSpaceNameRequired
	} else if db.spaces[name] == nil {
		return ErrShardSpaceNotFound
	}

	// Remove shard space.
	delete(db.spaces, name)
	return nil
}

// WriteSeries writes series data to the database.
func (db *Database) WriteSeries(series *protocol.Series) error {
	// TODO: Split points up by shard.
	// TODO: Issue message to broker.

	panic("not yet implemented") /* TODO */
}

// ExecuteQuery executes a query against a database.
func (db *Database) ExecuteQuery(u parser.User, q *parser.Query) error {
	spec := parser.NewQuerySpec(u, db.Name(), q)
	// TODO: Check permissions.
	//if ok, err := db.permissions.CheckQueryPermissions(u, db.Name(), spec); !ok {
	//	return err
	//}

	// TODO: ListSeries
	// TODO: DropSeries
	switch q.Type() {
	case parser.Delete:
		return db.executeDeleteQuery(u, spec)
	case parser.Select:
		return db.executeSelectQuery(u, spec)
	default:
		return ErrInvalidQuery
	}
}

// executeDeleteQuery executes a deletion query against the database.
func (db *Database) executeDeleteQuery(u parser.User, spec *parser.QuerySpec) error {
	// TODO: Execute deletion message to broker.
	panic("not yet implemented") // TODO
}

// executeSelectQuery executes a selection query against the database.
func (db *Database) executeSelectQuery(u parser.User, spec *parser.QuerySpec) error {
	panic("not yet implemented") // TODO
}

// MarshalJSON encodes a database into a JSON-encoded byte slice.
func (db *Database) MarshalJSON() ([]byte, error) {
	// Copy over properties to intermediate type.
	var o databaseJSON
	o.Name = db.name
	for _, u := range db.users {
		o.Users = append(o.Users, u)
	}
	for _, ss := range db.spaces {
		o.Spaces = append(o.Spaces, ss)
	}
	for _, s := range db.shards {
		o.Shards = append(o.Shards, s)
	}
	return json.Marshal(&o)
}

// UnmarshalJSON decodes a JSON-encoded byte slice to a database.
func (db *Database) UnmarshalJSON(data []byte) error {
	// Decode into intermediate type.
	var o databaseJSON
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Copy over properties from intermediate type.
	db.name = o.Name

	// Copy users.
	db.users = make(map[string]*DBUser)
	for _, u := range o.Users {
		db.users[u.Name] = u
	}

	// Copy shard spaces.
	db.spaces = make(map[string]*ShardSpace)
	for _, ss := range o.Spaces {
		db.spaces[ss.Name] = ss
	}

	// Copy shards.
	db.shards = make(map[uint32]*shard)
	for _, s := range o.Shards {
		db.shards[s.id] = s
	}

	return nil
}

// databaseJSON represents the JSON-serialization format for a database.
type databaseJSON struct {
	Name   string        `json:"name,omitempty"`
	Users  []*DBUser     `json:"users,omitempty"`
	Spaces []*ShardSpace `json:"spaces,omitempty"`
	Shards []*shard      `json:"shards,omitempty"`
}

// databases represents a list of databases, sortable by name.
type databases []*Database

func (p databases) Len() int           { return len(p) }
func (p databases) Less(i, j int) bool { return p[i].name < p[j].name }
func (p databases) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// ShardSpace represents a policy for creating new shards in a database.
type ShardSpace struct {
	// Unique name within database. Required.
	Name string

	// Expression used to match against series. Optional. Defaults to /.*/.
	Regex *regexp.Regexp

	Retention time.Duration
	Duration  time.Duration

	ReplicaN uint32
	SplitN   uint32
}

// NewShardSpace returns a new instance of ShardSpace with defaults set.
func NewShardSpace() *ShardSpace {
	return &ShardSpace{
		Regex:     regexp.MustCompile(`.*`),
		Retention: DefaultShardRetention,
		Duration:  DefaultShardDuration,
		SplitN:    DefaultSplitN,
		ReplicaN:  DefaultReplicaN,
	}
}

// MarshalJSON encodes a shard space to a JSON-encoded byte slice.
func (s *ShardSpace) MarshalJSON() ([]byte, error) {
	var o shardSpaceJSON
	o.Name = s.Name
	o.ReplicaN = s.ReplicaN
	o.SplitN = s.SplitN
	o.Regex = s.Regex.String()
	o.Retention = parser.FormatTimeDuration(s.Retention)
	o.Duration = parser.FormatTimeDuration(s.Duration)
	return json.Marshal(&o)
}

// UnmarshalJSON decodes a JSON-encoded byte slice to a shard space.
func (s *ShardSpace) UnmarshalJSON(data []byte) error {
	// Decode into intermediate type.
	var o shardSpaceJSON
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Copy over properties from intermediate type.
	s.Name = o.Name
	s.ReplicaN = o.ReplicaN
	s.SplitN = o.SplitN

	// Parse regex.
	if o.Regex != "" {
		if regex, err := compileRegex(o.Regex); err != nil {
			return fmt.Errorf("regex: %s", err)
		} else {
			s.Regex = regex
		}
	} else {
		s.Regex = regexp.MustCompile(`.*`)
	}

	// Parse retention.
	if o.Retention == "inf" || o.Retention == "" {
		s.Retention = time.Duration(0)
	} else {
		retention, err := parser.ParseTimeDuration(o.Retention)
		if err != nil {
			return fmt.Errorf("retention policy: %s", err)
		}
		s.Retention = retention
	}

	// Parse duration.
	if o.Duration == "inf" || o.Duration == "" {
		s.Duration = time.Duration(0)
	} else {
		duration, err := parser.ParseTimeDuration(o.Duration)
		if err != nil {
			return fmt.Errorf("shard duration: %s", err)
		}
		s.Duration = duration
	}

	return nil
}

// shardSpaceJSON represents an intermediate struct for JSON marshaling.
type shardSpaceJSON struct {
	Name      string `json:"name"`
	Regex     string `json:"regex,omitempty"`
	Retention string `json:"retentionPolicy,omitempty"`
	Duration  string `json:"shardDuration,omitempty"`
	ReplicaN  uint32 `json:"replicationFactor,omitempty"`
	SplitN    uint32 `json:"split,omitempty"`
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
