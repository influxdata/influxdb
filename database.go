package influxdb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb/engine"
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
	shards map[uint32]*Shard      // shards by id
	series map[string]*Series     // series by name
}

// newDatabase returns an instance of Database associated with a server.
func newDatabase(s *Server) *Database {
	return &Database{
		server: s,
		users:  make(map[string]*DBUser),
		spaces: make(map[string]*ShardSpace),
		shards: make(map[uint32]*Shard),
		series: make(map[string]*Series),
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
func (db *Database) ExecuteQuery(u parser.User, q *parser.Query, p engine.Processor) error {
	spec := parser.NewQuerySpec(u, db.Name(), q)
	// TODO: Check permissions.
	//if ok, err := db.permissions.CheckQueryPermissions(u, db.Name(), spec); !ok {
	//	return err
	//}

	// TODO: ListSeries
	// TODO: DropSeries
	switch q.Type() {
	case parser.Select:
		return db.executeSelectQuery(u, spec, p)
	case parser.Delete:
		return db.executeDeleteQuery(u, spec)
	default:
		return ErrInvalidQuery
	}
}

// executeSelectQuery executes a selection query against the database.
func (db *Database) executeSelectQuery(u parser.User, spec *parser.QuerySpec, p engine.Processor) error {
	q := spec.SelectQuery()

	// Find series matching query.
	series := db.seriesByTableNames(q.FromClause.Names)

	// Find a list of spaces matching the series.
	spaces := db.spacesBySeries(series)
	shards := ShardSpaces(spaces).Shards()

	// Select subset of shards matching date range.
	// If no shards are available then close the processor and return.
	shards = shardsInRange(shards, spec.GetStartTime(), spec.GetEndTime())
	if len(shards) == 0 {
		return p.Close()
	}

	// Sort shards in appropriate order based on query.
	if spec.IsAscending() {
		sort.Sort(shardsAsc(shards))
	} else {
		sort.Sort(shardsDesc(shards))
	}

	// If "group by" interval lines up with shard duration and from clause
	// is not "inner join" or "merge" then we can aggregate locally.
	local := true
	for _, s := range shards {
		if !spec.CanAggregateLocally(s.Duration()) {
			local = false
			break
		}
	}

	// If aggregating locally then use PassthroughEngineWithLimit processor.
	// Otherwise create a new query engine with a list of shard ids.
	if local {
		p = engine.NewPassthroughEngineWithLimit(p, 100, q.Limit)
	} else {
		var err error
		if p, err = engine.NewQueryEngine(p, q, Shards(shards).IDs()); err != nil {
			return fmt.Errorf("new query engine: %s", err)
		}
	}

	// Create MergeChannelProcessor.
	mcp := NewMergeChannelProcessor(p, 1)
	go mcp.ProcessChannels()

	// Loop over shards, create response channel, kick off querying.
	for i, s := range shards {
		c, err := mcp.NextChannel(1000)
		if err != nil {
			mcp.Close()
			return fmt.Errorf("next channel: %s", err)
		}

		// We query shards for data and stream them to query processor
		log4go.Debug("QUERYING: shard: %d", i)
		go s.query(spec, c)
	}

	// Close merge channel processor.
	if err := mcp.Close(); err != nil {
		log4go.Error("Error while querying shards: %s", err)
		return err
	}

	return p.Close()
}

// seriesByTableNames returns a list of series that match a set of table names.
func (db *Database) seriesByTableNames(names []*parser.TableName) (a []*Series) {
	for _, s := range db.series {
		for _, name := range names {
			if re, ok := name.Name.GetCompiledRegex(); ok {
				if re.MatchString(s.Name) {
					a = append(a, s)
				}
			} else if name.Name.Name == s.Name {
				a = append(a, s)
			}
		}
	}
	return
}

// spacesBySeries returns a list of unique shard spaces that match a set of series.
func (db *Database) spacesBySeries(series []*Series) (a []*ShardSpace) {
	m := make(map[*ShardSpace]struct{})
	for _, s := range series {
		for _, ss := range db.spaces {
			// Check if we've already matched the space with a previous series.
			if _, ok := m[ss]; ok {
				continue
			}

			// Add shard space shards that match.
			if ss.Regex.MatchString(s.Name) {
				a = append(a, ss)
				m[ss] = struct{}{}
			}
		}
	}
	return
}

// shardsInRange returns a subset of shards that are in the range of tmin and tmax.
func shardsInRange(shards []*Shard, tmin, tmax time.Time) (a []*Shard) {
	for _, s := range shards {
		if timeBetween(s.StartTime, tmin, tmax) && timeBetween(s.EndTime, tmin, tmax) {
			a = append(a, s)
		}
	}
	return
}

// timeBetween returns true if t is between min and max, inclusive.
func timeBetween(t, min, max time.Time) bool {
	return (t.Equal(min) || t.After(min)) && (t.Equal(max) || t.Before(max))
}

// executeDeleteQuery executes a deletion query against the database.
func (db *Database) executeDeleteQuery(u parser.User, spec *parser.QuerySpec) error {
	// TODO: Execute deletion message to broker.
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
	db.shards = make(map[uint32]*Shard)
	for _, s := range o.Shards {
		db.shards[s.ID] = s
	}

	return nil
}

// databaseJSON represents the JSON-serialization format for a database.
type databaseJSON struct {
	Name   string        `json:"name,omitempty"`
	Users  []*DBUser     `json:"users,omitempty"`
	Spaces []*ShardSpace `json:"spaces,omitempty"`
	Shards []*Shard      `json:"shards,omitempty"`
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

	Shards []*Shard
}

// NewShardSpace returns a new instance of ShardSpace with defaults set.
func NewShardSpace() *ShardSpace {
	return &ShardSpace{
		Regex:     regexp.MustCompile(`.*`),
		ReplicaN:  DefaultReplicaN,
		SplitN:    DefaultSplitN,
		Retention: DefaultShardRetention,
		Duration:  DefaultShardDuration,
	}
}

// MarshalJSON encodes a shard space to a JSON-encoded byte slice.
func (s *ShardSpace) MarshalJSON() ([]byte, error) {
	return json.Marshal(&shardSpaceJSON{
		Name:      s.Name,
		Regex:     s.Regex.String(),
		Retention: s.Retention,
		Duration:  s.Duration,
		ReplicaN:  s.ReplicaN,
		SplitN:    s.SplitN,
	})
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
	s.Retention = o.Retention
	s.Duration = o.Duration
	s.Shards = o.Shards

	s.Regex, _ = regexp.Compile(o.Regex)
	if s.Regex == nil {
		s.Regex = regexp.MustCompile(`.*`)
	}

	return nil
}

// shardSpaceJSON represents an intermediate struct for JSON marshaling.
type shardSpaceJSON struct {
	Name      string        `json:"name"`
	Regex     string        `json:"regex,omitempty"`
	ReplicaN  uint32        `json:"replicaN,omitempty"`
	SplitN    uint32        `json:"splitN,omitempty"`
	Retention time.Duration `json:"retention,omitempty"`
	Duration  time.Duration `json:"duration,omitempty"`
	Shards    []*Shard      `json:"shards,omitempty"`
}

// ShardSpaces represents a list of shard spaces.
type ShardSpaces []*ShardSpace

// Shards returns a list of all shards for all spaces.
func (a ShardSpaces) Shards() []*Shard {
	var shards []*Shard
	for _, ss := range a {
		shards = append(shards, ss.Shards...)
	}
	return shards
}

// Series represents a series of timeseries points.
type Series struct {
	ID     uint64 `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	Fields Fields `json:"fields,omitempty"`
}

// Field represents a series field.
type Field struct {
	ID   uint64 `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// String returns a string representation of the field.
func (f *Field) String() string {
	return fmt.Sprintf("Name: %s, ID: %d", f.Name, f.ID)
}

// Fields represents a list of fields.
type Fields []*Field

// Names returns a list of all field names.
func (a Fields) Names() []string {
	names := make([]string, len(a))
	for i, f := range a {
		names[i] = f.Name
	}
	return names
}
