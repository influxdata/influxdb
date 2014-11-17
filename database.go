package influxdb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/messaging"
	"github.com/influxdb/influxdb/protocol"
)

// Database represents a collection of shard spaces.
type Database struct {
	mu     sync.RWMutex
	server *Server
	name   string

	users  map[string]*DBUser     // database users by name
	spaces map[string]*ShardSpace // shard spaces by name
	shards map[uint64]*Shard      // shards by id
	series map[string]*Series     // series by name

	maxFieldID uint64 // largest field id in use
}

// newDatabase returns an instance of Database associated with a server.
func newDatabase(s *Server) *Database {
	return &Database{
		server: s,
		users:  make(map[string]*DBUser),
		spaces: make(map[string]*ShardSpace),
		shards: make(map[uint64]*Shard),
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

// shardSpaceBySeries returns a shard space that matches a series name.
func (db *Database) shardSpaceBySeries(name string) *ShardSpace {
	for _, ss := range db.spaces {
		if ss.Regex.MatchString(name) {
			return ss
		}
	}
	return nil
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

// shard returns a shard by id.
func (db *Database) shard(id uint64) *Shard {
	for _, ss := range db.spaces {
		for _, s := range ss.Shards {
			if s.ID == id {
				return s
			}
		}
	}
	return nil
}

// CreateShardIfNotExists creates a shard for a shard space for a given timestamp.
func (db *Database) CreateShardIfNotExists(space string, timestamp time.Time) error {
	c := &createShardIfNotExistsSpaceCommand{Database: db.name, Space: space, Timestamp: timestamp}
	_, err := db.server.broadcast(createShardIfNotExistsMessageType, c)
	return err
}

func (db *Database) applyCreateShardIfNotExists(id uint64, space string, timestamp time.Time) (error, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate shard space.
	ss := db.spaces[space]
	if ss == nil {
		return ErrShardSpaceNotFound, false
	}

	// If we can match to an existing shard date range then just ignore request.
	for _, s := range ss.Shards {
		if timeBetween(timestamp, s.StartTime, s.EndTime) {
			return nil, false
		}
	}

	// If no shards match then create a new one.
	startTime := timestamp.Truncate(ss.Duration).UTC()
	endTime := startTime.Add(ss.Duration).UTC()
	s := newShard()
	s.ID, s.StartTime, s.EndTime = id, startTime, endTime

	// Open shard.
	if err := s.open(db.server.shardPath(s.ID)); err != nil {
		panic("unable to open shard: " + err.Error())
	}

	// Append to shard space.
	ss.Shards = append(ss.Shards, s)

	return nil, true
}

// WriteSeries writes series data to the database.
func (db *Database) WriteSeries(series *protocol.Series) error {
	// Find shard space matching the series and split points by shard.
	db.mu.Lock()
	name := db.name
	space := db.shardSpaceBySeries(series.GetName())
	db.mu.Unlock()

	// Ensure there is a space available.
	if space == nil {
		return ErrShardSpaceNotFound
	}

	// Group points by shard.
	pointsByShard, unassigned := space.Split(series.Points)

	// Request shard creation for timestamps for missing shards.
	for _, p := range unassigned {
		timestamp := time.Unix(0, p.GetTimestamp())
		if err := db.CreateShardIfNotExists(space.Name, timestamp); err != nil {
			return fmt.Errorf("create shard(%s/%d): %s", space.Name, timestamp.Format(time.RFC3339Nano), err)
		}
	}

	// Try to split the points again. Fail if it doesn't work this time.
	pointsByShard, unassigned = space.Split(series.Points)
	if len(unassigned) > 0 {
		return fmt.Errorf("unmatched points in space(%s): %#v", unassigned)
	}

	// Publish each group of points.
	for shardID, points := range pointsByShard {
		// Marshal series into protobuf format.
		req := &protocol.WriteSeriesRequest{
			Database: proto.String(name),
			Series: &protocol.Series{
				Name:     series.Name,
				Fields:   series.Fields,
				FieldIds: series.FieldIds,
				ShardId:  proto.Uint64(shardID),
				Points:   points,
			},
		}
		data, err := proto.Marshal(req)
		if err != nil {
			return err
		}

		// Publish "write series" message on shard's topic to broker.
		m := &messaging.Message{
			Type:    writeSeriesMessageType,
			TopicID: shardID,
			Data:    data,
		}
		index, err := db.server.client.Publish(m)
		if err != nil {
			return err
		}
		if err := db.server.sync(index); err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) applyWriteSeries(s *protocol.Series) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	shard := db.shard(s.GetShardId())

	// Find shard.
	if s == nil {
		return ErrShardNotFound
	}

	// Find or create series.
	var changed bool
	var series *Series
	if series = db.series[s.GetName()]; series == nil {
		series = &Series{Name: s.GetName()}
		db.series[s.GetName()] = series
		changed = true
	}

	// Assign field ids.
	s.FieldIds = nil
	for _, name := range s.GetFields() {
		// Find field on series.
		var fieldID uint64
		for _, f := range series.Fields {
			if f.Name == name {
				fieldID = f.ID
				break
			}
		}

		// Create a new field, if not exists.
		if fieldID == 0 {
			db.maxFieldID++
			fieldID = db.maxFieldID
			series.Fields = append(series.Fields, &Field{ID: fieldID, Name: name})
			changed = true
		}

		// Append the field id.
		s.FieldIds = append(s.FieldIds, fieldID)
	}

	// Perist to metastore if changed.
	if changed {
		db.server.meta.mustUpdate(func(tx *metatx) error {
			return tx.saveDatabase(db)
		})
	}

	// Write to shard.
	return shard.writeSeries(s)
}

// ExecuteQuery executes a query against a database.
func (db *Database) ExecuteQuery(q influxql.Query) error {
	panic("not yet implemented: Database.ExecuteQuery()") // TODO
}

// timeBetween returns true if t is between min and max, inclusive.
func timeBetween(t, min, max time.Time) bool {
	return (t.Equal(min) || t.After(min)) && (t.Equal(max) || t.Before(max))
}

// MarshalJSON encodes a database into a JSON-encoded byte slice.
func (db *Database) MarshalJSON() ([]byte, error) {
	// Copy over properties to intermediate type.
	var o databaseJSON
	o.Name = db.name
	o.MaxFieldID = db.maxFieldID
	for _, u := range db.users {
		o.Users = append(o.Users, u)
	}
	for _, ss := range db.spaces {
		o.Spaces = append(o.Spaces, ss)
	}
	for _, s := range db.shards {
		o.Shards = append(o.Shards, s)
	}
	for _, s := range db.series {
		o.Series = append(o.Series, s)
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
	db.maxFieldID = o.MaxFieldID

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
	db.shards = make(map[uint64]*Shard)
	for _, s := range o.Shards {
		db.shards[s.ID] = s
	}

	// Copy series.
	db.series = make(map[string]*Series)
	for _, s := range o.Series {
		db.series[s.Name] = s
	}

	return nil
}

// databaseJSON represents the JSON-serialization format for a database.
type databaseJSON struct {
	Name       string        `json:"name,omitempty"`
	MaxFieldID uint64        `json:"maxFieldID,omitempty"`
	Users      []*DBUser     `json:"users,omitempty"`
	Spaces     []*ShardSpace `json:"spaces,omitempty"`
	Shards     []*Shard      `json:"shards,omitempty"`
	Series     []*Series     `json:"series,omitempty"`
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

// SplitPoints groups a set of points by shard id.
// Also returns a list of timestamps that did not match an existing shard.
func (ss *ShardSpace) Split(a []*protocol.Point) (points map[uint64][]*protocol.Point, unassigned []*protocol.Point) {
	points = make(map[uint64][]*protocol.Point)
	for _, p := range a {
		if s := ss.ShardByTimestamp(time.Unix(0, p.GetTimestamp())); s != nil {
			points[s.ID] = append(points[s.ID], p)
		} else {
			unassigned = append(unassigned, p)
		}
	}
	return
}

// ShardByTimestamp returns the shard in the space that owns a given timestamp.
// Returns nil if the shard does not exist.
func (ss *ShardSpace) ShardByTimestamp(timestamp time.Time) *Shard {
	for _, s := range ss.Shards {
		if timeBetween(timestamp, s.StartTime, s.EndTime) {
			return s
		}
	}
	return nil
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
	Name   string   `json:"name,omitempty"`
	Fields []*Field `json:"fields,omitempty"`
}

func (s *Series) FieldsByNames(names []string) (a []*Field) {
	for _, f := range s.Fields {
		for _, name := range names {
			if f.Name == name {
				a = append(a, f)
			}
		}
	}
	return
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
