package influxdb

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/influxdb/influxdb/influxql"
	// "github.com/influxdb/influxdb/messaging"
)

// Database represents a collection of retention policies.
type Database struct {
	mu     sync.RWMutex
	server *Server
	name   string

	users    map[string]*DBUser          // database users by name
	policies map[string]*RetentionPolicy // retention policies by name
	shards   map[uint64]*Shard           // shards by id
	series   map[string]*Series          // series by name

	defaultRetentionPolicy string
	maxFieldID             uint64 // largest field id in use
}

// newDatabase returns an instance of Database associated with a server.
func newDatabase(s *Server) *Database {
	return &Database{
		server:   s,
		users:    make(map[string]*DBUser),
		policies: make(map[string]*RetentionPolicy),
		shards:   make(map[uint64]*Shard),
		series:   make(map[string]*Series),
	}
}

// Name returns the database name.
func (db *Database) Name() string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.name
}

// DefaultRetentionPolicy returns the retention policy that writes and queries will default to or nil if not set.
func (db *Database) DefaultRetentionPolicy() *RetentionPolicy {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.policies[db.defaultRetentionPolicy]
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

// RetentionPolicy returns a retention policy by name.
func (db *Database) RetentionPolicy(name string) *RetentionPolicy {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.policies[name]
}

// CreateRetentionPolicy creates a retention policy in the database.
func (db *Database) CreateRetentionPolicy(ss *RetentionPolicy) error {
	c := &createRetentionPolicyCommand{
		Database: db.Name(),
		Name:     ss.Name,
		Duration: ss.Duration,
		ReplicaN: ss.ReplicaN,
		SplitN:   ss.SplitN,
	}
	_, err := db.server.broadcast(createRetentionPolicyMessageType, c)
	return err
}

func (db *Database) applyCreateRetentionPolicy(name string, duration time.Duration, replicaN, splitN uint32) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate retention policy.
	if name == "" {
		return ErrRetentionPolicyNameRequired
	} else if db.policies[name] != nil {
		return ErrRetentionPolicyExists
	}

	// Add space to the database.
	db.policies[name] = &RetentionPolicy{
		Name:     name,
		Duration: duration,
		ReplicaN: replicaN,
		SplitN:   splitN,
	}

	return nil
}

// DeleteRetentionPolicy removes a retention policy from the database.
func (db *Database) DeleteRetentionPolicy(name string) error {
	c := &deleteRetentionPolicyCommand{Database: db.Name(), Name: name}
	_, err := db.server.broadcast(deleteRetentionPolicyMessageType, c)
	return err
}

func (db *Database) applyDeleteRetentionPolicy(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate retention policy.
	if name == "" {
		return ErrRetentionPolicyNameRequired
	} else if db.policies[name] == nil {
		return ErrRetentionPolicyNotFound
	}

	// Remove retention policy.
	delete(db.policies, name)
	return nil
}

// SetDefaultRetentionPolicy sets the default policy to write data into and query from on a database.
func (db *Database) SetDefaultRetentionPolicy(name string) error {
	c := &setDefaultRetentionPolicyCommand{Database: db.Name(), Name: name}
	_, err := db.server.broadcast(setDefaultRetentionPolicyMessageType, c)
	return err
}

func (db *Database) applySetDefaultRetentionPolicy(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check the retention policy exists
	if db.policies[name] == nil {
		return ErrRetentionPolicyNotFound
	}

	db.defaultRetentionPolicy = name
	return nil
}

// shard returns a shard by id.
func (db *Database) shard(id uint64) *Shard {
	for _, ss := range db.policies {
		for _, s := range ss.Shards {
			if s.ID == id {
				return s
			}
		}
	}
	return nil
}

// CreateShardIfNotExists creates a shard for a retention policy for a given timestamp.
func (db *Database) CreateShardIfNotExists(space string, timestamp time.Time) error {
	c := &createShardIfNotExistsSpaceCommand{Database: db.name, Space: space, Timestamp: timestamp}
	_, err := db.server.broadcast(createShardIfNotExistsMessageType, c)
	return err
}

func (db *Database) applyCreateShardIfNotExists(id uint64, policy string, timestamp time.Time) (error, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate retention policy.
	ss := db.policies[policy]
	if ss == nil {
		return ErrRetentionPolicyNotFound, false
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

	// Append to retention policy.
	ss.Shards = append(ss.Shards, s)

	return nil, true
}

// WriteSeries writes series data to the database.
func (db *Database) WriteSeries(name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error {
	panic("not yet implemented: Database.WriteSeries()")

	/* TEMPORARILY REMOVED FOR PROTOBUFS.
	// Find retention policy matching the series and split points by shard.
	db.mu.Lock()
	name := db.name
	space := db.retentionPolicyBySeries(series.GetName())
	db.mu.Unlock()

	// Ensure there is a space available.
	if space == nil {
		return ErrRetentionPolicyNotFound
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
	*/
}

/* TEMPORARILY REMOVED FOR PROTOBUFS.
func (db *Database) applyWriteSeries(id uint64, t int64, values map[uint8]interface{}) error {
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
*/

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
	o.DefaultRetentionPolicy = db.defaultRetentionPolicy
	o.MaxFieldID = db.maxFieldID
	for _, u := range db.users {
		o.Users = append(o.Users, u)
	}
	for _, ss := range db.policies {
		o.Policies = append(o.Policies, ss)
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
	db.defaultRetentionPolicy = o.DefaultRetentionPolicy
	db.maxFieldID = o.MaxFieldID

	// Copy users.
	db.users = make(map[string]*DBUser)
	for _, u := range o.Users {
		db.users[u.Name] = u
	}

	// Copy shard policies.
	db.policies = make(map[string]*RetentionPolicy)
	for _, ss := range o.Policies {
		db.policies[ss.Name] = ss
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
	Name                   string             `json:"name,omitempty"`
	DefaultRetentionPolicy string             `json:"defaultRetentionPolicy,omitempty"`
	MaxFieldID             uint64             `json:"maxFieldID,omitempty"`
	Users                  []*DBUser          `json:"users,omitempty"`
	Policies               []*RetentionPolicy `json:"policies,omitempty"`
	Shards                 []*Shard           `json:"shards,omitempty"`
	Series                 []*Series          `json:"series,omitempty"`
}

// databases represents a list of databases, sortable by name.
type databases []*Database

func (p databases) Len() int           { return len(p) }
func (p databases) Less(i, j int) bool { return p[i].name < p[j].name }
func (p databases) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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
func NewRetentionPolicy() *RetentionPolicy {
	return &RetentionPolicy{
		ReplicaN: DefaultReplicaN,
		SplitN:   DefaultSplitN,
		Duration: DefaultShardRetention,
	}
}

/*
// SplitPoints groups a set of points by shard id.
// Also returns a list of timestamps that did not match an existing shard.
func (ss *RetentionPolicy) Split(a []*protocol.Point) (points map[uint64][]*protocol.Point, unassigned []*protocol.Point) {
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
*/

// ShardByTimestamp returns the shard in the space that owns a given timestamp.
// Returns nil if the shard does not exist.
func (ss *RetentionPolicy) ShardByTimestamp(timestamp time.Time) *Shard {
	for _, s := range ss.Shards {
		if timeBetween(timestamp, s.StartTime, s.EndTime) {
			return s
		}
	}
	return nil
}

// MarshalJSON encodes a retention policy to a JSON-encoded byte slice.
func (s *RetentionPolicy) MarshalJSON() ([]byte, error) {
	return json.Marshal(&retentionPolicyJSON{
		Name:     s.Name,
		Duration: s.Duration,
		ReplicaN: s.ReplicaN,
		SplitN:   s.SplitN,
	})
}

// UnmarshalJSON decodes a JSON-encoded byte slice to a retention policy.
func (s *RetentionPolicy) UnmarshalJSON(data []byte) error {
	// Decode into intermediate type.
	var o retentionPolicyJSON
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Copy over properties from intermediate type.
	s.Name = o.Name
	s.ReplicaN = o.ReplicaN
	s.SplitN = o.SplitN
	s.Duration = o.Duration
	s.Shards = o.Shards

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

// RetentionPolicys represents a list of shard policies.
type RetentionPolicys []*RetentionPolicy

// Shards returns a list of all shards for all policies.
func (a RetentionPolicys) Shards() []*Shard {
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
