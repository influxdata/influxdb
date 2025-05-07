// Package meta provides control over meta data for InfluxDB,
// such as controlling databases, retention policies, users, etc.
package meta

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/file"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

const (
	// SaltBytes is the number of bytes used for salts.
	SaltBytes = 32

	metaFile = "meta.db"

	// ShardGroupDeletedExpiration is the amount of time before a shard group info will be removed from cached
	// data after it has been marked deleted (2 weeks).
	ShardGroupDeletedExpiration = -2 * 7 * 24 * time.Hour
)

var (
	// ErrServiceUnavailable is returned when the meta service is unavailable.
	ErrServiceUnavailable = errors.New("meta service unavailable")

	// ErrService is returned when the meta service returns an error.
	ErrService = errors.New("meta service error")
)

// Client is used to execute commands on and read data from
// a meta service cluster.
type Client struct {
	logger *zap.Logger

	mu        sync.RWMutex
	closing   chan struct{}
	changed   chan struct{}
	cacheData *Data

	// Authentication cache.
	authCache map[string]authUser

	path string

	retentionAutoCreate bool
}

type authUser struct {
	bhash string
	salt  []byte
	hash  []byte
}

// NewClient returns a new *Client.
func NewClient(config *Config) *Client {
	return &Client{
		cacheData: &Data{
			ClusterID: uint64(rand.Int63()),
			Index:     1,
		},
		closing:             make(chan struct{}),
		changed:             make(chan struct{}),
		logger:              zap.NewNop(),
		authCache:           make(map[string]authUser),
		path:                config.Dir,
		retentionAutoCreate: config.RetentionAutoCreate,
	}
}

// Open a connection to a meta service cluster.
func (c *Client) Open() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Try to load from disk
	if err := c.Load(); err != nil {
		return err
	}

	// If this is a brand new instance, persist to disk immediatly.
	if c.cacheData.Index == 1 {
		if err := snapshot(c.path, c.cacheData); err != nil {
			return err
		}
	}

	return nil
}

// Close the meta service cluster connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}

	select {
	case <-c.closing:
		return nil
	default:
		close(c.closing)
	}

	return nil
}

// AcquireLease attempts to acquire the specified lease.
// TODO corylanou remove this for single node
func (c *Client) AcquireLease(name string) (*Lease, error) {
	l := Lease{
		Name:       name,
		Expiration: time.Now().Add(DefaultLeaseDuration),
	}
	return &l, nil
}

// ClusterID returns the ID of the cluster it's connected to.
func (c *Client) ClusterID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cacheData.ClusterID
}

// Database returns info for the requested database.
func (c *Client) Database(name string) *DatabaseInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, d := range c.cacheData.Databases {
		if d.Name == name {
			return &d
		}
	}

	return nil
}

// Databases returns a list of all database infos.
func (c *Client) Databases() []DatabaseInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dbs := c.cacheData.Databases
	if dbs == nil {
		return []DatabaseInfo{}
	}
	return dbs
}

// CreateDatabase creates a database or returns it if it already exists.
func (c *Client) CreateDatabase(name string) (*DatabaseInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if db := data.Database(name); db != nil {
		return db, nil
	}

	if err := data.CreateDatabase(name); err != nil {
		return nil, err
	}

	// create default retention policy
	if c.retentionAutoCreate {
		rpi := DefaultRetentionPolicyInfo()
		if err := data.CreateRetentionPolicy(name, rpi, true); err != nil {
			return nil, err
		}
	}

	db := data.Database(name)

	if err := c.commit(data); err != nil {
		return nil, err
	}

	return db, nil
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified
// retention policy.
//
// When creating a database with a retention policy, the retention policy will
// always be set to default. Therefore if the caller provides a retention policy
// that already exists on the database, but that retention policy is not the
// default one, an error will be returned.
//
// This call is only idempotent when the caller provides the exact same
// retention policy, and that retention policy is already the default for the
// database.
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, spec *RetentionPolicySpec) (*DatabaseInfo, error) {
	if spec == nil {
		return nil, errors.New("CreateDatabaseWithRetentionPolicy called with nil spec")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	db := data.Database(name)
	if db == nil {
		if err := data.CreateDatabase(name); err != nil {
			return nil, err
		}
		db = data.Database(name)
	}

	// No existing retention policies, so we can create the provided policy as
	// the new default policy.
	rpi := spec.NewRetentionPolicyInfo()
	if len(db.RetentionPolicies) == 0 {
		if err := data.CreateRetentionPolicy(name, rpi, true); err != nil {
			return nil, err
		}
	} else if !spec.Matches(db.RetentionPolicy(rpi.Name)) {
		// In this case we already have a retention policy on the database and
		// the provided retention policy does not match it. Therefore, this call
		// is not idempotent and we need to return an error.
		return nil, ErrRetentionPolicyConflict
	}

	// If a non-default retention policy was passed in that already exists then
	// it's an error regardless of if the exact same retention policy is
	// provided. CREATE DATABASE WITH RETENTION POLICY should only be used to
	// create DEFAULT retention policies.
	if db.DefaultRetentionPolicy != rpi.Name {
		return nil, ErrRetentionPolicyConflict
	}

	// Commit the changes.
	if err := c.commit(data); err != nil {
		return nil, err
	}

	// Refresh the database info.
	db = data.Database(name)

	return db, nil
}

// DropDatabase deletes a database.
func (c *Client) DropDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropDatabase(name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (c *Client) CreateRetentionPolicy(database string, spec *RetentionPolicySpec, makeDefault bool) (*RetentionPolicyInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	rp := spec.NewRetentionPolicyInfo()
	if err := data.CreateRetentionPolicy(database, rp, makeDefault); err != nil {
		return nil, err
	}

	if err := c.commit(data); err != nil {
		return nil, err
	}

	return rp, nil
}

// RetentionPolicy returns the requested retention policy info.
func (c *Client) RetentionPolicy(database, name string) (rpi *RetentionPolicyInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db := c.cacheData.Database(database)
	if db == nil {
		return nil, influxdb.ErrDatabaseNotFound(database)
	}

	return db.RetentionPolicy(name), nil
}

// DropRetentionPolicy drops a retention policy from a database.
func (c *Client) DropRetentionPolicy(database, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropRetentionPolicy(database, name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// UpdateRetentionPolicy updates a retention policy.
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate, makeDefault bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.UpdateRetentionPolicy(database, name, rpu, makeDefault); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// Users returns a slice of UserInfo representing the currently known users.
func (c *Client) Users() []UserInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	users := c.cacheData.Users

	if users == nil {
		return []UserInfo{}
	}
	return users
}

// User returns the user with the given name, or ErrUserNotFound.
func (c *Client) User(name string) (User, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, u := range c.cacheData.Users {
		if u.Name == name {
			return &u, nil
		}
	}

	return nil, ErrUserNotFound
}

// bcryptCost is the cost associated with generating password with bcrypt.
// This setting is lowered during testing to improve test suite performance.
var bcryptCost = bcrypt.DefaultCost

// hashWithSalt returns a salted hash of password using salt.
func (c *Client) hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	hasher.Write(salt)
	hasher.Write([]byte(password))
	return hasher.Sum(nil)
}

// saltedHash returns a salt and salted hash of password.
func (c *Client) saltedHash(password string) (salt, hash []byte, err error) {
	salt = make([]byte, SaltBytes)
	if _, err := io.ReadFull(crand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, c.hashWithSalt(salt, password), nil
}

// CreateUser adds a user with the given name and password and admin status.
func (c *Client) CreateUser(name, password string, admin bool) (User, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	// See if the user already exists.
	if u := data.user(name); u != nil {
		if err := bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte(password)); err != nil || u.Admin != admin {
			return nil, ErrUserExists
		}
		return u, nil
	}

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return nil, err
	}

	if err := data.CreateUser(name, string(hash), admin); err != nil {
		return nil, err
	}

	u := data.user(name)

	if err := c.commit(data); err != nil {
		return nil, err
	}

	return u, nil
}

// UpdateUser updates the password of an existing user.
func (c *Client) UpdateUser(name, password string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return err
	}

	if err := data.UpdateUser(name, string(hash)); err != nil {
		return err
	}

	delete(c.authCache, name)

	return c.commit(data)
}

// DropUser removes the user with the given name.
func (c *Client) DropUser(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropUser(name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// SetPrivilege sets a privilege for the given user on the given database.
func (c *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.SetPrivilege(username, database, p); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// SetAdminPrivilege sets or unsets admin privilege to the given username.
func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.SetAdminPrivilege(username, admin); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// UserPrivileges returns the privileges for a user mapped by database name.
func (c *Client) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	p, err := c.cacheData.UserPrivileges(username)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// UserPrivilege returns the privilege for the given user on the given database.
func (c *Client) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	p, err := c.cacheData.UserPrivilege(username, database)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// AdminUserExists returns true if any user has admin privilege.
func (c *Client) AdminUserExists() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.AdminUserExists()
}

// Authenticate returns a UserInfo if the username and password match an existing entry.
func (c *Client) Authenticate(username, password string) (User, error) {
	// Find user.
	c.mu.RLock()
	userInfo := c.cacheData.user(username)
	c.mu.RUnlock()
	if userInfo == nil {
		return nil, ErrUserNotFound
	}

	// Check the local auth cache first.
	c.mu.RLock()
	au, ok := c.authCache[username]
	c.mu.RUnlock()
	if ok {
		// verify the password using the cached salt and hash
		if bytes.Equal(c.hashWithSalt(au.salt, password), au.hash) {
			return userInfo, nil
		}

		// fall through to requiring a full bcrypt hash for invalid passwords
	}

	// Compare password with user hash.
	if err := bcrypt.CompareHashAndPassword([]byte(userInfo.Hash), []byte(password)); err != nil {
		return nil, ErrAuthenticate
	}

	// generate a salt and hash of the password for the cache
	salt, hashed, err := c.saltedHash(password)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.authCache[username] = authUser{salt: salt, hash: hashed, bhash: userInfo.Hash}
	c.mu.Unlock()
	return userInfo, nil
}

// UserCount returns the number of users stored.
func (c *Client) UserCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.cacheData.Users)
}

// ShardIDs returns a list of all shard ids.
func (c *Client) ShardIDs() []uint64 {
	c.mu.RLock()

	var a []uint64
	for _, dbi := range c.cacheData.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, si := range sgi.Shards {
					a = append(a, si.ID)
				}
			}
		}
	}
	c.mu.RUnlock()
	sort.Sort(uint64Slice(a))
	return a
}

// ShardGroupsByTimeRange returns a list of all shard groups on a database and policy that may contain data
// for the specified time range. Shard groups are sorted by start time.
func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []ShardGroupInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Find retention policy.
	rpi, err := c.cacheData.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(policy)
	}
	groups := make([]ShardGroupInfo, 0, len(rpi.ShardGroups))
	for _, g := range rpi.ShardGroups {
		if g.Deleted() || !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// ShardsByTimeRange returns a slice of shards that may contain data in the time range.
func (c *Client) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []ShardInfo, err error) {
	m := make(map[*ShardInfo]struct{})
	for _, mm := range sources.Measurements() {
		groups, err := c.ShardGroupsByTimeRange(mm.Database, mm.RetentionPolicy, tmin, tmax)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			for i := range g.Shards {
				m[&g.Shards[i]] = struct{}{}
			}
		}
	}

	a = make([]ShardInfo, 0, len(m))
	for sh := range m {
		a = append(a, *sh)
	}

	return a, nil
}

// DropShard deletes a shard by ID.
func (c *Client) DropShard(id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()
	data.DropShard(id)
	return c.commit(data)
}

// TruncateShardGroups truncates any shard group that could contain timestamps beyond t.
func (c *Client) TruncateShardGroups(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()
	data.TruncateShardGroups(t)
	return c.commit(data)
}

// PruneShardGroups remove deleted shard groups from the data store.
func (c *Client) PruneShardGroups() error {
	expiration := time.Now().Add(ShardGroupDeletedExpiration)
	c.mu.Lock()
	defer c.mu.Unlock()
	data := c.cacheData.Clone()
	changed := data.PruneShardGroups(expiration)
	if changed {
		return c.commit(data)
	}
	return nil
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (c *Client) CreateShardGroup(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	// Check under a read-lock
	c.mu.RLock()
	if sg, _ := c.cacheData.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		c.mu.RUnlock()
		return sg, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check again under the write lock
	data := c.cacheData.Clone()
	if sg, _ := data.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		return sg, nil
	}

	sgi, err := createShardGroup(data, database, policy, timestamp)
	if err != nil {
		return nil, err
	}

	if err := c.commit(data); err != nil {
		return nil, err
	}

	return sgi, nil
}

func createShardGroup(data *Data, database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	// It is the responsibility of the caller to check if it exists before calling this method.
	if sg, _ := data.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		return nil, ErrShardGroupExists
	}

	if err := data.CreateShardGroup(database, policy, timestamp); err != nil {
		return nil, err
	}

	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, errors.New("retention policy deleted after shard group created")
	}

	sgi := rpi.ShardGroupByTimestamp(timestamp)
	return sgi, nil
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DeleteShardGroup(database, policy, id); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

type ShardGroupFullInfo struct {
	ShardGroup      *ShardGroupInfo
	Database        string
	RetentionPolicy string
}

// PrecreateShardGroups creates shard groups whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
func (c *Client) PrecreateShardGroups(from, to time.Time) ([]ShardGroupFullInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data := c.cacheData.Clone()
	var changed bool

	// return nil if no shard groups are created
	var newShardGroups []ShardGroupFullInfo = nil
	for _, di := range data.Databases {
		for _, rp := range di.RetentionPolicies {
			if len(rp.ShardGroups) == 0 {
				// No data was ever written to this group, or all groups have been deleted.
				continue
			}
			g := rp.ShardGroups[len(rp.ShardGroups)-1] // Get the last group in time.
			if !g.Deleted() && g.EndTime.Before(to) && g.EndTime.After(from) {
				// Group is not deleted, will end before the future time, but is still yet to expire.
				// This last check is important, so the system doesn't create shards groups wholly
				// in the past.

				// Create successive shard group.
				nextShardGroupTime := g.EndTime.Add(1 * time.Nanosecond)
				// if it already exists, continue
				if sg, _ := data.ShardGroupByTimestamp(di.Name, rp.Name, nextShardGroupTime); sg != nil {
					c.logger.Info("Shard group already exists",
						logger.ShardGroup(sg.ID),
						logger.Database(di.Name),
						logger.RetentionPolicy(rp.Name))
					continue
				}
				newGroup, err := createShardGroup(data, di.Name, rp.Name, nextShardGroupTime)
				if err != nil {
					c.logger.Info("Failed to precreate successive shard group",
						zap.Uint64("group_id", g.ID), zap.Error(err))
					continue
				}
				newShardGroups = append(newShardGroups, ShardGroupFullInfo{ShardGroup: newGroup, Database: di.Name, RetentionPolicy: rp.Name})
				changed = true
				c.logger.Info("New shard group successfully precreated",
					logger.ShardGroup(newGroup.ID),
					logger.Database(di.Name),
					logger.RetentionPolicy(rp.Name))
			}
		}
	}

	if changed {
		if err := c.commit(data); err != nil {
			return nil, err
		}
	}
	return newShardGroups, nil
}

// ShardOwner returns the owning shard group info for a specific shard.
func (c *Client) ShardOwner(shardID uint64) (database, policy string, sgi *ShardGroupInfo) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, dbi := range c.cacheData.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, g := range rpi.ShardGroups {
				if g.Deleted() {
					continue
				}

				for _, sh := range g.Shards {
					if sh.ID == shardID {
						database = dbi.Name
						policy = rpi.Name
						sgi = &g
						return
					}
				}
			}
		}
	}
	return
}

// CreateContinuousQuery saves a continuous query with the given name for the given database.
func (c *Client) CreateContinuousQuery(database, name, query string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.CreateContinuousQuery(database, name, query); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// DropContinuousQuery removes the continuous query with the given name on the given database.
func (c *Client) DropContinuousQuery(database, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropContinuousQuery(database, name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// CreateSubscription creates a subscription against the given database and retention policy.
func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.CreateSubscription(database, rp, name, mode, destinations); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// DropSubscription removes the named subscription from the given database and retention policy.
func (c *Client) DropSubscription(database, rp, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropSubscription(database, rp, name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// SetData overwrites the underlying data in the meta store.
func (c *Client) SetData(data *Data) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	d := data.Clone()

	if err := c.commit(d); err != nil {
		return err
	}

	return nil
}

// Data returns a clone of the underlying data in the meta store.
func (c *Client) Data() Data {
	c.mu.RLock()
	defer c.mu.RUnlock()
	d := c.cacheData.Clone()
	return *d
}

// WaitForDataChanged returns a channel that will get closed when
// the metastore data has changed.
func (c *Client) WaitForDataChanged() chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.changed
}

// commit writes data to the underlying store.
// This method assumes c's mutex is already locked.
func (c *Client) commit(data *Data) error {
	data.Index++

	// try to write to disk before updating in memory
	if err := snapshot(c.path, data); err != nil {
		return err
	}

	// update in memory
	c.cacheData = data

	// close channels to signal changes
	close(c.changed)
	c.changed = make(chan struct{})

	return nil
}

// MarshalBinary returns a binary representation of the underlying data.
func (c *Client) MarshalBinary() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.MarshalBinary()
}

// WithLogger sets the logger for the client.
func (c *Client) WithLogger(log *zap.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger = log.With(zap.String("service", "metaclient"))
}

// snapshot saves the current meta data to disk.
func snapshot(path string, data *Data) error {
	filename := filepath.Join(path, metaFile)
	tmpFile := filename + "tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer f.Close()

	var d []byte
	if b, err := data.MarshalBinary(); err != nil {
		return err
	} else {
		d = b
	}

	if _, err := f.Write(d); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	//close file handle before renaming to support Windows
	if err = f.Close(); err != nil {
		return err
	}

	return file.RenameFile(tmpFile, filename)
}

// Load loads the current meta data from disk.
func (c *Client) Load() error {
	file := filepath.Join(c.path, metaFile)

	f, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	if err := c.cacheData.UnmarshalBinary(data); err != nil {
		return err
	}
	return nil
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
