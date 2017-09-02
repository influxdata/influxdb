// Package meta provides control over meta data for InfluxDB,
// such as controlling databases, retention policies, users, etc.
package meta

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/uber-go/zap"

	"golang.org/x/crypto/bcrypt"
)

const (
	// SaltBytes is the number of bytes used for salts.
	SaltBytes = 32

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

type StorageService interface {
	Load() (*Data, error)

	// Node
	AddNode(node *NodeInfo) error
	UpdateNode(node *NodeInfo) error
	GetNode(nodeID string) (*NodeInfo, error)
	GetNodes() ([]*NodeInfo, error)
	DeleteNode(nodeID string) error
	DeleteNodes() error

	// Database
	AddDatabase(db *DatabaseInfo) error
	UpdateDatabase(db *DatabaseInfo) error
	GetDatabase(dbName string) (*DatabaseInfo, error)
	GetDatabases() ([]*DatabaseInfo, error)
	DeleteDatabase(dbName string) error
	DeleteDatabases() error

	// Continuous query
	AddContinuousQuery(dbName string, cq *ContinuousQueryInfo) error
	UpdateContinuousQuery(dbName string, cq *ContinuousQueryInfo) error
	GetContinuousQuery(dbName, cqName string) (*ContinuousQueryInfo, error)
	GetContinuousQueries(dbName string) ([]*ContinuousQueryInfo, error)
	DeleteContinuousQuery(dbName, cqName string) error
	DeleteContinuousQueries(dbName string) error

	// Retention policy
	AddRetentionPolicy(dbName string, rp *RetentionPolicyInfo) error
	UpdateRetentionPolicy(dbName string, rp *RetentionPolicyInfo) error
	GetRetentionPolicy(dbName, rpName string) (*RetentionPolicyInfo, error)
	GetRetentionPolicies(dbName string) ([]*RetentionPolicyInfo, error)
	DeleteRetentionPolicy(dbName, rpName string) error
	DeleteRetentionPolicies(dbName string) error

	// ShardGroup
	AddShardGroup(dbName, rpName string, sg *ShardGroupInfo) error
	UpdateShardGroup(dbName, rpName string, sg *ShardGroupInfo) error
	GetShardGroup(dbName, rpName, sgID string) (*ShardGroupInfo, error)
	GetShardGroups(dbName, rpName string) ([]*ShardGroupInfo, error)
	DeleteShardGroup(dbName, rpName, sgID string) error
	DeleteShardGroups(dbName, rpName string) error

	// Subscription
	AddSubscription(dbName, rpName string, sub *SubscriptionInfo) error
	UpdateSubscription(dbName, rpName string, sub *SubscriptionInfo) error
	GetSubscription(dbName, rpName, subName string) (*SubscriptionInfo, error)
	GetSubscriptions(dbName, rpName string) ([]*SubscriptionInfo, error)
	DeleteSubscription(dbName, rpName, subName string) error
	DeleteSubscriptions(dbName, rpName string) error

	// User
	AddUser(user *UserInfo) error
	UpdateUser(user *UserInfo) error
	GetUser(userName string) (*UserInfo, error)
	GetUsers() ([]*UserInfo, error)
	DeleteUser(userName string) error
	DeleteUsers() error
}

// Client is used to execute commands on and read data from
// a meta service cluster.
type Client struct {
	logger zap.Logger

	mu        sync.RWMutex
	closing   chan struct{}
	changed   chan struct{}
	cacheData *Data

	config *Config
	// Authentication cache.
	authCache map[string]authUser

	// meta storage service
	storage StorageService

	retentionAutoCreate bool
}

type authUser struct {
	bhash string
	salt  []byte
	hash  []byte
}

// NewClient returns a new *Client.
func NewClient(config *Config) (*Client, error) {
	storage, err := NewEtcdStorageService(config)
	if err != nil {
		return nil, err
	}

	return &Client{
		config: config,
		cacheData: &Data{
			ClusterID: uint64(rand.Int63()),
			Index:     1,
		},
		closing:             make(chan struct{}),
		changed:             make(chan struct{}),
		logger:              zap.New(zap.NullEncoder()),
		authCache:           make(map[string]authUser, 0),
		storage:             storage,
		retentionAutoCreate: config.RetentionAutoCreate,
	}, nil
}

// Open a connection to a meta service cluster.
func (c *Client) Open() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Register itself
	node := &NodeInfo{
		ID:      c.config.NodeID,
		Host:    c.config.NodeHost,
		TCPHost: c.config.NodeTCPHost,
	}
	err := c.storage.AddNode(node)
	if err != nil {
		return err
	}

	// Try to load from storage service
	return c.Load()
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

// Nodes returns a list of all nodes infos.
func (c *Client) Nodes() map[uint64]*NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.cacheData.Nodes == nil {
		return make(map[uint64]*NodeInfo)
	}

	return c.cacheData.Nodes
}

// Database returns info for the requested database.
func (c *Client) Database(name string) *DatabaseInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cacheData.Database(name)
}

// Databases returns a list of all database infos.
func (c *Client) Databases() map[string]*DatabaseInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.cacheData.Databases == nil {
		return make(map[string]*DatabaseInfo)
	}

	return c.cacheData.Databases
}

// FIXME, redesign
// CreateDatabase creates a database or returns it if it already exists.
func (c *Client) CreateDatabase(name string) (*DatabaseInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if db := c.cacheData.Database(name); db != nil {
		return db, nil
	}

	db, err := c.cacheData.CreateDatabase(name)
	if err != nil {
		return nil, err
	}

	c.cacheData.CommitDatabase(db)

	var lastErr error
	defer func() {
		if lastErr != nil {
			c.cacheData.DropDatabase(name)
		}
	}()

	// create default retention policy
	var rpi *RetentionPolicyInfo
	if c.retentionAutoCreate {
		rpi = DefaultRetentionPolicyInfo()
		rpi, err := c.cacheData.ValidateRetentionPolicy(name, rpi)
		if err != nil {
			lastErr = err
			return nil, err
		}

		db, err := c.cacheData.UpdateDefaultRetentionPolicy(name, rpi, true)
		if err != nil {
			lastErr = err
			return nil, err
		}

		c.cacheData.CommitRetentionPolicy(name, rpi)

		// Since the default rp may change
		c.cacheData.CommitDatabase(db)
	}

	// Commit to storage, transaction
	db = c.cacheData.Database(name)
	if err := c.storage.AddDatabase(db); err != nil {
		lastErr = err
		return nil, err
	}

	if rpi != nil {
		if err := c.storage.AddRetentionPolicy(name, rpi); err != nil {
			c.storage.DeleteDatabase(name)
			lastErr = err
			return nil, err
		}
	}

	return db, nil
}

// FIXME, redesign
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
//
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, spec *RetentionPolicySpec) (*DatabaseInfo, error) {
	if spec == nil {
		return nil, errors.New("CreateDatabaseWithRetentionPolicy called with nil spec")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	newDB := false
	db := c.cacheData.Database(name)
	if db == nil {
		d, err := c.cacheData.CreateDatabase(name)
		if err != nil {
			return nil, err
		}
		c.cacheData.CommitDatabase(d)
		db = d
		newDB = true
	}

	var lastErr error
	defer func() {
		if newDB && lastErr != nil {
			c.cacheData.DropDatabase(name)
		}
	}()

	// No existing retention policies, so we can create the provided policy as
	// the new default policy.
	rpi := spec.NewRetentionPolicyInfo()
	if len(db.RetentionPolicies) == 0 {
		if _, err := c.cacheData.ValidateRetentionPolicy(name, rpi); err != nil {
			lastErr = err
			return nil, err
		}

		d, err := c.cacheData.UpdateDefaultRetentionPolicy(name, rpi, true)
		if err != nil {
			lastErr = err
			return nil, err
		}

		db = d
	} else if !spec.Matches(db.RetentionPolicy(rpi.Name)) {
		// In this case we already have a retention policy on the database and
		// the provided retention policy does not match it. Therefore, this call
		// is not idempotent and we need to return an error.
		lastErr = ErrRetentionPolicyConflict
		return nil, ErrRetentionPolicyConflict
	}

	// If a non-default retention policy was passed in that already exists then
	// it's an error regardless of if the exact same retention policy is
	// provided. CREATE DATABASE WITH RETENTION POLICY should only be used to
	// create DEFAULT retention policies.
	if db.DefaultRetentionPolicy != rpi.Name {
		lastErr = ErrRetentionPolicyConflict
		return nil, ErrRetentionPolicyConflict
	}

	// Since the default rp may change
	c.cacheData.CommitDatabase(db)
	c.cacheData.CommitRetentionPolicy(name, rpi)

	// Commit to storage
	if newDB {
		if err := c.storage.AddDatabase(db); err != nil {
			lastErr = err
			return nil, err
		}
	}

	if err := c.storage.UpdateRetentionPolicy(name, rpi); err != nil {
		lastErr = err
		if newDB {
			c.storage.DeleteDatabase(name)
		}
	}

	return db, nil
}

// DropDatabase deletes a database.
func (c *Client) DropDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.storage.DeleteDatabase(name); err != nil {
		return err
	}

	if err := c.cacheData.DropDatabase(name); err != nil {
		return err
	}

	return nil
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (c *Client) CreateRetentionPolicy(database string, spec *RetentionPolicySpec, makeDefault bool) (*RetentionPolicyInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	rp := spec.NewRetentionPolicyInfo()
	if _, err := c.cacheData.ValidateRetentionPolicy(database, rp); err != nil {
		return nil, err
	}

	var updatedDB *DatabaseInfo
	if makeDefault {
		db, err := c.cacheData.UpdateDefaultRetentionPolicy(database, rp, makeDefault)
		if err != nil {
			return nil, err
		}
		updatedDB = db
	}

	// commit to stroage
	if err := c.storage.AddRetentionPolicy(database, rp); err != nil {
		return nil, err
	}

	if updatedDB != nil {
		// Default rp has changed
		if err := c.storage.UpdateDatabase(updatedDB); err != nil {
			c.storage.DeleteRetentionPolicy(database, rp.Name)
			return nil, err
		}
		c.cacheData.CommitDatabase(updatedDB)
	}

	c.cacheData.CommitRetentionPolicy(database, rp)

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

	if err := c.storage.DeleteRetentionPolicy(database, name); err != nil {
		return err
	}

	if err := c.cacheData.DropRetentionPolicy(database, name); err != nil {
		return err
	}

	return nil
}

// UpdateRetentionPolicy updates a retention policy.
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate, makeDefault bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	rpi, err := c.cacheData.UpdateRetentionPolicy(database, name, rpu)
	if err != nil {
		return err
	}

	var updatedDB *DatabaseInfo
	if makeDefault {
		db, err := c.cacheData.UpdateDefaultRetentionPolicy(database, rpi, makeDefault)
		if err != nil {
			return err
		}
		updatedDB = db
	}

	// commit to storage
	if err := c.storage.UpdateRetentionPolicy(database, rpi); err != nil {
		return err
	}

	if updatedDB != nil {
		if err := c.storage.UpdateDatabase(updatedDB); err != nil {
			return err
		}
		c.cacheData.CommitDatabase(updatedDB)
	}
	c.cacheData.CommitRetentionPolicy(database, rpi)

	return nil
}

// Users returns a slice of UserInfo representing the currently known users.
func (c *Client) Users() map[string]*UserInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	users := c.cacheData.Users

	if users == nil {
		return make(map[string]*UserInfo)
	}
	return users
}

// User returns the user with the given name, or ErrUserNotFound.
func (c *Client) User(name string) (User, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	u := c.cacheData.User(name)
	if u == nil {
		return nil, ErrUserNotFound
	}
	return u, nil
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

	// See if the user already exists.
	if u := c.cacheData.user(name); u != nil {
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

	u, err := c.cacheData.CreateUser(name, string(hash), admin)
	if err != nil {
		return nil, err
	}

	if err := c.storage.AddUser(u); err != nil {
		return nil, err
	}

	c.cacheData.CommitUser(u)

	return u, nil
}

// UpdateUser updates the password of an existing user.
func (c *Client) UpdateUser(name, password string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return err
	}

	u, err := c.cacheData.UpdateUser(name, string(hash))
	if err != nil {
		return err
	}

	if err := c.storage.UpdateUser(u); err != nil {
		return err
	}

	delete(c.authCache, name)

	c.cacheData.CommitUser(u)

	return nil
}

// DropUser removes the user with the given name.
func (c *Client) DropUser(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.storage.DeleteUser(name); err != nil {
		return err
	}

	if err := c.cacheData.DropUser(name); err != nil {
		return err
	}

	return nil
}

// SetPrivilege sets a privilege for the given user on the given database.
func (c *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	updatedUser, err := c.cacheData.SetPrivilege(username, database, p)
	if err != nil {
		return err
	}

	if err := c.storage.UpdateUser(updatedUser); err != nil {
		return err
	}

	c.cacheData.CommitUser(updatedUser)

	return nil
}

// SetAdminPrivilege sets or unsets admin privilege to the given username.
func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	updatedUser, err := c.cacheData.SetAdminPrivilege(username, admin)
	if err != nil {
		return err
	}

	if err := c.storage.UpdateUser(updatedUser); err != nil {
		return err
	}

	c.cacheData.CommitUser(updatedUser)
	c.cacheData.SetAdminUserExists()

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
func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []*ShardGroupInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Find retention policy.
	rpi, err := c.cacheData.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(policy)
	}
	groups := make([]*ShardGroupInfo, 0, len(rpi.ShardGroups))
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

	database, rpName, sgi := c.cacheData.DropShard(id)

	if sgi != nil {
		if err := c.storage.UpdateShardGroup(database, rpName, sgi); err != nil {
			return err
		}
	}
	return nil
}

// PruneShardGroups remove deleted shard groups from the data store.
func (c *Client) PruneShardGroups() error {
	expiration := time.Now().Add(ShardGroupDeletedExpiration)
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, d := range c.cacheData.Databases {
		for _, rp := range d.RetentionPolicies {
			var remainingShardGroups []*ShardGroupInfo
			for _, sgi := range rp.ShardGroups {
				if sgi.DeletedAt.IsZero() || !expiration.After(sgi.DeletedAt) {
					remainingShardGroups = append(remainingShardGroups, sgi)
					continue
				}
			}
			rp.ShardGroups = remainingShardGroups
			if err := c.storage.UpdateRetentionPolicy(d.Name, rp); err != nil {
				return err
			}
		}
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
	if sg, _ := c.cacheData.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		return sg, nil
	}

	sgi, err := c.cacheData.CreateShardGroup(database, policy, timestamp)
	if err != nil {
		return nil, err
	}

	if err := c.storage.AddShardGroup(database, policy, sgi); err != nil {
		return nil, err
	}

	c.cacheData.CommitShardGroup(database, policy, sgi)

	return sgi, nil
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	sgi, err := c.cacheData.ShardGroup(database, policy, id)
	if err != nil {
		return err
	}

	updatedSgi := *sgi
	// just set DeleteAt for sgi
	if err := c.cacheData.DeleteShardGroup(database, policy, &updatedSgi); err != nil {
		return err
	}

	if err := c.storage.UpdateShardGroup(database, policy, &updatedSgi); err != nil {
		return err
	}
	c.cacheData.CommitShardGroup(database, policy, &updatedSgi)

	return nil
}

// PrecreateShardGroups creates shard groups whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, di := range c.cacheData.Databases {
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
				if sg, _ := c.cacheData.ShardGroupByTimestamp(di.Name, rp.Name, nextShardGroupTime); sg != nil {
					c.logger.Info(fmt.Sprintf("shard group %d exists for database %s, retention policy %s", sg.ID, di.Name, rp.Name))
					continue
				}

				newGroup, err := c.cacheData.CreateShardGroup(di.Name, rp.Name, nextShardGroupTime)
				if err != nil {
					c.logger.Info(fmt.Sprintf("failed to precreate successive shard group for group %d: %s", g.ID, err.Error()))
					continue
				}

				if err := c.storage.AddShardGroup(di.Name, rp.Name, newGroup); err != nil {
					c.logger.Info(fmt.Sprintf("failed to precreate successive shard group for group %d: %s", g.ID, err.Error()))
					continue
				}
				c.cacheData.CommitShardGroup(di.Name, rp.Name, newGroup)
				c.logger.Info(fmt.Sprintf("new shard group %d successfully precreated for database %s, retention policy %s", newGroup.ID, di.Name, rp.Name))
			}
		}
	}

	return nil
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
						sgi = g
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

	cq, err := c.cacheData.CreateContinuousQuery(database, name, query)
	if err != nil {
		return err
	}

	if err := c.storage.AddContinuousQuery(database, cq); err != nil {
		return err
	}
	c.cacheData.CommitContinuousQuery(database, cq)

	return nil
}

// DropContinuousQuery removes the continuous query with the given name on the given database.
func (c *Client) DropContinuousQuery(database, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.storage.DeleteContinuousQuery(database, name); err != nil {
		return err
	}

	if err := c.cacheData.DropContinuousQuery(database, name); err != nil {
		return err
	}

	return nil
}

// CreateSubscription creates a subscription against the given database and retention policy.
func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	sub, err := c.cacheData.CreateSubscription(database, rp, name, mode, destinations)
	if err != nil {
		return err
	}

	if err := c.storage.AddSubscription(database, rp, sub); err != nil {
		return err
	}
	c.cacheData.CommitSubscription(database, rp, sub)

	return nil
}

// DropSubscription removes the named subscription from the given database and retention policy.
func (c *Client) DropSubscription(database, rp, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.storage.DeleteSubscription(database, rp, name); err != nil {
		return err
	}

	if err := c.cacheData.DropSubscription(database, rp, name); err != nil {
		return err
	}

	return nil
}

// WaitForDataChanged returns a channel that will get closed when
// the metastore data has changed.
func (c *Client) WaitForDataChanged() chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.changed
}

// MarshalBinary returns a binary representation of the underlying data.
func (c *Client) MarshalBinary() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.MarshalBinary()
}

// WithLogger sets the logger for the client.
func (c *Client) WithLogger(log zap.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger = log.With(zap.String("service", "metaclient"))
}

// Load loads the current meta data storage service.
func (c *Client) Load() error {
	data, err := c.storage.Load()
	if err != nil {
		return err
	}

	c.cacheData = data
	return nil
}

func (c *Client) SetData(data *Data) error {
	return nil
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
