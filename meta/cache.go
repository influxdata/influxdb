package meta

import (
	"sync"
)

// Cache implements a read-through cache to a backing store.
// The cache is invalidated when data is mutated or when the data is updated.
type Cache struct {
	mu    sync.RWMutex
	data  *Data // cached data
	store Store // backing store
}

// SetData sets the top-level metadata on the cache.
func (c *Cache) SetData(data *Data) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ignore datasets with old data.
	if c.data != nil && data.Version < c.data.Version {
		return
	}

	// Update top-level data.
	c.data = data
}

/*
// CreateContinuousQuery creates a new continuous query.
func (c *Cache) CreateContinuousQuery(q *influxql.CreateContinuousQueryStatement) error

// DropContinuousQuery removes a continuous query.
func (c *Cache) DropContinuousQuery(q *influxql.DropContinuousQueryStatement) error

// CreateNode adds a new node to the cluster.
func (c *Cache) CreateNode(host string) error

// DeleteNode removes a node from the cluster.
func (c *Cache) DeleteNode(id uint64) error

// CreateDatabase creates a new database.
func (c *Cache) CreateDatabase(name string) error

// DropDatabase removes a database.
func (c *Cache) DropDatabase(name string) error

// CreateDatabaseIfNotExists creates a new database if it doesn't exist.
func (c *Cache) CreateDatabaseIfNotExists(name string) error

// CreateRetentionPolicy creates a retention policy on a database.
func (c *Cache) CreateRetentionPolicy(database string, rp *RetentionPolicyInfo) error

// CreateRetentionPolicyIfNotExists creates a retention policy on a database.
// Ignored if it already exists.
func (c *Cache) CreateRetentionPolicyIfNotExists(database string, rp *RetentionPolicyInfo) error

// DeleteRetentionPolicy removes a retention policy from a database.
func (c *Cache) DeleteRetentionPolicy(database, name string) error

// SetDefaultRetentionPolicy sets the name of the default retention policy on a database.
func (c *Cache) SetDefaultRetentionPolicy(database, name string) error

// UpdateRetentionPolicy updates an existing retention policy.
func (c *Cache) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) error

// CreateShardGroupIfNotExists creates a shard for a retention policy for a given time.
func (c *Cache) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) error

// CreateUser creates a new user.
func (c *Cache) CreateUser(username, password string, admin bool) error

// DeleteUser removes a user from the system.
func (c *Cache) DeleteUser(username string) error

// UpdateUser updates an existing user.
func (c *Cache) UpdateUser(username, password string) error

// SetPrivilege sets a privilege on a user.
func (c *Cache) SetPrivilege(p influxql.Privilege, username string, dbname string) error

// DeleteShardGroup removes a shard group.
func (c *Cache) DeleteShardGroup(database, policy string, shardID uint64) error
*/
