package internal

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

// MetaClientMock is a mockable implementation of meta.MetaClient.
type MetaClientMock struct {
	CloseFn                             func() error
	CreateContinuousQueryFn             func(database, name, query string) error
	CreateDatabaseFn                    func(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseWithRetentionPolicyFn func(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	CreateRetentionPolicyFn             func(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
	CreateShardGroupFn                  func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	CreateSubscriptionFn                func(database, rp, name, mode string, destinations []string) error
	CreateUserFn                        func(name, password string, admin bool) (meta.User, error)

	DatabaseFn  func(name string) *meta.DatabaseInfo
	DatabasesFn func() []meta.DatabaseInfo

	DataFn                func() meta.Data
	DeleteShardGroupFn    func(database string, policy string, id uint64) error
	DropContinuousQueryFn func(database, name string) error
	DropDatabaseFn        func(name string) error
	DropRetentionPolicyFn func(database, name string) error
	DropSubscriptionFn    func(database, rp, name string) error
	DropShardFn           func(id uint64) error
	DropUserFn            func(name string) error

	OpenFn func() error

	PruneShardGroupsFn func() error

	RetentionPolicyFn func(database, name string) (rpi *meta.RetentionPolicyInfo, err error)

	AuthenticateFn           func(username, password string) (ui meta.User, err error)
	AdminUserExistsFn        func() bool
	SetAdminPrivilegeFn      func(username string, admin bool) error
	SetDataFn                func(*meta.Data) error
	SetPrivilegeFn           func(username, database string, p influxql.Privilege) error
	ShardGroupsByTimeRangeFn func(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	ShardOwnerFn             func(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo)
	UpdateRetentionPolicyFn  func(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
	UpdateUserFn             func(name, password string) error
	UserPrivilegeFn          func(username, database string) (*influxql.Privilege, error)
	UserPrivilegesFn         func(username string) (map[string]influxql.Privilege, error)
	UserFn                   func(username string) (meta.User, error)
	UsersFn                  func() []meta.UserInfo
}

// Close a MetaClientMock
func (c *MetaClientMock) Close() error {
	return c.CloseFn()
}

// CreateContinuousQuery creates a mock of MetaClient.CreateContinuousQuery
func (c *MetaClientMock) CreateContinuousQuery(database, name, query string) error {
	return c.CreateContinuousQueryFn(database, name, query)
}

// CreateDatabase creates a mock of MetaClient.CreateDatabase
func (c *MetaClientMock) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	return c.CreateDatabaseFn(name)
}

// CreateDatabaseWithRetentionPolicy creates a mock of MetaClient.CreateDatabaseWithRetentionPolicy
func (c *MetaClientMock) CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
	return c.CreateDatabaseWithRetentionPolicyFn(name, spec)
}

// CreateRetentionPolicy creates a mock of MetaClient.CreateDatabaseWithRetentionPolicy
func (c *MetaClientMock) CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error) {
	return c.CreateRetentionPolicyFn(database, spec, makeDefault)
}

// CreateShardGroup creates a mock of MetaClient.CreateDatabaseWithRetentionPolicy
func (c *MetaClientMock) CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	return c.CreateShardGroupFn(database, policy, timestamp)
}

// CreateSubscription creates a mock of MetaClient.CreateSubscription
func (c *MetaClientMock) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return c.CreateSubscriptionFn(database, rp, name, mode, destinations)
}

// CreateUser creates a mock of MetaClient.CreateUser
func (c *MetaClientMock) CreateUser(name, password string, admin bool) (meta.User, error) {
	return c.CreateUserFn(name, password, admin)
}

// Database creates a mock of MetaClient.Database
func (c *MetaClientMock) Database(name string) *meta.DatabaseInfo {
	return c.DatabaseFn(name)
}

// Databases creates a mock of MetaClient.Databases
func (c *MetaClientMock) Databases() []meta.DatabaseInfo {
	return c.DatabasesFn()
}

// DeleteShardGroup creates a mock of MetaClient.DeleteShardGroup
func (c *MetaClientMock) DeleteShardGroup(database string, policy string, id uint64) error {
	return c.DeleteShardGroupFn(database, policy, id)
}

// DropContinuousQuery creates a mock of MetaClient.DropContinuousQuery
func (c *MetaClientMock) DropContinuousQuery(database, name string) error {
	return c.DropContinuousQueryFn(database, name)
}

// DropDatabase creates a mock of MetaClient.DropDatabase
func (c *MetaClientMock) DropDatabase(name string) error {
	return c.DropDatabaseFn(name)
}

// DropRetentionPolicy creates a mock of MetaClient.DropRetentionPolicy
func (c *MetaClientMock) DropRetentionPolicy(database, name string) error {
	return c.DropRetentionPolicyFn(database, name)
}

// DropShard creates a mock of MetaClient.DropShard
func (c *MetaClientMock) DropShard(id uint64) error {
	return c.DropShardFn(id)
}

// DropSubscription creates a mock of MetaClient.DropSubscription
func (c *MetaClientMock) DropSubscription(database, rp, name string) error {
	return c.DropSubscriptionFn(database, rp, name)
}

// DropUser creates a mock of MetaClient.DropUser
func (c *MetaClientMock) DropUser(name string) error {
	return c.DropUserFn(name)
}

// RetentionPolicy creates a mock of MetaClient.RetentionPolicy
func (c *MetaClientMock) RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error) {
	return c.RetentionPolicyFn(database, name)
}

// SetAdminPrivilege creates a mock of MetaClient.SetAdminPrivilege
func (c *MetaClientMock) SetAdminPrivilege(username string, admin bool) error {
	return c.SetAdminPrivilegeFn(username, admin)
}

// SetPrivilege creates a mock of MetaClient.SetPrivilege
func (c *MetaClientMock) SetPrivilege(username, database string, p influxql.Privilege) error {
	return c.SetPrivilegeFn(username, database, p)
}

// ShardGroupsByTimeRange creates a mock of MetaClient.ShardGroupsByTimeRange
func (c *MetaClientMock) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	return c.ShardGroupsByTimeRangeFn(database, policy, min, max)
}

// ShardOwner creates a mock of MetaClient.ShardOwner
func (c *MetaClientMock) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	return c.ShardOwnerFn(shardID)
}

// UpdateRetentionPolicy creates a mock of MetClient.UpdateRetentionPolicy
func (c *MetaClientMock) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error {
	return c.UpdateRetentionPolicyFn(database, name, rpu, makeDefault)
}

// UpdateUser creates a mock of MetaClient.UpdateUser
func (c *MetaClientMock) UpdateUser(name, password string) error {
	return c.UpdateUserFn(name, password)
}

// UserPrivilege creates a mock of MetaClient.UserPrivilege
func (c *MetaClientMock) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	return c.UserPrivilegeFn(username, database)
}

// UserPrivileges creates a mock of MetaClient.UserPrivileges
func (c *MetaClientMock) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	return c.UserPrivilegesFn(username)
}

// Authenticate creates a mock of MetaClient.Authenticate
func (c *MetaClientMock) Authenticate(username, password string) (meta.User, error) {
	return c.AuthenticateFn(username, password)
}

// AdminUserExists creates a mock of MetaClient.AdminUserExists
func (c *MetaClientMock) AdminUserExists() bool { return c.AdminUserExistsFn() }

// User creates a mock of MetaClient.User
func (c *MetaClientMock) User(username string) (meta.User, error) { return c.UserFn(username) }

// Users creates a mock of MetaClient.Users
func (c *MetaClientMock) Users() []meta.UserInfo { return c.UsersFn() }

// Open creates a mock of MetaClient.Open
func (c *MetaClientMock) Open() error { return c.OpenFn() }

// Data creates a mock of MetaClient.Data
func (c *MetaClientMock) Data() meta.Data { return c.DataFn() }

// SetData creates a mock of MetaClient.SetData
func (c *MetaClientMock) SetData(d *meta.Data) error { return c.SetDataFn(d) }

// PruneShardGroups creates a mock of MetaClient.PruneShardGroups
func (c *MetaClientMock) PruneShardGroups() error { return c.PruneShardGroupsFn() }
