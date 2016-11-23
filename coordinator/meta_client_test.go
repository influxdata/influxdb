package coordinator_test

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

// MetaClient is a mockable implementation of cluster.MetaClient.
type MetaClient struct {
	CreateContinuousQueryFn             func(database, name, query string) error
	CreateDatabaseFn                    func(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseWithRetentionPolicyFn func(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	CreateRetentionPolicyFn             func(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
	CreateSubscriptionFn                func(database, rp, name, mode string, destinations []string) error
	CreateUserFn                        func(name, password string, admin bool) (*meta.UserInfo, error)
	DatabaseFn                          func(name string) *meta.DatabaseInfo
	DatabasesFn                         func() []meta.DatabaseInfo
	DataNodeFn                          func(id uint64) (*meta.NodeInfo, error)
	DataNodesFn                         func() ([]meta.NodeInfo, error)
	DeleteDataNodeFn                    func(id uint64) error
	DeleteMetaNodeFn                    func(id uint64) error
	DropContinuousQueryFn               func(database, name string) error
	DropDatabaseFn                      func(name string) error
	DropRetentionPolicyFn               func(database, name string) error
	DropSubscriptionFn                  func(database, rp, name string) error
	DropShardFn                         func(id uint64) error
	DropUserFn                          func(name string) error
	MetaNodesFn                         func() ([]meta.NodeInfo, error)
	RetentionPolicyFn                   func(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
	SetAdminPrivilegeFn                 func(username string, admin bool) error
	SetPrivilegeFn                      func(username, database string, p influxql.Privilege) error
	ShardGroupsByTimeRangeFn            func(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	UpdateRetentionPolicyFn             func(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
	UpdateUserFn                        func(name, password string) error
	UserPrivilegeFn                     func(username, database string) (*influxql.Privilege, error)
	UserPrivilegesFn                    func(username string) (map[string]influxql.Privilege, error)
	UsersFn                             func() []meta.UserInfo
}

func (c *MetaClient) CreateContinuousQuery(database, name, query string) error {
	return c.CreateContinuousQueryFn(database, name, query)
}

func (c *MetaClient) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	return c.CreateDatabaseFn(name)
}

func (c *MetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
	return c.CreateDatabaseWithRetentionPolicyFn(name, spec)
}

func (c *MetaClient) CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error) {
	return c.CreateRetentionPolicyFn(database, spec, makeDefault)
}

func (c *MetaClient) DropShard(id uint64) error {
	return c.DropShardFn(id)
}

func (c *MetaClient) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return c.CreateSubscriptionFn(database, rp, name, mode, destinations)
}

func (c *MetaClient) CreateUser(name, password string, admin bool) (*meta.UserInfo, error) {
	return c.CreateUserFn(name, password, admin)
}

func (c *MetaClient) Database(name string) *meta.DatabaseInfo {
	return c.DatabaseFn(name)
}

func (c *MetaClient) Databases() []meta.DatabaseInfo {
	return c.DatabasesFn()
}

func (c *MetaClient) DataNode(id uint64) (*meta.NodeInfo, error) {
	return c.DataNodeFn(id)
}

func (c *MetaClient) DataNodes() ([]meta.NodeInfo, error) {
	return c.DataNodesFn()
}

func (c *MetaClient) DeleteDataNode(id uint64) error {
	return c.DeleteDataNodeFn(id)
}

func (c *MetaClient) DeleteMetaNode(id uint64) error {
	return c.DeleteMetaNodeFn(id)
}

func (c *MetaClient) DropContinuousQuery(database, name string) error {
	return c.DropContinuousQueryFn(database, name)
}

func (c *MetaClient) DropDatabase(name string) error {
	return c.DropDatabaseFn(name)
}

func (c *MetaClient) DropRetentionPolicy(database, name string) error {
	return c.DropRetentionPolicyFn(database, name)
}

func (c *MetaClient) DropSubscription(database, rp, name string) error {
	return c.DropSubscriptionFn(database, rp, name)
}

func (c *MetaClient) DropUser(name string) error {
	return c.DropUserFn(name)
}

func (c *MetaClient) MetaNodes() ([]meta.NodeInfo, error) {
	return c.MetaNodesFn()
}

func (c *MetaClient) RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error) {
	return c.RetentionPolicyFn(database, name)
}

func (c *MetaClient) SetAdminPrivilege(username string, admin bool) error {
	return c.SetAdminPrivilegeFn(username, admin)
}

func (c *MetaClient) SetPrivilege(username, database string, p influxql.Privilege) error {
	return c.SetPrivilegeFn(username, database, p)
}

func (c *MetaClient) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	return c.ShardGroupsByTimeRangeFn(database, policy, min, max)
}

func (c *MetaClient) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error {
	return c.UpdateRetentionPolicyFn(database, name, rpu, makeDefault)
}

func (c *MetaClient) UpdateUser(name, password string) error {
	return c.UpdateUserFn(name, password)
}

func (c *MetaClient) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	return c.UserPrivilegeFn(username, database)
}

func (c *MetaClient) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	return c.UserPrivilegesFn(username)
}

func (c *MetaClient) Users() []meta.UserInfo {
	return c.UsersFn()
}

// DefaultMetaClientDatabaseFn returns a single database (db0) with a retention policy.
func DefaultMetaClientDatabaseFn(name string) *meta.DatabaseInfo {
	return &meta.DatabaseInfo{
		Name: DefaultDatabase,
		DefaultRetentionPolicy: DefaultRetentionPolicy,
	}
}
