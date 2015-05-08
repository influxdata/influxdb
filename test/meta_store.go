package test

import (
	"time"

	"sync/atomic"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

var (
	shardID uint64
)

type MetaStore struct {
<<<<<<< HEAD
	OpenFn  func(path string) error
	CloseFn func() error

	CreateContinuousQueryFn func(query string) (*meta.ContinuousQueryInfo, error)
	DropContinuousQueryFn   func(query string) error
=======
	CreateContinuousQueryFn func(q *influxql.CreateContinuousQueryStatement) (*meta.ContinuousQueryInfo, error)
	DropContinuousQueryFn   func(q *influxql.DropContinuousQueryStatement) error
>>>>>>> Allow coordinator to map points to multiple shards

	NodeFn       func(id uint64) (*meta.NodeInfo, error)
	NodeByHostFn func(host string) (*meta.NodeInfo, error)
	CreateNodeFn func(host string) (*meta.NodeInfo, error)
	DeleteNodeFn func(id uint64) error

	DatabaseFn                  func(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseFn            func(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseIfNotExistsFn func(name string) (*meta.DatabaseInfo, error)
	DropDatabaseFn              func(name string) error

	RetentionPolicyFn                  func(database, name string) (*meta.RetentionPolicyInfo, error)
	CreateRetentionPolicyFn            func(database string, rp *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error)
	CreateRetentionPolicyIfNotExistsFn func(database string, rp *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error)
	SetDefaultRetentionPolicyFn        func(database, name string) error
	UpdateRetentionPolicyFn            func(database, name string, rpu *meta.RetentionPolicyUpdate) (*meta.RetentionPolicyInfo, error)
	DeleteRetentionPolicyFn            func(database, name string) error

	ShardGroupFn                  func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	CreateShardGroupIfNotExistsFn func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	DeleteShardGroupFn            func(database, policy string, shardID uint64) error

	UserFn         func(username string) (*meta.UserInfo, error)
	CreateUserFn   func(username, password string, admin bool) (*meta.UserInfo, error)
	UpdateUserFn   func(username, password string) (*meta.UserInfo, error)
	DeleteUserFn   func(username string) error
	SetPrivilegeFn func(p influxql.Privilege, username string, dbname string) error
}

<<<<<<< HEAD
func (m MetaStore) Open(path string) error {
	return m.OpenFn(path)
}

func (m MetaStore) Close() error {
	return m.CloseFn()
}

func (m MetaStore) CreateContinuousQuery(query string) (*meta.ContinuousQueryInfo, error) {
	return m.CreateContinuousQueryFn(query)
}

func (m MetaStore) DropContinuousQuery(query string) error {
	return m.DropContinuousQueryFn(query)
=======
func (m MetaStore) CreateContinuousQuery(q *influxql.CreateContinuousQueryStatement) (*meta.ContinuousQueryInfo, error) {
	return m.CreateContinuousQueryFn(q)
}

func (m MetaStore) DropContinuousQuery(q *influxql.DropContinuousQueryStatement) error {
	return m.DropContinuousQueryFn(q)
>>>>>>> Allow coordinator to map points to multiple shards
}

func (m MetaStore) Node(id uint64) (*meta.NodeInfo, error) {
	return m.NodeFn(id)
}

func (m MetaStore) NodeByHost(host string) (*meta.NodeInfo, error) {
	return m.NodeByHostFn(host)
}

func (m MetaStore) CreateNode(host string) (*meta.NodeInfo, error) {
	return m.CreateNodeFn(host)
}

func (m MetaStore) DeleteNode(id uint64) error {
	return m.DeleteNodeFn(id)
}

func (m MetaStore) Database(name string) (*meta.DatabaseInfo, error) {
	return m.DatabaseFn(name)
}

func (m MetaStore) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	return m.CreateDatabaseFn(name)
}

func (m MetaStore) CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error) {
	return m.CreateDatabaseIfNotExistsFn(name)
}

func (m MetaStore) DropDatabase(name string) error {
	return m.DropDatabaseFn(name)
}

func (m MetaStore) RetentionPolicy(database, name string) (*meta.RetentionPolicyInfo, error) {
	return m.RetentionPolicyFn(database, name)
}

func (m MetaStore) CreateRetentionPolicy(database string, rp *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error) {
	return m.CreateRetentionPolicyFn(database, rp)
}

func (m MetaStore) CreateRetentionPolicyIfNotExists(database string, rp *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error) {
	return m.CreateRetentionPolicyIfNotExistsFn(database, rp)
}

func (m MetaStore) SetDefaultRetentionPolicy(database, name string) error {
	return m.SetDefaultRetentionPolicyFn(database, name)
}

func (m MetaStore) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate) (*meta.RetentionPolicyInfo, error) {
	return m.UpdateRetentionPolicyFn(database, name, rpu)
}

func (m MetaStore) DeleteRetentionPolicy(database, name string) error {
	return m.DeleteRetentionPolicyFn(database, name)
}

func (m MetaStore) ShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	return m.ShardGroupFn(database, policy, timestamp)
}

func (m MetaStore) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	return m.CreateShardGroupIfNotExistsFn(database, policy, timestamp)
}

func (m MetaStore) DeleteShardGroup(database, policy string, shardID uint64) error {
	return m.DeleteShardGroupFn(database, policy, shardID)
}

func (m MetaStore) User(username string) (*meta.UserInfo, error) {
	return m.UserFn(username)
}

func (m MetaStore) CreateUser(username, password string, admin bool) (*meta.UserInfo, error) {
	return m.CreateUserFn(username, password, admin)
}

func (m MetaStore) UpdateUser(username, password string) (*meta.UserInfo, error) {
	return m.UpdateUserFn(username, password)
}
func (m MetaStore) DeleteUser(username string) error {
	return m.DeleteUserFn(username)
}

func (m MetaStore) SetPrivilege(p influxql.Privilege, username string, dbname string) error {
	return m.SetPrivilegeFn(p, username, dbname)
}

func NewRetentionPolicy(name string, duration time.Duration, nodeCount int) *meta.RetentionPolicyInfo {

	shards := []meta.ShardInfo{}
	ownerIDs := []uint64{}
	for i := 1; i <= nodeCount; i++ {
		ownerIDs = append(ownerIDs, uint64(i))
	}

	// each node is fully replicated with each other
	shards = append(shards, meta.ShardInfo{
		ID:       nextShardID(),
		OwnerIDs: ownerIDs,
	})

	rp := &meta.RetentionPolicyInfo{
		Name:               "myrp",
		ReplicaN:           nodeCount,
		Duration:           duration,
		ShardGroupDuration: duration,
		ShardGroups: []meta.ShardGroupInfo{
			meta.ShardGroupInfo{
				ID:        nextShardID(),
				StartTime: time.Unix(0, 0),
				EndTime:   time.Unix(0, 0).Add(duration).Add(-1),
				Shards:    shards,
			},
		},
	}
	return rp
}

func AttachShardGroupInfo(rp *meta.RetentionPolicyInfo, ownerIDs []uint64) {
	var startTime, endTime time.Time
	if len(rp.ShardGroups) == 0 {
		startTime = time.Unix(0, 0)
	} else {
		startTime = rp.ShardGroups[len(rp.ShardGroups)-1].StartTime.Add(rp.ShardGroupDuration)
	}
	endTime = startTime.Add(rp.ShardGroupDuration).Add(-1)

	sh := meta.ShardGroupInfo{
		ID:        uint64(len(rp.ShardGroups) + 1),
		StartTime: startTime,
		EndTime:   endTime,
		Shards: []meta.ShardInfo{
			meta.ShardInfo{
				ID:       nextShardID(),
				OwnerIDs: ownerIDs,
			},
		},
	}
	rp.ShardGroups = append(rp.ShardGroups, sh)
}

func nextShardID() uint64 {
	return atomic.AddUint64(&shardID, 1)
}
