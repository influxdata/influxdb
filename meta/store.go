package meta

import (
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// Store represents an interface access and mutate metadata.
type Store interface {
	CreateContinuousQuery(q *influxql.CreateContinuousQueryStatement) (*ContinuousQueryInfo, error)
	DropContinuousQuery(q *influxql.DropContinuousQueryStatement) error

	CreateNode(host string) (*NodeInfo, error)
	DeleteNode(id uint64) error

	CreateDatabase(name string) (*NodeInfo, error)
	CreateDatabaseIfNotExists(name string) (*DatabaseInfo, error)
	DropDatabase(name string) error

	CreateRetentionPolicy(database string, rp *RetentionPolicyInfo) (*RetentionPolicyInfo, error)
	CreateRetentionPolicyIfNotExists(database string, rp *RetentionPolicyInfo) (*RetentionPolicyInfo, error)
	SetDefaultRetentionPolicy(database, name string) error
	UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) (*RetentionPolicyInfo, error)
	DeleteRetentionPolicy(database, name string) error

	CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*ShardGroupInfo, error)
	DeleteShardGroup(database, policy string, shardID uint64) error

	CreateUser(username, password string, admin bool) (*UserInfo, error)
	UpdateUser(username, password string) (*UserInfo, error)
	DeleteUser(username string) error
	SetPrivilege(p influxql.Privilege, username string, dbname string) error
}

// RetentionPolicyUpdate represents retention policy fields to be updated.
type RetentionPolicyUpdate struct {
	Name     *string
	Duration *time.Duration
	ReplicaN *uint32
}
