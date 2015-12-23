package meta

import (
	"time"

	"github.com/influxdb/influxdb/influxql"
)

type Client struct {
}

func NewClient(c *Config) *Client {
	return &Client{}
}

func (c *Client) ClusterID() (id uint64, err error) {
	return 0, nil
}

// Node returns a node by id.
func (c *Client) Node(id uint64) (*NodeInfo, error) {
	return nil, nil
}

func (c *Client) Database(name string) (*DatabaseInfo, error) {
	return nil, nil
}

func (c *Client) Databases() ([]DatabaseInfo, error) {
	return nil, nil
}

func (c *Client) CreateDatabase(name string) (*DatabaseInfo, error) {
	return nil, nil
}

func (c *Client) CreateDatabaseIfNotExists(name string) (*DatabaseInfo, error) {
	return nil, nil
}

func (c *Client) CreateRetentionPolicyIfNotExists(database string, rpi *RetentionPolicyInfo) (*RetentionPolicyInfo, error) {
	return nil, nil
}

func (c *Client) DropRetentionPolicy(database, name string) error {
	return nil
}

func (c *Client) SetDefaultRetentionPolicy(database, name string) error {
	return nil
}

func (c *Client) IsLeader() bool {
	return false
}

func (c *Client) WaitForLeader(timeout time.Duration) error {
	return nil
}

func (c *Client) Users() (a []UserInfo, err error) {
	return nil, nil
}

func (c *Client) User(name string) (*UserInfo, error) {
	return nil, nil
}

func (c *Client) AdminUserExists() (bool, error) {
	return false, nil
}

func (c *Client) Authenticate(username, password string) (*UserInfo, error) {
	return nil, nil
}

func (c *Client) RetentionPolicy(database, name string) (rpi *RetentionPolicyInfo, err error) {
	return nil, nil
}

func (c *Client) VisitRetentionPolicies(f func(d DatabaseInfo, r RetentionPolicyInfo)) {

}

func (c *Client) UserCount() (int, error) {
	return 0, nil
}

func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []ShardGroupInfo, err error) {
	return nil, nil
}

func (c *Client) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	return nil, nil
}

func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	return nil
}

func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	return nil
}

func (c *Client) ShardOwner(shardID uint64) (string, string, *ShardGroupInfo) {
	return "", "", nil
}

func (c *Client) ExecuteStatement(stmt influxql.Statement) *influxql.Result {
	return nil
}

func (c *Client) WaitForDataChanged() error {
	return nil
}

func (c *Client) MarshalBinary() ([]byte, error) {
	return nil, nil
}
