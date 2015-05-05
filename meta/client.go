package meta

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta/internal"
)

type Client struct{}

// CreateContinuousQuery creates a new continuous query.
func (c *Client) CreateContinuousQuery(q *influxql.CreateContinuousQueryStatement) error

// DropContinuousQuery removes a continuous query.
func (c *Client) DropContinuousQuery(q *influxql.DropContinuousQueryStatement) error

// CreateNode adds a new node to the cluster.
func (c *Client) CreateNode(host string) error {
	return c.exec(
		internal.Command_CreateNodeCommand,
		internal.E_CreateNodeCommand_Command,
		&internal.CreateNodeCommand{
			Host: proto.String(host),
		})
}

// DeleteNode removes a node from the cluster.
func (c *Client) DeleteNode(id uint64) error

// CreateDatabase creates a new database.
func (c *Client) CreateDatabase(name string) error

// DropDatabase removes a database.
func (c *Client) DropDatabase(name string) error

// CreateDatabaseIfNotExists creates a new database if it doesn't exist.
func (c *Client) CreateDatabaseIfNotExists(name string) error

// CreateRetentionPolicy creates a retention policy on a database.
func (c *Client) CreateRetentionPolicy(database string, rp *RetentionPolicyInfo) error

// CreateRetentionPolicyIfNotExists creates a retention policy on a database.
// Ignored if it already exists.
func (c *Client) CreateRetentionPolicyIfNotExists(database string, rp *RetentionPolicyInfo) error

// DeleteRetentionPolicy removes a retention policy from a database.
func (c *Client) DeleteRetentionPolicy(database, name string) error

// SetDefaultRetentionPolicy sets the name of the default retention policy on a database.
func (c *Client) SetDefaultRetentionPolicy(database, name string) error

// UpdateRetentionPolicy updates an existing retention policy.
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) error

// CreateShardGroupIfNotExists creates a shard for a retention policy for a given time.
func (c *Client) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) error

// CreateUser creates a new user.
func (c *Client) CreateUser(username, password string, admin bool) error

// DeleteUser removes a user from the system.
func (c *Client) DeleteUser(username string) error

// UpdateUser updates an existing user.
func (c *Client) UpdateUser(username, password string) error

// SetPrivilege sets a privilege on a user.
func (c *Client) SetPrivilege(p influxql.Privilege, username string, dbname string) error

// DeleteShardGroup removes a shard group.
func (c *Client) DeleteShardGroup(database, policy string, shardID uint64) error

func (c *Client) exec(typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	// Encode command.
	cmd := internal.Command{Type: &typ}
	if err := proto.SetExtension(&cmd, desc, value); err != nil {
		return err
	}

	// TODO: Send to service.

	return nil
}

// RetentionPolicyUpdate represents retention policy fields to be updated.
type RetentionPolicyUpdate struct {
	Name     *string
	Duration *time.Duration
	ReplicaN *uint32
}
