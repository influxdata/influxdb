package influxdb

import (
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/messaging"
)

const (
	// Data node messages
	createDataNodeMessageType = messaging.MessageType(0x00)
	deleteDataNodeMessageType = messaging.MessageType(0x01)

	// Database messages
	createDatabaseMessageType = messaging.MessageType(0x10)
	deleteDatabaseMessageType = messaging.MessageType(0x11)

	// Retention policy messages
	createRetentionPolicyMessageType     = messaging.MessageType(0x20)
	updateRetentionPolicyMessageType     = messaging.MessageType(0x21)
	deleteRetentionPolicyMessageType     = messaging.MessageType(0x22)
	setDefaultRetentionPolicyMessageType = messaging.MessageType(0x23)

	// User messages
	createUserMessageType = messaging.MessageType(0x30)
	updateUserMessageType = messaging.MessageType(0x31)
	deleteUserMessageType = messaging.MessageType(0x32)

	// Shard messages
	createShardGroupIfNotExistsMessageType = messaging.MessageType(0x40)
	deleteShardGroupMessageType            = messaging.MessageType(0x41)

	// Series messages
	createSeriesIfNotExistsMessageType = messaging.MessageType(0x50)

	// Measurement messages
	createFieldsIfNotExistsMessageType = messaging.MessageType(0x60)

	// Continuous Query messages
	createContinuousQueryMessageType = messaging.MessageType(0x70)

	// Write series data messages (per-topic)
	writeRawSeriesMessageType = messaging.MessageType(0x80)

	// Privilege messages
	setPrivilegeMessageType = messaging.MessageType(0x90)
)

type createDataNodeCommand struct {
	URL string `json:"url"`
}

type deleteDataNodeCommand struct {
	ID uint64 `json:"id"`
}

type createDatabaseCommand struct {
	Name string `json:"name"`
}

type deleteDatabaseCommand struct {
	Name string `json:"name"`
}

type createShardGroupIfNotExistsCommand struct {
	Database  string    `json:"database"`
	Policy    string    `json:"policy"`
	Timestamp time.Time `json:"timestamp"`
}

type deleteShardGroupCommand struct {
	Database string `json:"database"`
	Policy   string `json:"policy"`
	ID       uint64 `json:"id"`
}

type createUserCommand struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Admin    bool   `json:"admin,omitempty"`
}

type updateUserCommand struct {
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

type deleteUserCommand struct {
	Username string `json:"username"`
}

type setPrivilegeCommand struct {
	Privilege influxql.Privilege `json:"privilege"`
	Username  string             `json:"username"`
	Database  string             `json:"database"`
}

type createRetentionPolicyCommand struct {
	Database string        `json:"database"`
	Name     string        `json:"name"`
	Duration time.Duration `json:"duration"`
	ReplicaN uint32        `json:"replicaN"`
	SplitN   uint32        `json:"splitN"`
}

type updateRetentionPolicyCommand struct {
	Database string                 `json:"database"`
	Name     string                 `json:"name"`
	Policy   *RetentionPolicyUpdate `json:"policy"`
}

type deleteRetentionPolicyCommand struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

type setDefaultRetentionPolicyCommand struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

type createFieldsIfNotExistCommand struct {
	Database    string                       `json:"database"`
	Measurement string                       `json:"measurement"`
	Fields      map[string]influxql.DataType `json:"fields"`
}

type createSeriesIfNotExistsCommand struct {
	Database string            `json:"database"`
	Name     string            `json:"name"`
	Tags     map[string]string `json:"tags"`
}

// createContinuousQueryCommand is the raft command for creating a continuous query on a database
type createContinuousQueryCommand struct {
	Query string `json:"query"`
}
