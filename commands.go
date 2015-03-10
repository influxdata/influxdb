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
	dropDatabaseMessageType   = messaging.MessageType(0x11)

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
	dropSeriesMessageType = messaging.MessageType(0x50)

	// Measurement messages
	createMeasurementsIfNotExistsMessageType = messaging.MessageType(0x60)
	dropMeasurementMessageType               = messaging.MessageType(0x61)

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

type dropDatabaseCommand struct {
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
	Database           string        `json:"database"`
	Name               string        `json:"name"`
	Duration           time.Duration `json:"duration"`
	ShardGroupDuration time.Duration `json:"shardGroupDuration"`
	ReplicaN           uint32        `json:"replicaN"`
	SplitN             uint32        `json:"splitN"`
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

type dropMeasurementCommand struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

type createMeasurementSubcommand struct {
	Name   string              `json:"name"`
	Tags   []map[string]string `json:"tags"`
	Fields []*Field            `json:"fields"`
}

type createMeasurementsIfNotExistsCommand struct {
	Database     string                         `json:"database"`
	Measurements []*createMeasurementSubcommand `json:"measurements"`
}

func newCreateMeasurementsIfNotExistsCommand(database string) *createMeasurementsIfNotExistsCommand {
	return &createMeasurementsIfNotExistsCommand{
		Database:     database,
		Measurements: make([]*createMeasurementSubcommand, 0),
	}
}

// addMeasurementIfNotExists adds the Measurement to the command, but only if not already present
// in the command.
func (c *createMeasurementsIfNotExistsCommand) addMeasurementIfNotExists(name string) *createMeasurementSubcommand {
	for _, m := range c.Measurements {
		if m.Name == name {
			return m
		}
	}
	m := &createMeasurementSubcommand{
		Name:   name,
		Tags:   make([]map[string]string, 0),
		Fields: make([]*Field, 0),
	}
	c.Measurements = append(c.Measurements, m)
	return m
}

// addSeriesIfNotExists adds the Series, identified by Measurement name and tag set, to
// the command, but only if not already present in the command.
func (c *createMeasurementsIfNotExistsCommand) addSeriesIfNotExists(measurement string, tags map[string]string) {
	m := c.addMeasurementIfNotExists(measurement)

	tagset := string(marshalTags(tags))
	for _, t := range m.Tags {
		if string(marshalTags(t)) == tagset {
			// Series already present in subcommand, nothing to do.
			return
		}
	}
	// Tag-set needs to added to subcommand.
	m.Tags = append(m.Tags, tags)

	return
}

// addFieldIfNotExists adds the field to the command for the Measurement, but only if it is not already
// present. It will return an error if the field is present in the command, but is of a different type.
func (c *createMeasurementsIfNotExistsCommand) addFieldIfNotExists(measurement, name string, typ influxql.DataType) error {
	m := c.addMeasurementIfNotExists(measurement)

	for _, f := range m.Fields {
		if f.Name == name {
			if f.Type != typ {
				return ErrFieldTypeConflict
			}
			// Field already present in subcommand with same type, nothing to do.
			return nil
		}
	}

	// New field for this measurement so add it to the subcommand.
	newField := &Field{Name: name, Type: typ}
	m.Fields = append(m.Fields, newField)
	return nil
}

type dropSeriesCommand struct {
	Database            string              `json:"datbase"`
	SeriesByMeasurement map[string][]uint32 `json:"seriesIds"`
}

// createContinuousQueryCommand is the raft command for creating a continuous query on a database
type createContinuousQueryCommand struct {
	Query string `json:"query"`
}
