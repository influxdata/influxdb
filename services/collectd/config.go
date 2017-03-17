package collectd

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultBindAddress is the default port to bind to.
	DefaultBindAddress = ":25826"

	// DefaultDatabase is the default DB to write to.
	DefaultDatabase = "collectd"

	// DefaultRetentionPolicy is the default retention policy of the writes.
	DefaultRetentionPolicy = ""

	// DefaultBatchSize is the default write batch size.
	DefaultBatchSize = 5000

	// DefaultBatchPending is the default number of pending write batches.
	DefaultBatchPending = 10

	// DefaultBatchDuration is the default batch timeout duration.
	DefaultBatchDuration = toml.Duration(10 * time.Second)

	// DefaultTypesDB is the default location of the collectd types db file.
	DefaultTypesDB = "/usr/share/collectd/types.db"

	// DefaultReadBuffer is the default buffer size for the UDP listener.
	// Sets the size of the operating system's receive buffer associated with
	// the UDP traffic. Keep in mind that the OS must be able
	// to handle the number set here or the UDP listener will error and exit.
	//
	// DefaultReadBuffer = 0 means to use the OS default, which is usually too
	// small for high UDP performance.
	//
	// Increasing OS buffer limits:
	//     Linux:      sudo sysctl -w net.core.rmem_max=<read-buffer>
	//     BSD/Darwin: sudo sysctl -w kern.ipc.maxsockbuf=<read-buffer>
	DefaultReadBuffer = 0

	// DefaultSecurityLevel is the default security level.
	DefaultSecurityLevel = "none"

	// DefaultAuthFile is the default location of the user/password file.
	DefaultAuthFile = "/etc/collectd/auth_file"
)

// Config represents a configuration for the collectd service.
type Config struct {
	Enabled         bool          `toml:"enabled"`
	BindAddress     string        `toml:"bind-address"`
	Database        string        `toml:"database"`
	RetentionPolicy string        `toml:"retention-policy"`
	BatchSize       int           `toml:"batch-size"`
	BatchPending    int           `toml:"batch-pending"`
	BatchDuration   toml.Duration `toml:"batch-timeout"`
	ReadBuffer      int           `toml:"read-buffer"`
	TypesDB         string        `toml:"typesdb"`
	SecurityLevel   string        `toml:"security-level"`
	AuthFile        string        `toml:"auth-file"`
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		BindAddress:     DefaultBindAddress,
		Database:        DefaultDatabase,
		RetentionPolicy: DefaultRetentionPolicy,
		ReadBuffer:      DefaultReadBuffer,
		BatchSize:       DefaultBatchSize,
		BatchPending:    DefaultBatchPending,
		BatchDuration:   DefaultBatchDuration,
		TypesDB:         DefaultTypesDB,
		SecurityLevel:   DefaultSecurityLevel,
		AuthFile:        DefaultAuthFile,
	}
}

// WithDefaults takes the given config and returns a new config with any required
// default values set.
func (c *Config) WithDefaults() *Config {
	d := *c
	if d.BindAddress == "" {
		d.BindAddress = DefaultBindAddress
	}
	if d.Database == "" {
		d.Database = DefaultDatabase
	}
	if d.RetentionPolicy == "" {
		d.RetentionPolicy = DefaultRetentionPolicy
	}
	if d.BatchSize == 0 {
		d.BatchSize = DefaultBatchSize
	}
	if d.BatchPending == 0 {
		d.BatchPending = DefaultBatchPending
	}
	if d.BatchDuration == 0 {
		d.BatchDuration = DefaultBatchDuration
	}
	if d.ReadBuffer == 0 {
		d.ReadBuffer = DefaultReadBuffer
	}
	if d.TypesDB == "" {
		d.TypesDB = DefaultTypesDB
	}
	if d.SecurityLevel == "" {
		d.SecurityLevel = DefaultSecurityLevel
	}
	if d.AuthFile == "" {
		d.AuthFile = DefaultAuthFile
	}

	return &d
}

// Validate returns an error if the Config is invalid.
func (c *Config) Validate() error {
	switch c.SecurityLevel {
	case "none", "sign", "encrypt":
	default:
		return errors.New("Invalid security level")
	}

	return nil
}

// Configs wraps a slice of Config to aggregate diagnostics.
type Configs []Config

// Diagnostics returns one set of diagnostics for all of the Configs.
func (c Configs) Diagnostics() (*diagnostics.Diagnostics, error) {
	d := &diagnostics.Diagnostics{
		Columns: []string{"enabled", "bind-address", "database", "retention-policy", "batch-size", "batch-pending", "batch-timeout"},
	}

	for _, cc := range c {
		if !cc.Enabled {
			d.AddRow([]interface{}{false})
			continue
		}

		r := []interface{}{true, cc.BindAddress, cc.Database, cc.RetentionPolicy, cc.BatchSize, cc.BatchPending, cc.BatchDuration}
		d.AddRow(r)
	}

	return d, nil
}

// Enabled returns true if any underlying Config is Enabled.
func (c Configs) Enabled() bool {
	for _, cc := range c {
		if cc.Enabled {
			return true
		}
	}
	return false
}
