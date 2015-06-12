package collectd

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	DefaultBindAddress = ":25826"

	DefaultDatabase = "collectd"

	DefaultRetentionPolicy = ""

	DefaultBatchSize = 5000

	DefaultBatchDuration = toml.Duration(10 * time.Second)

	DefaultTypesDB = "/usr/share/collectd/types.db"
)

// Config represents a configuration for the collectd service.
type Config struct {
	Enabled         bool          `toml:"enabled"`
	BindAddress     string        `toml:"bind-address"`
	Database        string        `toml:"database"`
	RetentionPolicy string        `toml:"retention-policy"`
	BatchSize       int           `toml:"batch-size"`
	BatchDuration   toml.Duration `toml:"batch-timeout"`
	TypesDB         string        `toml:"typesdb"`
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		BindAddress:     DefaultBindAddress,
		Database:        DefaultDatabase,
		RetentionPolicy: DefaultRetentionPolicy,
		BatchSize:       DefaultBatchSize,
		BatchDuration:   DefaultBatchDuration,
		TypesDB:         DefaultTypesDB,
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
	if d.BatchDuration == 0 {
		d.BatchDuration = DefaultBatchDuration
	}
	if d.TypesDB == "" {
		d.TypesDB = DefaultTypesDB
	}
	return &d
}
