package meta

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
)

const (
	// DefaultLeaseDuration is the default duration for leases.
	DefaultLeaseDuration = 60 * time.Second

	// DefaultLoggingEnabled determines if log messages are printed for the meta service.
	DefaultLoggingEnabled = true
)

// Config represents the meta configuration.
type Config struct {
	Dir string `toml:"dir"`

	RetentionAutoCreate bool `toml:"retention-autocreate"`
	LoggingEnabled      bool `toml:"logging-enabled"`
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		RetentionAutoCreate: true,
		LoggingEnabled:      DefaultLoggingEnabled,
	}
}

// Validate returns an error if the config is invalid.
func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	}
	return nil
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c *Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	return diagnostics.RowFromMap(map[string]interface{}{
		"dir": c.Dir,
	}), nil
}
