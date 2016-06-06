package meta

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultLeaseDuration is the default duration for leases.
	DefaultLeaseDuration = 60 * time.Second

	// DefaultLoggingEnabled determines if log messages are printed for the meta service
	DefaultLoggingEnabled = true

	// DefaultRetentionPolicyName is the default retention policy name.
	DefaultRetentionPolicyName = "autogen"
)

// Config represents the meta configuration.
type Config struct {
	Dir string `toml:"dir"`

	RetentionAutoCreate        bool   `toml:"retention-autocreate"`
	DefaultRetentionPolicyName string `toml:"default-retention-policy-name"`
	LoggingEnabled             bool   `toml:"logging-enabled"`
	PprofEnabled               bool   `toml:"pprof-enabled"`

	LeaseDuration toml.Duration `toml:"lease-duration"`
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		RetentionAutoCreate:        true,
		DefaultRetentionPolicyName: DefaultRetentionPolicyName,
		LeaseDuration:              toml.Duration(DefaultLeaseDuration),
		LoggingEnabled:             DefaultLoggingEnabled,
	}
}

func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	}
	return nil
}
