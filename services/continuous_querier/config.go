package continuous_querier

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

// Default values for aspects of interval computation.
const (
	// The default value of how often to check whether any CQs need to be run.
	DefaultRunInterval = time.Second
)

// Config represents a configuration for the continuous query service.
type Config struct {
	// Enables logging in CQ service to display when CQ's are processed and how many points are wrote.
	LogEnabled bool `toml:"log-enabled"`

	// If this flag is set to false, both the brokers and data nodes should ignore any CQ processing.
	Enabled bool `toml:"enabled"`

	// Run interval for checking continuous queries. This should be set to the least common factor
	// of the interval for running continuous queries. If you only aggregate continuous queries
	// every minute, this should be set to 1 minute. The default is set to '1s' so the interval
	// is compatible with most aggregations.
	RunInterval toml.Duration `toml:"run-interval"`
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		LogEnabled:  true,
		Enabled:     true,
		RunInterval: toml.Duration(DefaultRunInterval),
	}
}

// Validate returns an error if the Config is invalid.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	// TODO: Should we enforce a minimum interval?
	// Polling every nanosecond, for instance, will greatly impact performance.
	if c.RunInterval <= 0 {
		return errors.New("run-interval must be positive")
	}

	return nil
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	if !c.Enabled {
		return diagnostics.RowFromMap(map[string]interface{}{
			"enabled": false,
		}), nil
	}

	return diagnostics.RowFromMap(map[string]interface{}{
		"enabled":      true,
		"run-interval": c.RunInterval,
	}), nil
}
