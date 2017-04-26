package precreator

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultCheckInterval is the shard precreation check time if none is specified.
	DefaultCheckInterval = 10 * time.Minute

	// DefaultAdvancePeriod is the default period ahead of the endtime of a shard group
	// that its successor group is created.
	DefaultAdvancePeriod = 30 * time.Minute
)

// Config represents the configuration for shard precreation.
type Config struct {
	Enabled       bool          `toml:"enabled"`
	CheckInterval toml.Duration `toml:"check-interval"`
	AdvancePeriod toml.Duration `toml:"advance-period"`
}

// NewConfig returns a new Config with defaults.
func NewConfig() Config {
	return Config{
		Enabled:       true,
		CheckInterval: toml.Duration(DefaultCheckInterval),
		AdvancePeriod: toml.Duration(DefaultAdvancePeriod),
	}
}

// Validate returns an error if the Config is invalid.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	// TODO: Should we enforce a minimum interval?
	// Polling every nanosecond, for instance, will greatly impact performance.
	if c.CheckInterval <= 0 {
		return errors.New("check-interval must be positive")
	}
	if c.AdvancePeriod <= 0 {
		return errors.New("advance-period must be positive")
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
		"enabled":        true,
		"check-interval": c.CheckInterval,
		"advance-period": c.AdvancePeriod,
	}), nil
}
