package precreator

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/v2/toml"
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

	if c.CheckInterval <= 0 {
		return errors.New("check-interval must be positive")
	}
	if c.AdvancePeriod <= 0 {
		return errors.New("advance-period must be positive")
	}

	return nil
}
