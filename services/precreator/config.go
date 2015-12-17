package precreator

import (
	"time"

	"github.com/influxdata/config"
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
	Enabled       bool            `toml:"enabled"`
	CheckInterval config.Duration `toml:"check-interval"`
	AdvancePeriod config.Duration `toml:"advance-period"`
}

// NewConfig returns a new Config with defaults.
func NewConfig() Config {
	return Config{
		Enabled:       true,
		CheckInterval: config.Duration(DefaultCheckInterval),
		AdvancePeriod: config.Duration(DefaultAdvancePeriod),
	}
}
