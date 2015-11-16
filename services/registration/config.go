package registration

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	defaultURL           = "https://enterprise.influxdata.com"
	defaultStatsInterval = time.Minute
)

// Config represents the configuration for the registration service.
type Config struct {
	Enabled       bool          `toml:"enabled"`
	URL           string        `toml:"url"`
	Token         string        `toml:"token"`
	StatsInterval toml.Duration `toml:"stats-interval"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		Enabled:       true,
		URL:           defaultURL,
		StatsInterval: toml.Duration(defaultStatsInterval),
	}
}
