package registration

import (
	"time"

	"github.com/influxdata/config"
	"github.com/influxdata/enterprise-client/v2"
)

const (
	defaultStatsInterval = time.Minute
)

// Config represents the configuration for the registration service.
type Config struct {
	Enabled       bool            `toml:"enabled"`
	Token         string          `toml:"token"`
	StatsInterval config.Duration `toml:"stats-interval"`
	Hosts         []*client.Host  `toml:"hosts"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		Enabled:       false,
		StatsInterval: config.Duration(defaultStatsInterval),
	}
}
