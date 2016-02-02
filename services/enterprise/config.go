package enterprise

import (
	"time"

	"github.com/influxdata/config"
	"github.com/influxdata/enterprise-client/v2"
)

const (
	defaultStatsInterval = time.Minute
)

// Config represents the configuration for the enterprise service.
type Config struct {
	Enabled       bool            `toml:"enabled"`
	Token         string          `toml:"token"`
	StatsInterval config.Duration `toml:"stats-interval"`
	AdminPort     uint16          `toml:"admin-port"`
	Hosts         []*client.Host  `toml:"hosts"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		Enabled:       false,
		StatsInterval: config.Duration(defaultStatsInterval),
		AdminPort:     8090,
	}
}
