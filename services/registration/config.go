package registration

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	DefaultURL           = "https://enterprise.influxdata.com"
	DefaultStatsInterval = time.Minute
)

type Config struct {
	Enabled       bool          `toml:"enabled"`
	URL           string        `toml:"url"`
	Token         string        `toml:"token"`
	StatsInterval toml.Duration `toml:"stats-interval"`
}

func NewConfig() Config {
	return Config{
		Enabled:       true,
		URL:           DefaultURL,
		StatsInterval: toml.Duration(DefaultStatsInterval),
	}
}
