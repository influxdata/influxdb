package retention

import (
	"time"

	"github.com/influxdata/config"
)

// Config represents the configuration for the retention service.
type Config struct {
	Enabled       bool            `toml:"enabled"`
	CheckInterval config.Duration `toml:"check-interval"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{Enabled: true, CheckInterval: config.Duration(30 * time.Minute)}
}
