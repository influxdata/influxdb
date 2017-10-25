package storage

import (
	"github.com/influxdata/influxdb/monitor/diagnostics"
)

const (
	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress = ":8082"
)

// Config represents a configuration for a HTTP service.
type Config struct {
	Enabled     bool   `toml:"enabled"`
	LogEnabled  bool   `toml:"log-enabled"` // verbose logging
	BindAddress string `toml:"bind-address"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:     false,
		LogEnabled:  true,
		BindAddress: DefaultBindAddress,
	}
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
		"log-enabled":  c.LogEnabled,
		"bind-address": c.BindAddress,
	}), nil
}
