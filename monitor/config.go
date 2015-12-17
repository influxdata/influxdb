package monitor

import (
	"time"

	"github.com/influxdata/config"
)

const (
	// DefaultStoreEnabled is whether the system writes gathered information in
	// an InfluxDB system for historical analysis.
	DefaultStoreEnabled = true

	// DefaultStoreDatabase is the name of the database where gathered information is written
	DefaultStoreDatabase = "_internal"

	// DefaultStoreInterval is the period between storing gathered information.
	DefaultStoreInterval = 10 * time.Second
)

// Config represents the configuration for the monitor service.
type Config struct {
	StoreEnabled  bool            `toml:"store-enabled"`
	StoreDatabase string          `toml:"store-database"`
	StoreInterval config.Duration `toml:"store-interval"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		StoreEnabled:  true,
		StoreDatabase: DefaultStoreDatabase,
		StoreInterval: config.Duration(DefaultStoreInterval),
	}
}
