package monitor

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultStoreEnabled is whether the system writes gathered information in
	// an InfluxDB system for historical analysis.
	DefaultStoreEnabled = true

	// DefaultStoreDatabase is the name of the database where gathered information is written
	DefaultStoreDatabase = "_internal"

	// DefaultStoreInterval is the period between storing gathered information.
	DefaultStoreInterval = time.Minute

	// DefaultStoreAddress is the destination system for gathered information.
	DefaultStoreAddress = "127.0.0.1:8086"
)

// Config represents the configuration for the monitor service.
type Config struct {
	StoreEnabled  bool          `toml:"store-enabled"`
	StoreDatabase string        `toml:"store-database"`
	StoreInterval toml.Duration `toml:"store-interval"`
	StoreAddress  string        `toml:"store-address"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		StoreEnabled:  false,
		StoreDatabase: DefaultStoreDatabase,
		StoreInterval: toml.Duration(DefaultStoreInterval),
		StoreAddress:  DefaultStoreAddress,
	}
}
