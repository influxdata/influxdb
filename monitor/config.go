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

	// DefaultStoreRetentionPolicy is the name of the retention policy for monitor data.
	DefaultStoreRetentionPolicy = "monitor"

	// DefaultRetentionPolicyDuration is the duration the data is retained.
	DefaultStoreRetentionPolicyDuration = 168 * time.Hour

	// DefaultStoreReplicationFactor is the default replication factor for the data.
	DefaultStoreReplicationFactor = 1

	// DefaultStoreInterval is the period between storing gathered information.
	DefaultStoreInterval = 10 * time.Second
)

// Config represents the configuration for the monitor service.
type Config struct {
	StoreEnabled           bool          `toml:"store-enabled"`
	StoreDatabase          string        `toml:"store-database"`
	StoreRetentionPolicy   string        `toml:"store-retention-policy"`
	StoreRetentionDuration toml.Duration `toml:"store-retention-duration"`
	StoreReplicationFactor int           `toml:"store-replication-factor"`
	StoreInterval          toml.Duration `toml:"store-interval"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		StoreEnabled:           true,
		StoreDatabase:          DefaultStoreDatabase,
		StoreRetentionPolicy:   DefaultStoreRetentionPolicy,
		StoreRetentionDuration: toml.Duration(DefaultStoreRetentionPolicyDuration),
		StoreReplicationFactor: DefaultStoreReplicationFactor,
		StoreInterval:          toml.Duration(DefaultStoreInterval),
	}
}
