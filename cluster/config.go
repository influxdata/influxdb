package cluster

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultBindAddress is the default bind address for the HTTP server.
	DefaultBindAddress = ":8087"

	// DefaultShardWriterTimeout is the default timeout set on shard writers.
	DefaultShardWriterTimeout = 5 * time.Second
)

// Config represents the configuration for the the clustering service.
type Config struct {
	BindAddress        string        `toml:"bind-address"`
	ShardWriterTimeout toml.Duration `toml:"shard-writer-timeout"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		BindAddress:        DefaultBindAddress,
		ShardWriterTimeout: toml.Duration(DefaultShardWriterTimeout),
	}
}
