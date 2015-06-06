package cluster

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultShardWriterTimeout is the default timeout set on shard writers.
	DefaultShardWriterTimeout = 5 * time.Second
)

// Config represents the configuration for the the clustering service.
type Config struct {
	ShardWriterTimeout toml.Duration `toml:"shard-writer-timeout"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		ShardWriterTimeout: toml.Duration(DefaultShardWriterTimeout),
	}
}
