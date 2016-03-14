package cluster

import (
	"time"

	"github.com/influxdata/config"
)

const (
	// DefaultWriteTimeout is the default timeout for a complete write to succeed.
	DefaultWriteTimeout = 5 * time.Second

	// DefaultShardWriterTimeout is the default timeout set on shard writers.
	DefaultShardWriterTimeout = 5 * time.Second

	// DefaultShardMapperTimeout is the default timeout set on shard mappers.
	DefaultShardMapperTimeout = 5 * time.Second

	// DefaultMaxRemoteWriteConnections is the maximum number of open connections
	// that will be available for remote writes to another host.
	DefaultMaxRemoteWriteConnections = 3
)

// Config represents the configuration for the clustering service.
type Config struct {
	ForceRemoteShardMapping   bool            `toml:"force-remote-mapping"`
	WriteTimeout              config.Duration `toml:"write-timeout"`
	ShardWriterTimeout        config.Duration `toml:"shard-writer-timeout"`
	MaxRemoteWriteConnections int             `toml:"max-remote-write-connections"`
	ShardMapperTimeout        config.Duration `toml:"shard-mapper-timeout"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		WriteTimeout:              config.Duration(DefaultWriteTimeout),
		ShardWriterTimeout:        config.Duration(DefaultShardWriterTimeout),
		ShardMapperTimeout:        config.Duration(DefaultShardMapperTimeout),
		MaxRemoteWriteConnections: DefaultMaxRemoteWriteConnections,
	}
}
