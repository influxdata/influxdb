package cluster

import (
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultWriteTimeout is the default timeout for a complete write to succeed.
	DefaultWriteTimeout = 5 * time.Second

	// DefaultShardWriterTimeout is the default timeout set on shard writers.
	DefaultShardWriterTimeout = 5 * time.Second

	// DefaultShardMapperTimeout is the default timeout set on shard mappers.
	DefaultShardMapperTimeout = 5 * time.Second

	// DefaultQueryTimeout is the default timeout for executing a query.
	// A value of zero will have no query timeout.
	DefaultQueryTimeout = time.Duration(0)

	// DefaultMaxRemoteWriteConnections is the maximum number of open connections
	// that will be available for remote writes to another host.
	DefaultMaxRemoteWriteConnections = 3

	// DefaultMaxConcurrentQueries is the maximum number of running queries.
	// A value of zero will make the maximum query limit unlimited.
	DefaultMaxConcurrentQueries = 0

	// DefaultMaxSelectPointN is the maximum number of points a SELECT can process.
	// A value of zero will make the maximum point count unlimited.
	DefaultMaxSelectPointN = 0

	// DefaultMaxSelectSeriesN is the maximum number of series a SELECT can run.
	// A value of zero will make the maximum series count unlimited.
	DefaultMaxSelectSeriesN = 0
)

// Config represents the configuration for the clustering service.
type Config struct {
	ForceRemoteShardMapping   bool          `toml:"force-remote-mapping"`
	WriteTimeout              toml.Duration `toml:"write-timeout"`
	ShardWriterTimeout        toml.Duration `toml:"shard-writer-timeout"`
	MaxRemoteWriteConnections int           `toml:"max-remote-write-connections"`
	ShardMapperTimeout        toml.Duration `toml:"shard-mapper-timeout"`
	MaxConcurrentQueries      int           `toml:"max-concurrent-queries"`
	QueryTimeout              toml.Duration `toml:"query-timeout"`
	MaxSelectPointN           int           `toml:"max-select-point"`
	MaxSelectSeriesN          int           `toml:"max-select-series"`
	MaxSelectBucketsN         int           `toml:"max-select-buckets"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		WriteTimeout:              toml.Duration(DefaultWriteTimeout),
		ShardWriterTimeout:        toml.Duration(DefaultShardWriterTimeout),
		ShardMapperTimeout:        toml.Duration(DefaultShardMapperTimeout),
		QueryTimeout:              toml.Duration(DefaultQueryTimeout),
		MaxRemoteWriteConnections: DefaultMaxRemoteWriteConnections,
		MaxConcurrentQueries:      DefaultMaxConcurrentQueries,
		MaxSelectPointN:           DefaultMaxSelectPointN,
		MaxSelectSeriesN:          DefaultMaxSelectSeriesN,
	}
}
