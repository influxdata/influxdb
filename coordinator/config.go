// Package coordinator contains abstractions for writing points, executing statements,
// and accessing meta data.
package coordinator

import (
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultWriteTimeout is the default timeout for a complete write to succeed.
	DefaultWriteTimeout = 10 * time.Second

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

// Config represents the configuration for the coordinator service.
type Config struct {
	WriteTimeout         toml.Duration `toml:"write-timeout"`
	MaxConcurrentQueries int           `toml:"max-concurrent-queries"`
	QueryTimeout         toml.Duration `toml:"query-timeout"`
	LogQueriesAfter      toml.Duration `toml:"log-queries-after"`
	MaxSelectPointN      int           `toml:"max-select-point"`
	MaxSelectSeriesN     int           `toml:"max-select-series"`
	MaxSelectBucketsN    int           `toml:"max-select-buckets"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		WriteTimeout:         toml.Duration(DefaultWriteTimeout),
		QueryTimeout:         toml.Duration(query.DefaultQueryTimeout),
		MaxConcurrentQueries: DefaultMaxConcurrentQueries,
		MaxSelectPointN:      DefaultMaxSelectPointN,
		MaxSelectSeriesN:     DefaultMaxSelectSeriesN,
	}
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	return diagnostics.RowFromMap(map[string]interface{}{
		"write-timeout":          c.WriteTimeout,
		"max-concurrent-queries": c.MaxConcurrentQueries,
		"query-timeout":          c.QueryTimeout,
		"log-queries-after":      c.LogQueriesAfter,
		"max-select-point":       c.MaxSelectPointN,
		"max-select-series":      c.MaxSelectSeriesN,
		"max-select-buckets":     c.MaxSelectBucketsN,
	}), nil
}
