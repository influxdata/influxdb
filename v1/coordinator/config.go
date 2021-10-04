// Package coordinator contains abstractions for writing points, executing statements,
// and accessing meta data.
package coordinator

import (
	"github.com/influxdata/influxdb/v2/toml"
)

const (
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
	MaxConcurrentQueries int           `toml:"max-concurrent-queries"`
	LogQueriesAfter      toml.Duration `toml:"log-queries-after"`
	MaxSelectPointN      int           `toml:"max-select-point"`
	MaxSelectSeriesN     int           `toml:"max-select-series"`
	MaxSelectBucketsN    int           `toml:"max-select-buckets"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		MaxConcurrentQueries: DefaultMaxConcurrentQueries,
		MaxSelectPointN:      DefaultMaxSelectPointN,
		MaxSelectSeriesN:     DefaultMaxSelectSeriesN,
	}
}
