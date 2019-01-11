package storage

import (
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/influxdata/influxdb/tsdb/tsm1"
)

const (
	DefaultRetentionInterval   = 1 * time.Hour
	DefaultValidateKeys        = false
	DefaultTraceLoggingEnabled = false

	DefaultSeriesFileDirectoryName = "_series"
	DefaultIndexDirectoryName      = "index"
	DefaultWALDirectoryName        = "wal"
	DefaultEngineDirectoryName     = "data"
)

// Config holds the configuration for an Engine.
type Config struct {
	// Frequency of retention in seconds.
	RetentionInterval toml.Duration `toml:"retention-interval"`

	// Enables unicode validation on series keys on write.
	ValidateKeys bool `toml:"validate-keys"`

	// Enables trace logging for the engine.
	TraceLoggingEnabled bool `toml:"trace-logging-enabled"`

	// Series file config.
	SeriesFilePath string `toml:"series-file-path"` // Overrides the default path.

	// WAL config.
	WAL     tsm1.WALConfig `toml:"wal"`
	WALPath string         `toml:"wal-path"` // Overrides the default path.

	// Engine config.
	Engine     tsm1.Config `toml:"engine"`
	EnginePath string      `toml:"engine-path"` // Overrides the default path.

	// Index config.
	Index     tsi1.Config `toml:"index"`
	IndexPath string      `toml:"index-path"` // Overrides the default path.
}

// NewConfig initialises a new config for an Engine.
func NewConfig() Config {
	return Config{
		RetentionInterval:   toml.Duration(DefaultRetentionInterval),
		ValidateKeys:        DefaultValidateKeys,
		TraceLoggingEnabled: DefaultTraceLoggingEnabled,

		WAL:    tsm1.NewWALConfig(),
		Engine: tsm1.NewConfig(),
		Index:  tsi1.NewConfig(),
	}
}

// GetSeriesFilePath returns the path to the series file.
func (c Config) GetSeriesFilePath(base string) string {
	if c.SeriesFilePath != "" {
		return c.SeriesFilePath
	}
	return filepath.Join(base, DefaultSeriesFileDirectoryName)
}

// GetIndexPath returns the path to the index.
func (c Config) GetIndexPath(base string) string {
	if c.IndexPath != "" {
		return c.IndexPath
	}
	return filepath.Join(base, DefaultIndexDirectoryName)
}

// GetWALPath returns the path to the WAL.
func (c Config) GetWALPath(base string) string {
	if c.WALPath != "" {
		return c.WALPath
	}
	return filepath.Join(base, DefaultWALDirectoryName)
}

// GetEnginePath returns the path to the engine.
func (c Config) GetEnginePath(base string) string {
	if c.EnginePath != "" {
		return c.EnginePath
	}
	return filepath.Join(base, DefaultEngineDirectoryName)
}
