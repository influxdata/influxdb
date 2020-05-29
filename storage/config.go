package storage

import (
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/v2/toml"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"github.com/influxdata/influxdb/v2/tsdb/tsi1"
	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
)

// Default configuration values.
const (
	DefaultRetentionInterval       = time.Hour
	DefaultSeriesFileDirectoryName = "_series"
	DefaultIndexDirectoryName      = "index"
	DefaultWALDirectoryName        = "wal"
	DefaultEngineDirectoryName     = "data"
)

// Config holds the configuration for an Engine.
type Config struct {
	// Frequency of retention in seconds.
	RetentionInterval toml.Duration `toml:"retention-interval"`

	// Series file config.
	SeriesFilePath string `toml:"series-file-path"` // Overrides the default path.

	// Series file config.
	SeriesFile seriesfile.Config `toml:"tsdb"`

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
		RetentionInterval: toml.Duration(DefaultRetentionInterval),
		SeriesFile:        seriesfile.NewConfig(),
		WAL:               tsm1.NewWALConfig(),
		Engine:            tsm1.NewConfig(),
		Index:             tsi1.NewConfig(),
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
