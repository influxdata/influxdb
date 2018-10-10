package storage

import (
	"github.com/influxdata/platform/tsdb"
	"github.com/influxdata/platform/tsdb/tsi1"
)

// Config defaults
const (
	DefaultRetentionInterval = 3600 // 1 hour.
)

// Config holds the configuration for an Engine.
type Config struct {
	RetentionInterval int64 `toml:"retention_interval"` // Frequency of retention in seconds.

	EngineOptions tsdb.EngineOptions `toml:"-"`
	Index         tsi1.Config        `toml:"index"`
	tsdb.Config
}

// NewConfig initialises a new config for an Engine.
func NewConfig() Config {
	return Config{
		RetentionInterval: DefaultRetentionInterval,
		EngineOptions:     tsdb.NewEngineOptions(),
		Index:             tsi1.NewConfig(),
		Config:            tsdb.NewConfig(),
	}
}
