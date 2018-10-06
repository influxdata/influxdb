package storage

import (
	"github.com/influxdata/platform/tsdb"
	"github.com/influxdata/platform/tsdb/tsi1"
)

// Config defaults
const ()

// Config holds the configuration for an Engine.
type Config struct {
	EngineOptions tsdb.EngineOptions `toml:"-"`
	Index         tsi1.Config        `toml:"index"`
	tsdb.Config
}

// NewConfig initialises a new config for an Engine.
func NewConfig() Config {
	return Config{
		EngineOptions: tsdb.NewEngineOptions(),
		Index:         tsi1.NewConfig(),
		Config:        tsdb.NewConfig(),
	}
}
