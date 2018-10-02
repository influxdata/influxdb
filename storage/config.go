package storage

import (
	"github.com/influxdata/platform/tsdb"
	"github.com/influxdata/platform/tsdb/index/tsi1"
)

// Config defaults
const ()

// Config holds the configuration for an Engine.
type Config struct {
	EngineOptions tsdb.EngineOptions `toml:"-"`
	Index         tsi1.Config        `toml:"index"`
}
