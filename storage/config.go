package storage

import (
	"time"

	"github.com/influxdata/platform/toml"
	"github.com/influxdata/platform/tsdb/tsi1"
	"github.com/influxdata/platform/tsdb/tsm1"
)

const (
	// TODO(jeff): document
	DefaultRetentionInterval = 1 * time.Hour

	// TODO(jeff): document
	DefaultValidateKeys = false
)

// Config holds the configuration for an Engine.
type Config struct {
	// Frequency of retention in seconds.
	RetentionInterval toml.Duration `toml:"retention-interval"`

	// TODO(jeff): document
	ValidateKeys bool `toml:"validate-keys"`

	Engine tsm1.Config `toml:"engine"`
	Index  tsi1.Config `toml:"index"`
}

// NewConfig initialises a new config for an Engine.
func NewConfig() Config {
	return Config{
		RetentionInterval: toml.Duration(DefaultRetentionInterval),
		ValidateKeys:      DefaultValidateKeys,
		Engine:            tsm1.NewConfig(),
		Index:             tsi1.NewConfig(),
	}
}
