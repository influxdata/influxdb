package udp

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultBatchSize is the default UDP batch size.
	DefaultBatchSize = 1000

	// DefaultBatchPending is the default number of pending UDP batches.
	DefaultBatchPending = 5

	// DefaultBatchTimeout is the default UDP batch timeout.
	DefaultBatchTimeout = time.Second
)

type Config struct {
	Enabled     bool   `toml:"enabled"`
	BindAddress string `toml:"bind-address"`

	Database        string        `toml:"database"`
	RetentionPolicy string        `toml:"retention-policy"`
	BatchSize       int           `toml:"batch-size"`
	BatchPending    int           `toml:"batch-pending"`
	BatchTimeout    toml.Duration `toml:"batch-timeout"`
}

// WithDefaults takes the given config and returns a new config with any required
// default values set.
func (c *Config) WithDefaults() *Config {
	d := *c
	if d.BatchSize == 0 {
		d.BatchSize = DefaultBatchSize
	}
	if d.BatchPending == 0 {
		d.BatchPending = DefaultBatchPending
	}
	if d.BatchTimeout == 0 {
		d.BatchTimeout = toml.Duration(DefaultBatchTimeout)
	}
	return &d
}
