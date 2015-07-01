package tsdb

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultMaxWALSize is the default size of the WAL before it is flushed.
	DefaultMaxWALSize = 100 * 1024 * 1024 // 100MB

	// DefaultWALFlushInterval is the frequency the WAL will get flushed if
	// it doesn't reach its size threshold.
	DefaultWALFlushInterval = 10 * time.Minute
)

type Config struct {
	Dir              string        `toml:"dir"`
	MaxWALSize       int           `toml:"max-wal-size"`
	WALFlushInterval toml.Duration `toml:"wal-flush-interval"`
}

func NewConfig() Config {
	return Config{
		MaxWALSize:       DefaultMaxWALSize,
		WALFlushInterval: toml.Duration(DefaultWALFlushInterval),
	}
}
