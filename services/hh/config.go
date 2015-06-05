package hh

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultMaxSize is the default maximum size of all hinted handoff queues in bytes.
	DefaultMaxSize = 1024 * 1024 * 1024

	// DefaultRetryInterval is the default amout of time the system waits before
	// attempting to flush hinted handoff queues.
	DefaultRetryInterval = time.Second

	// DefaultMaxBackoffTime is the default maximum time to wait when a node with pending
	// hinted handoff writes is unavailable.
	DefaultMaxBackoffTime = 5 * time.Minute
)

type Config struct {
	Enabled        bool          `toml:"enabled"`
	Dir            string        `toml:"dir"`
	MaxSize        int64         `toml:"max-size"`
	RetryInterval  toml.Duration `toml:"retry-interval"`
	MaxBackoffTime toml.Duration `toml:"max-backoff-time"`
}

func NewConfig() Config {
	return Config{
		Enabled:        true,
		MaxSize:        DefaultMaxSize,
		RetryInterval:  toml.Duration(DefaultRetryInterval),
		MaxBackoffTime: toml.Duration(DefaultMaxBackoffTime),
	}
}
