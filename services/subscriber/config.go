package subscriber

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultHTTPTimeout      = 30 * time.Second
	DefaultWriteConcurrency = 40
	DefaultWriteBufferSize  = 1000
)

// Config represents a configuration of the subscriber service.
type Config struct {
	// Whether to enable to Subscriber service
	Enabled bool `toml:"enabled"`

	HTTPTimeout toml.Duration `toml:"http-timeout"`

	// The number of writer goroutines processing the write channel.
	WriteConcurrency int `toml:"write-concurrency"`

	// The number of in-flight writes buffered in the write channel.
	WriteBufferSize int `toml:"write-buffer-size"`
}

// NewConfig returns a new instance of a subscriber config.
func NewConfig() Config {
	return Config{
		Enabled:          true,
		HTTPTimeout:      toml.Duration(DefaultHTTPTimeout),
		WriteConcurrency: DefaultWriteConcurrency,
		WriteBufferSize:  DefaultWriteBufferSize,
	}
}

func (c Config) Validate() error {
	if c.HTTPTimeout <= 0 {
		return errors.New("http-timeout must be greater than 0")
	}

	if c.WriteBufferSize <= 0 {
		return errors.New("write-buffer-size must be greater than 0")
	}

	if c.WriteConcurrency <= 0 {
		return errors.New("write-concurrency must be greater than 0")
	}

	return nil
}
