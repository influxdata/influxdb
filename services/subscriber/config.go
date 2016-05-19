package subscriber

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultHTTPTimeout = 30 * time.Second
)

// Config represents a configuration of the subscriber service.
type Config struct {
	// Whether to enable to Subscriber service
	Enabled bool `toml:"enabled"`

	HTTPTimeout toml.Duration `toml:"http-timeout"`
}

// NewConfig returns a new instance of a subscriber config.
func NewConfig() Config {
	return Config{
		Enabled:     true,
		HTTPTimeout: toml.Duration(DefaultHTTPTimeout),
	}
}

func (c Config) Validate() error {
	if c.HTTPTimeout <= 0 {
		return errors.New("http-timeout must be greater than 0")
	}
	return nil
}
