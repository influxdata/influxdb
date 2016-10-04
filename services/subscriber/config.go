package subscriber

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false
	InsecureSkipVerify bool `toml:"insecure-skip-verify"`

	// configure the path to the PEM encoded CA certs file. If the
	// empty string, the default system certs will be used
	CaCerts string `toml:"ca-certs"`
}

// NewConfig returns a new instance of a subscriber config.
func NewConfig() Config {
	return Config{
		Enabled:            true,
		HTTPTimeout:        toml.Duration(DefaultHTTPTimeout),
		InsecureSkipVerify: false,
		CaCerts:            "",
	}
}

func (c Config) Validate() error {
	if c.HTTPTimeout <= 0 {
		return errors.New("http-timeout must be greater than 0")
	}
	if c.CaCerts != "" && !fileExists(c.CaCerts) {
		abspath, err := filepath.Abs(c.CaCerts)
		if err != nil {
			return fmt.Errorf("ca-certs file %s does not exist. Wrapped Error: %v", c.CaCerts, err)
		}
		return fmt.Errorf("ca-certs file %s does not exist", abspath)
	}
	return nil
}

func fileExists(fileName string) bool {
	info, err := os.Stat(fileName)
	return err == nil && !info.IsDir()
}
