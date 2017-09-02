package meta

import (
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
)

const (
	// DefaultLeaseDuration is the default duration for leases.
	DefaultLeaseDuration = 60 * time.Second

	// DefaultLoggingEnabled determines if log messages are printed for the meta service.
	DefaultLoggingEnabled = true
)

// Config represents the meta configuration.
type Config struct {
	NodeID      uint64 `toml:"node-id"`
	NodeHost    string `toml:"node-host"`
	NodeTCPHost string `toml:"node-tcphost"`

	EtcdEndpoints string `toml:"etdc-endpoints"`

	Dir string `toml:"dir"`

	// in Seconds
	LeaseDuration int64 `toml:"lease-duration"`

	RetentionAutoCreate bool `toml:"retention-autocreate"`
	LoggingEnabled      bool `toml:"logging-enabled"`
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		NodeID:      1,
		NodeHost:    "localhost",
		NodeTCPHost: "localhost:8089",

		EtcdEndpoints: "http://localhost:2379",

		LeaseDuration:       2,
		RetentionAutoCreate: true,
		LoggingEnabled:      DefaultLoggingEnabled,
	}
}

// Validate returns an error if the config is invalid.
func (c *Config) Validate() error {
	allRequired := [][2]string{
		{"node-host", c.NodeHost},
		{"node-tcp-host", c.NodeTCPHost},
		{"etdc-endpoints", c.EtcdEndpoints},
	}

	for _, required := range allRequired {
		if required[1] == "" {
			return errors.New(fmt.Sprintf("%s must be specified", required[0]))
		}
	}

	if c.LeaseDuration <= 0 {
		return errors.New("lease-duration must be a positive number")
	}
	return nil
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c *Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	return diagnostics.RowFromMap(map[string]interface{}{
		"etcd-endpoints": c.EtcdEndpoints,
	}), nil
}
