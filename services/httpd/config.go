package httpd

import "github.com/influxdata/influxdb/monitor/diagnostics"

const (
	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress = ":8086"

	// DefaultRealm is the default realm sent back when issuing a basic auth challenge.
	DefaultRealm = "InfluxDB"

	// DefaultBindSocket is the default unix socket to bind to.
	DefaultBindSocket = "/var/run/influxdb.sock"
)

// Config represents a configuration for a HTTP service.
type Config struct {
	Enabled            bool   `toml:"enabled"`
	BindAddress        string `toml:"bind-address"`
	AuthEnabled        bool   `toml:"auth-enabled"`
	LogEnabled         bool   `toml:"log-enabled"`
	WriteTracing       bool   `toml:"write-tracing"`
	PprofEnabled       bool   `toml:"pprof-enabled"`
	HTTPSEnabled       bool   `toml:"https-enabled"`
	HTTPSCertificate   string `toml:"https-certificate"`
	HTTPSPrivateKey    string `toml:"https-private-key"`
	MaxRowLimit        int    `toml:"max-row-limit"`
	MaxConnectionLimit int    `toml:"max-connection-limit"`
	SharedSecret       string `toml:"shared-secret"`
	Realm              string `toml:"realm"`
	UnixSocketEnabled  bool   `toml:"unix-socket-enabled"`
	BindSocket         string `toml:"bind-socket"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:           true,
		BindAddress:       DefaultBindAddress,
		LogEnabled:        true,
		PprofEnabled:      true,
		HTTPSEnabled:      false,
		HTTPSCertificate:  "/etc/ssl/influxdb.pem",
		MaxRowLimit:       0,
		Realm:             DefaultRealm,
		UnixSocketEnabled: false,
		BindSocket:        DefaultBindSocket,
	}
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	if !c.Enabled {
		return diagnostics.RowFromMap(map[string]interface{}{
			"enabled": false,
		}), nil
	}

	return diagnostics.RowFromMap(map[string]interface{}{
		"enabled":              true,
		"bind-address":         c.BindAddress,
		"https-enabled":        c.HTTPSEnabled,
		"max-row-limit":        c.MaxRowLimit,
		"max-connection-limit": c.MaxConnectionLimit,
	}), nil
}
