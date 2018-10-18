package httpd

import (
	"crypto/tls"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress = ":8086"

	// DefaultRealm is the default realm sent back when issuing a basic auth challenge.
	DefaultRealm = "InfluxDB"

	// DefaultBindSocket is the default unix socket to bind to.
	DefaultBindSocket = "/var/run/influxdb.sock"

	// DefaultMaxBodySize is the default maximum size of a client request body, in bytes. Specify 0 for no limit.
	DefaultMaxBodySize = 25e6

	// DefaultEnqueuedWriteTimeout is the maximum time a write request can wait to be processed.
	DefaultEnqueuedWriteTimeout = 30 * time.Second
)

// Config represents a configuration for a HTTP service.
type Config struct {
	Enabled                 bool          `toml:"enabled"`
	BindAddress             string        `toml:"bind-address"`
	AuthEnabled             bool          `toml:"auth-enabled"`
	LogEnabled              bool          `toml:"log-enabled"`
	SuppressWriteLog        bool          `toml:"suppress-write-log"`
	WriteTracing            bool          `toml:"write-tracing"`
	FluxEnabled             bool          `toml:"flux-enabled"`
	PprofEnabled            bool          `toml:"pprof-enabled"`
	DebugPprofEnabled       bool          `toml:"debug-pprof-enabled"`
	HTTPSEnabled            bool          `toml:"https-enabled"`
	HTTPSCertificate        string        `toml:"https-certificate"`
	HTTPSPrivateKey         string        `toml:"https-private-key"`
	MaxRowLimit             int           `toml:"max-row-limit"`
	MaxConnectionLimit      int           `toml:"max-connection-limit"`
	SharedSecret            string        `toml:"shared-secret"`
	Realm                   string        `toml:"realm"`
	UnixSocketEnabled       bool          `toml:"unix-socket-enabled"`
	UnixSocketGroup         *toml.Group   `toml:"unix-socket-group"`
	UnixSocketPermissions   toml.FileMode `toml:"unix-socket-permissions"`
	BindSocket              string        `toml:"bind-socket"`
	MaxBodySize             int           `toml:"max-body-size"`
	AccessLogPath           string        `toml:"access-log-path"`
	MaxConcurrentWriteLimit int           `toml:"max-concurrent-write-limit"`
	MaxEnqueuedWriteLimit   int           `toml:"max-enqueued-write-limit"`
	EnqueuedWriteTimeout    time.Duration `toml:"enqueued-write-timeout"`
	TLS                     *tls.Config   `toml:"-"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:               true,
		FluxEnabled:           false,
		BindAddress:           DefaultBindAddress,
		LogEnabled:            true,
		PprofEnabled:          true,
		DebugPprofEnabled:     false,
		HTTPSEnabled:          false,
		HTTPSCertificate:      "/etc/ssl/influxdb.pem",
		MaxRowLimit:           0,
		Realm:                 DefaultRealm,
		UnixSocketEnabled:     false,
		UnixSocketPermissions: 0777,
		BindSocket:            DefaultBindSocket,
		MaxBodySize:           DefaultMaxBodySize,
		EnqueuedWriteTimeout:  DefaultEnqueuedWriteTimeout,
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
		"access-log-path":      c.AccessLogPath,
	}), nil
}
