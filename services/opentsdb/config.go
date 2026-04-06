package opentsdb

import (
	"crypto/tls"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultBindAddress is the default address that the service binds to.
	DefaultBindAddress = ":4242"

	// DefaultDatabase is the default database used for writes.
	DefaultDatabase = "opentsdb"

	// DefaultRetentionPolicy is the default retention policy used for writes.
	DefaultRetentionPolicy = ""

	// DefaultConsistencyLevel is the default write consistency level.
	DefaultConsistencyLevel = "one"

	// DefaultBatchSize is the default OpenTSDB batch size.
	DefaultBatchSize = 1000

	// DefaultBatchTimeout is the default OpenTSDB batch timeout.
	DefaultBatchTimeout = time.Second

	// DefaultBatchPending is the default number of batches that can be in the queue.
	DefaultBatchPending = 5

	// DefaultCertificate is the default location of the certificate used when TLS is enabled.
	// For defaults, we assume this also contains the private key.
	DefaultCertificate = "/etc/ssl/influxdb.pem"
)

// Config represents the configuration of the OpenTSDB service.
type Config struct {
	Enabled             bool          `toml:"enabled"`
	BindAddress         string        `toml:"bind-address"`
	Database            string        `toml:"database"`
	RetentionPolicy     string        `toml:"retention-policy"`
	ConsistencyLevel    string        `toml:"consistency-level"`
	TLSEnabled          bool          `toml:"tls-enabled"`
	Certificate         string        `toml:"certificate"`
	PrivateKey          string        `toml:"private-key"`
	InsecureCertificate bool          `toml:"insecure-certificate"`
	BatchSize           int           `toml:"batch-size"`
	BatchPending        int           `toml:"batch-pending"`
	BatchTimeout        toml.Duration `toml:"batch-timeout"`
	LogPointErrors      bool          `toml:"log-point-errors"`
	TLS                 *tls.Config   `toml:"-"`
}

// Compile-time check that *Config implements toml.Defaulter so it can be used
// as a slice element type with ApplyEnvOverrides.
var _ toml.Defaulter = (*Config)(nil)

// ApplyDefaults populates the Config with default values. It is called by
// NewConfig and by toml.ApplyEnvOverrides on slice elements appended via
// indexed environment variables. ApplyDefaults assumes the receiver is a
// freshly constructed (zero-valued) Config and sets fields unconditionally.
func (c *Config) ApplyDefaults() {
	c.BindAddress = DefaultBindAddress
	c.Database = DefaultDatabase
	c.RetentionPolicy = DefaultRetentionPolicy
	c.ConsistencyLevel = DefaultConsistencyLevel
	c.TLSEnabled = false
	c.Certificate = DefaultCertificate // TLSCertLoader will default PrivateKey if PrivateKey remains empty.
	c.BatchSize = DefaultBatchSize
	c.BatchPending = DefaultBatchPending
	c.BatchTimeout = toml.Duration(DefaultBatchTimeout)
	c.LogPointErrors = true
}

// NewConfig returns a new config for the service.
func NewConfig() Config {
	var c Config
	c.ApplyDefaults()
	return c
}

// WithDefaults takes the given config and returns a new config with any required
// default values set.
func (c *Config) WithDefaults() *Config {
	d := *c
	if d.BindAddress == "" {
		d.BindAddress = DefaultBindAddress
	}
	if d.Database == "" {
		d.Database = DefaultDatabase
	}
	if d.RetentionPolicy == "" {
		d.RetentionPolicy = DefaultRetentionPolicy
	}
	if d.ConsistencyLevel == "" {
		d.ConsistencyLevel = DefaultConsistencyLevel
	}
	if d.Certificate == "" {
		d.Certificate = DefaultCertificate
	}
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

// Configs wraps a slice of Config to aggregate diagnostics.
type Configs []Config

// Diagnostics returns one set of diagnostics for all of the Configs.
func (c Configs) Diagnostics() (*diagnostics.Diagnostics, error) {
	d := &diagnostics.Diagnostics{
		Columns: []string{"enabled", "bind-address", "database", "retention-policy", "batch-size", "batch-pending", "batch-timeout"},
	}

	for _, cc := range c {
		if !cc.Enabled {
			d.AddRow([]interface{}{false})
			continue
		}

		r := []interface{}{true, cc.BindAddress, cc.Database, cc.RetentionPolicy, cc.BatchSize, cc.BatchPending, cc.BatchTimeout}
		d.AddRow(r)
	}

	return d, nil
}

// Enabled returns true if any underlying Config is Enabled.
func (c Configs) Enabled() bool {
	for _, cc := range c {
		if cc.Enabled {
			return true
		}
	}
	return false
}
