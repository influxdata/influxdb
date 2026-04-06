package udp

import (
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultBindAddress is the default binding interface if none is specified.
	DefaultBindAddress = ":8089"

	// DefaultDatabase is the default database for UDP traffic.
	DefaultDatabase = "udp"

	// DefaultRetentionPolicy is the default retention policy used for writes.
	DefaultRetentionPolicy = ""

	// DefaultBatchSize is the default UDP batch size.
	DefaultBatchSize = 5000

	// DefaultBatchPending is the default number of pending UDP batches.
	DefaultBatchPending = 10

	// DefaultBatchTimeout is the default UDP batch timeout.
	DefaultBatchTimeout = time.Second

	// DefaultPrecision is the default time precision used for UDP services.
	DefaultPrecision = "n"

	// DefaultReadBuffer is the default buffer size for the UDP listener.
	// Sets the size of the operating system's receive buffer associated with
	// the UDP traffic. Keep in mind that the OS must be able
	// to handle the number set here or the UDP listener will error and exit.
	//
	// DefaultReadBuffer = 0 means to use the OS default, which is usually too
	// small for high UDP performance.
	//
	// Increasing OS buffer limits:
	//     Linux:      sudo sysctl -w net.core.rmem_max=<read-buffer>
	//     BSD/Darwin: sudo sysctl -w kern.ipc.maxsockbuf=<read-buffer>
	DefaultReadBuffer = 0

	// DefaultWriters is the default number of writers.
	DefaultWriters = 1
)

// Config holds various configuration settings for the UDP listener.
type Config struct {
	Enabled     bool   `toml:"enabled"`
	BindAddress string `toml:"bind-address"`

	Database        string        `toml:"database"`
	RetentionPolicy string        `toml:"retention-policy"`
	BatchSize       int           `toml:"batch-size"`
	BatchPending    int           `toml:"batch-pending"`
	ReadBuffer      int           `toml:"read-buffer"`
	BatchTimeout    toml.Duration `toml:"batch-timeout"`
	Precision       string        `toml:"precision"`
	Writers         int           `toml:"writers"`
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
	c.BatchSize = DefaultBatchSize
	c.BatchPending = DefaultBatchPending
	c.ReadBuffer = DefaultReadBuffer
	c.BatchTimeout = toml.Duration(DefaultBatchTimeout)
	c.Precision = DefaultPrecision
	c.Writers = DefaultWriters
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	var c Config
	c.ApplyDefaults()
	return c
}

// WithDefaults takes the given config and returns a new config with any
// zero-valued fields filled in from ApplyDefaults. Existing non-zero values
// are preserved. ApplyDefaults remains the single source of truth for what
// the default values are.
func (c *Config) WithDefaults() *Config {
	d := *c
	var defaults Config
	defaults.ApplyDefaults()
	if d.Database == "" {
		d.Database = defaults.Database
	}
	if d.BatchSize == 0 {
		d.BatchSize = defaults.BatchSize
	}
	if d.BatchPending == 0 {
		d.BatchPending = defaults.BatchPending
	}
	if d.BatchTimeout == 0 {
		d.BatchTimeout = defaults.BatchTimeout
	}
	if d.Precision == "" {
		d.Precision = defaults.Precision
	}
	if d.ReadBuffer == 0 {
		d.ReadBuffer = defaults.ReadBuffer
	}
	if d.Writers == 0 {
		d.Writers = defaults.Writers
	}
	return &d
}

// Configs wraps a slice of Config to aggregate diagnostics.
type Configs []Config

// Diagnostics returns one set of diagnostics for all of the Configs.
func (c Configs) Diagnostics() (*diagnostics.Diagnostics, error) {
	d := &diagnostics.Diagnostics{
		Columns: []string{"enabled", "bind-address", "database", "retention-policy", "batch-size", "batch-pending", "batch-timeout", "precision", "writers"},
	}

	for _, cc := range c {
		if !cc.Enabled {
			d.AddRow([]interface{}{false})
			continue
		}

		r := []interface{}{true, cc.BindAddress, cc.Database, cc.RetentionPolicy, cc.BatchSize, cc.BatchPending, cc.BatchTimeout, cc.Precision, cc.Writers}
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
