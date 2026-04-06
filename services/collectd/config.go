package collectd

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultBindAddress is the default port to bind to.
	DefaultBindAddress = ":25826"

	// DefaultDatabase is the default DB to write to.
	DefaultDatabase = "collectd"

	// DefaultRetentionPolicy is the default retention policy of the writes.
	DefaultRetentionPolicy = ""

	// DefaultBatchSize is the default write batch size.
	DefaultBatchSize = 5000

	// DefaultBatchPending is the default number of pending write batches.
	DefaultBatchPending = 10

	// DefaultBatchDuration is the default batch timeout duration.
	DefaultBatchDuration = toml.Duration(10 * time.Second)

	// DefaultTypesDB is the default location of the collectd types db file.
	DefaultTypesDB = "/usr/share/collectd/types.db"

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

	// DefaultSecurityLevel is the default security level.
	DefaultSecurityLevel = "none"

	// DefaultAuthFile is the default location of the user/password file.
	DefaultAuthFile = "/etc/collectd/auth_file"

	// DefaultParseMultiValuePlugin is "split", defaulting to version <1.2 where plugin values were split into separate rows
	DefaultParseMultiValuePlugin = "split"
)

// Config represents a configuration for the collectd service.
type Config struct {
	Enabled               bool          `toml:"enabled"`
	BindAddress           string        `toml:"bind-address"`
	Database              string        `toml:"database"`
	RetentionPolicy       string        `toml:"retention-policy"`
	BatchSize             int           `toml:"batch-size"`
	BatchPending          int           `toml:"batch-pending"`
	BatchDuration         toml.Duration `toml:"batch-timeout"`
	ReadBuffer            int           `toml:"read-buffer"`
	TypesDB               string        `toml:"typesdb"`
	SecurityLevel         string        `toml:"security-level"`
	AuthFile              string        `toml:"auth-file"`
	ParseMultiValuePlugin string        `toml:"parse-multivalue-plugin"`
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
	c.ReadBuffer = DefaultReadBuffer
	c.BatchSize = DefaultBatchSize
	c.BatchPending = DefaultBatchPending
	c.BatchDuration = DefaultBatchDuration
	c.TypesDB = DefaultTypesDB
	c.SecurityLevel = DefaultSecurityLevel
	c.AuthFile = DefaultAuthFile
	c.ParseMultiValuePlugin = DefaultParseMultiValuePlugin
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
	if d.BindAddress == "" {
		d.BindAddress = defaults.BindAddress
	}
	if d.Database == "" {
		d.Database = defaults.Database
	}
	if d.RetentionPolicy == "" {
		d.RetentionPolicy = defaults.RetentionPolicy
	}
	if d.BatchSize == 0 {
		d.BatchSize = defaults.BatchSize
	}
	if d.BatchPending == 0 {
		d.BatchPending = defaults.BatchPending
	}
	if d.BatchDuration == 0 {
		d.BatchDuration = defaults.BatchDuration
	}
	if d.ReadBuffer == 0 {
		d.ReadBuffer = defaults.ReadBuffer
	}
	if d.TypesDB == "" {
		d.TypesDB = defaults.TypesDB
	}
	if d.SecurityLevel == "" {
		d.SecurityLevel = defaults.SecurityLevel
	}
	if d.AuthFile == "" {
		d.AuthFile = defaults.AuthFile
	}
	if d.ParseMultiValuePlugin == "" {
		d.ParseMultiValuePlugin = defaults.ParseMultiValuePlugin
	}

	return &d
}

// Validate returns an error if the Config is invalid.
func (c *Config) Validate() error {
	switch c.SecurityLevel {
	case "none", "sign", "encrypt":
	default:
		return errors.New("Invalid security level")
	}

	switch c.ParseMultiValuePlugin {
	case "split", "join":
	default:
		return errors.New(`Invalid value for parse-multivalue-plugin. Valid options are "split" and "join"`)
	}

	return nil
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

		r := []interface{}{true, cc.BindAddress, cc.Database, cc.RetentionPolicy, cc.BatchSize, cc.BatchPending, cc.BatchDuration}
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
