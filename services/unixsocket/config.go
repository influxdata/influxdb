package unixsocket

import (
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultBindSocket is the default binding interface if none is specified.
	DefaultBindSocket = "/var/run/influxdb.sock"

	// DefaultDatabase is the default database for unix socket traffic.
	DefaultDatabase = "unixsocket"

	// DefaultRetentionPolicy is the default retention policy used for writes.
	DefaultRetentionPolicy = ""

	// DefaultBatchSize is the default unix socket batch size.
	DefaultBatchSize = 5000

	// DefaultBatchPending is the default number of pending unix socket batches.
	DefaultBatchPending = 10

	// DefaultBatchTimeout is the default unix socket batch timeout.
	DefaultBatchTimeout = time.Second

	// DefaultPrecision is the default time precision used for unix socket services.
	DefaultPrecision = "n"

	// DefaultReadBuffer is the default buffer size for the unix socket listener.
	// Sets the size of the operating system's receive buffer associated with
	// the unix socket traffic. Keep in mind that the OS must be able
	// to handle the number set here or the unix socket listener will error and exit.
	//
	// DefaultReadBuffer = 0 means to use the OS default, which is usually too
	// small for high unix socket performance.
	//
	// Increasing OS buffer limits:
	//     Linux:      sudo sysctl -w net.core.rmem_max=<read-buffer>
	//     BSD/Darwin: sudo sysctl -w kern.ipc.maxsockbuf=<read-buffer>
	DefaultReadBuffer = 0
)

// Config holds various configuration settings for the unix socket listener.
type Config struct {
	Enabled    bool   `toml:"enabled"`
	BindSocket string `toml:"bind-socket"`

	Database        string        `toml:"database"`
	RetentionPolicy string        `toml:"retention-policy"`
	BatchSize       int           `toml:"batch-size"`
	BatchPending    int           `toml:"batch-pending"`
	ReadBuffer      int           `toml:"read-buffer"`
	BatchTimeout    toml.Duration `toml:"batch-timeout"`
	Precision       string        `toml:"precision"`
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		BindSocket:      DefaultBindSocket,
		Database:        DefaultDatabase,
		RetentionPolicy: DefaultRetentionPolicy,
		BatchSize:       DefaultBatchSize,
		BatchPending:    DefaultBatchPending,
		BatchTimeout:    toml.Duration(DefaultBatchTimeout),
	}
}

// WithDefaults takes the given config and returns a new config with any required
// default values set.
func (c *Config) WithDefaults() *Config {
	d := *c
	if d.Database == "" {
		d.Database = DefaultDatabase
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
	if d.Precision == "" {
		d.Precision = DefaultPrecision
	}
	if d.ReadBuffer == 0 {
		d.ReadBuffer = DefaultReadBuffer
	}
	return &d
}
