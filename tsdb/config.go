package tsdb

import (
	"errors"
	"fmt"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/platform/toml"
	"github.com/influxdata/platform/tsdb/defaults"
)

// TODO(jeff): port things to use the defaults package

// EOF represents a "not found" key returned by a Cursor.
const EOF = query.ZeroTime

const ( // See the defaults package for explanations of what these mean
	DefaultEngine                         = defaults.DefaultEngine
	DefaultIndex                          = defaults.DefaultIndex
	DefaultCacheMaxMemorySize             = defaults.DefaultCacheMaxMemorySize
	DefaultCacheSnapshotMemorySize        = defaults.DefaultCacheSnapshotMemorySize
	DefaultCacheSnapshotWriteColdDuration = defaults.DefaultCacheSnapshotWriteColdDuration
	DefaultCompactFullWriteColdDuration   = defaults.DefaultCompactFullWriteColdDuration
	DefaultCompactThroughput              = defaults.DefaultCompactThroughput
	DefaultCompactThroughputBurst         = defaults.DefaultCompactThroughputBurst
	DefaultMaxPointsPerBlock              = defaults.DefaultMaxPointsPerBlock
	DefaultMaxConcurrentCompactions       = defaults.DefaultMaxConcurrentCompactions
	DefaultSeriesFileDirectory            = defaults.DefaultSeriesFileDirectory
)

// Config holds the configuration for the tsbd package.
type Config struct {
	Dir    string `toml:"dir"`
	Engine string `toml:"-"`
	Index  string `toml:"index-version"`

	// General WAL configuration options
	WALDir string `toml:"wal-dir"`

	// WALFsyncDelay is the amount of time that a write will wait before fsyncing.  A duration
	// greater than 0 can be used to batch up multiple fsync calls.  This is useful for slower
	// disks or when WAL write contention is seen.  A value of 0 fsyncs every write to the WAL.
	WALFsyncDelay toml.Duration `toml:"wal-fsync-delay"`

	// Enables unicode validation on series keys on write.
	ValidateKeys bool `toml:"validate-keys"`

	// Query logging
	QueryLogEnabled bool `toml:"query-log-enabled"`

	// Compaction options for tsm1 (descriptions above with defaults)
	CacheMaxMemorySize             toml.Size     `toml:"cache-max-memory-size"`
	CacheSnapshotMemorySize        toml.Size     `toml:"cache-snapshot-memory-size"`
	CacheSnapshotWriteColdDuration toml.Duration `toml:"cache-snapshot-write-cold-duration"`
	CompactFullWriteColdDuration   toml.Duration `toml:"compact-full-write-cold-duration"`
	CompactThroughput              toml.Size     `toml:"compact-throughput"`
	CompactThroughputBurst         toml.Size     `toml:"compact-throughput-burst"`

	// Limits

	// MaxConcurrentCompactions is the maximum number of concurrent level and full compactions
	// that can be running at one time across all shards.  Compactions scheduled to run when the
	// limit is reached are blocked until a running compaction completes.  Snapshot compactions are
	// not affected by this limit.  A value of 0 limits compactions to runtime.GOMAXPROCS(0).
	MaxConcurrentCompactions int `toml:"max-concurrent-compactions"`

	TraceLoggingEnabled bool `toml:"trace-logging-enabled"`

	// TSMWillNeed controls whether we hint to the kernel that we intend to
	// page in mmap'd sections of TSM files. This setting defaults to off, as it has
	// been found to be problematic in some cases. It may help users who have
	// slow disks.
	TSMWillNeed bool `toml:"tsm-use-madv-willneed"`
}

// NewConfig returns the default configuration for tsdb.
func NewConfig() Config {
	return Config{
		Engine: DefaultEngine,
		Index:  DefaultIndex,

		QueryLogEnabled: true,

		CacheMaxMemorySize:             toml.Size(DefaultCacheMaxMemorySize),
		CacheSnapshotMemorySize:        toml.Size(DefaultCacheSnapshotMemorySize),
		CacheSnapshotWriteColdDuration: toml.Duration(DefaultCacheSnapshotWriteColdDuration),
		CompactFullWriteColdDuration:   toml.Duration(DefaultCompactFullWriteColdDuration),
		CompactThroughput:              toml.Size(DefaultCompactThroughput),
		CompactThroughputBurst:         toml.Size(DefaultCompactThroughputBurst),
		MaxConcurrentCompactions:       DefaultMaxConcurrentCompactions,

		TraceLoggingEnabled: false,
		TSMWillNeed:         false,
	}
}

// Validate validates the configuration hold by c.
func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Data.Dir must be specified")
	}

	if c.MaxConcurrentCompactions < 0 {
		return errors.New("max-concurrent-compactions must be greater than 0")
	}

	valid := false
	for _, e := range RegisteredIndexes() {
		if e == c.Index {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("unrecognized index %s", c.Index)
	}

	return nil
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	return diagnostics.RowFromMap(map[string]interface{}{
		"dir":                                c.Dir,
		"wal-dir":                            c.WALDir,
		"wal-fsync-delay":                    c.WALFsyncDelay,
		"cache-max-memory-size":              c.CacheMaxMemorySize,
		"cache-snapshot-memory-size":         c.CacheSnapshotMemorySize,
		"cache-snapshot-write-cold-duration": c.CacheSnapshotWriteColdDuration,
		"compact-full-write-cold-duration":   c.CompactFullWriteColdDuration,
		"max-concurrent-compactions":         c.MaxConcurrentCompactions,
	}), nil
}
