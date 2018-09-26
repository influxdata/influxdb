package tsdb

import (
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultEngine is the default engine for new shards
	DefaultEngine = "tsm1"

	// DefaultIndex is the default index for new shards
	DefaultIndex = TSI1IndexName

	// tsdb/engine/wal configuration options

	// Default settings for TSM

	// DefaultCacheMaxMemorySize is the maximum size a shard's cache can
	// reach before it starts rejecting writes.
	DefaultCacheMaxMemorySize = 1024 * 1024 * 1024 // 1GB

	// DefaultCacheSnapshotMemorySize is the size at which the engine will
	// snapshot the cache and write it to a TSM file, freeing up memory
	DefaultCacheSnapshotMemorySize = 25 * 1024 * 1024 // 25MB

	// DefaultCacheSnapshotWriteColdDuration is the length of time at which
	// the engine will snapshot the cache and write it to a new TSM file if
	// the shard hasn't received writes or deletes
	DefaultCacheSnapshotWriteColdDuration = time.Duration(10 * time.Minute)

	// DefaultCompactFullWriteColdDuration is the duration at which the engine
	// will compact all TSM files in a shard if it hasn't received a write or delete
	DefaultCompactFullWriteColdDuration = time.Duration(4 * time.Hour)

	// DefaultCompactThroughput is the rate limit in bytes per second that we
	// will allow TSM compactions to write to disk. Not that short bursts are allowed
	// to happen at a possibly larger value, set by DefaultCompactThroughputBurst.
	// A value of 0 here will disable compaction rate limiting
	DefaultCompactThroughput = 48 * 1024 * 1024

	// DefaultCompactThroughputBurst is the rate limit in bytes per second that we
	// will allow TSM compactions to write to disk. If this is not set, the burst value
	// will be set to equal the normal throughput
	DefaultCompactThroughputBurst = 48 * 1024 * 1024

	// DefaultMaxConcurrentCompactions is the maximum number of concurrent full and level compactions
	// that can run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.
	DefaultMaxConcurrentCompactions = 0

	// DefaultMaxIndexLogFileSize is the default threshold, in bytes, when an index
	// write-ahead log file will compact into an index file.
	DefaultMaxIndexLogFileSize = 1 * 1024 * 1024 // 1MB
)

// Config holds the configuration for the tsbd package.
type Config struct {
	Dir    string `toml:"dir"`
	Engine string `toml:"-"`
	Index  string `toml:"index-version"`

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

	// MaxIndexLogFileSize is the threshold, in bytes, when an index write-ahead log file will
	// compact into an index file. Lower sizes will cause log files to be compacted more quickly
	// and result in lower heap usage at the expense of write throughput. Higher sizes will
	// be compacted less frequently, store more series in-memory, and provide higher write throughput.
	MaxIndexLogFileSize toml.Size `toml:"max-index-log-file-size"`

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

		MaxIndexLogFileSize: toml.Size(DefaultMaxIndexLogFileSize),

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
	for _, e := range RegisteredEngines() {
		if e == c.Engine {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("unrecognized engine %s", c.Engine)
	}

	valid = false
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
