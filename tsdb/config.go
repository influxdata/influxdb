package tsdb

import (
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2/toml"
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

	// DefaultMaxPointsPerBlock is the maximum number of points in an encoded
	// block in a TSM file
	DefaultMaxPointsPerBlock = 1000

	// DefaultMaxValuesPerTag is the maximum number of values a tag can have within a measurement.
	DefaultMaxValuesPerTag = 100000

	// DefaultMaxConcurrentCompactions is the maximum number of concurrent full and level compactions
	// that can run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.
	DefaultMaxConcurrentCompactions = 0

	// DefaultMaxIndexLogFileSize is the default threshold, in bytes, when an index
	// write-ahead log file will compact into an index file.
	DefaultMaxIndexLogFileSize = 1 * 1024 * 1024 // 1MB

	// DefaultSeriesIDSetCacheSize is the default number of series ID sets to cache in the TSI index.
	DefaultSeriesIDSetCacheSize = 100

	// DefaultSeriesFileMaxConcurrentSnapshotCompactions is the maximum number of concurrent series
	// partition snapshot compactions that can run at one time.
	// A value of 0 results in runtime.GOMAXPROCS(0).
	DefaultSeriesFileMaxConcurrentSnapshotCompactions = 0
)

// Config holds the configuration for the tsbd package.
type Config struct {
	Dir    string `toml:"dir"`
	Engine string `toml:"-"`
	Index  string `toml:"index-version"`

	// General WAL configuration options
	WALDir string `toml:"wal-dir"`

	// WALMaxConcurrentWrites sets the max number of WAL writes that can be attempted at one time.
	// In reality only one write to disk can run at a time, but we allow the preceding encoding steps
	// to run concurrently. This can cause allocations to increase quickly when writing to a slow disk.
	// Set to 0 to use the default (<nprocs> * 2).
	WALMaxConcurrentWrites int `toml:"wal-max-concurrent-writes"`

	// WALMaxWriteDelay is the max amount of time the WAL will wait to begin a write when there are
	// already WALMaxConcurrentWrites in progress. A value of 0 disables any timeout.
	WALMaxWriteDelay time.Duration `toml:"wal-max-write-delay"`

	// WALFsyncDelay is the amount of time that a write will wait before fsyncing.  A duration
	// greater than 0 can be used to batch up multiple fsync calls.  This is useful for slower
	// disks or when WAL write contention is seen.  A value of 0 fsyncs every write to the WAL.
	WALFsyncDelay toml.Duration `toml:"wal-fsync-delay"`

	// Enables unicode validation on series keys on write.
	ValidateKeys bool `toml:"validate-keys"`

	// When true, skips size validation on fields
	SkipFieldSizeValidation bool `toml:"skip-field-size-validation"`

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

	// SeriesIDSetCacheSize is the number items that can be cached within the TSI index. TSI caching can help
	// with query performance when the same tag key/value predicates are commonly used on queries.
	// Setting series-id-set-cache-size to 0 disables the cache.
	SeriesIDSetCacheSize int `toml:"series-id-set-cache-size"`

	// SeriesFileMaxConcurrentSnapshotCompactions is the maximum number of concurrent snapshot compactions
	// that can be running at one time across all series partitions in a database. Snapshots scheduled
	// to run when the limit is reached are blocked until a running snapshot completes.  Only snapshot
	// compactions are affected by this limit. A value of 0 limits snapshot compactions to the lesser of
	// 8 (series file partition quantity) and runtime.GOMAXPROCS(0).
	SeriesFileMaxConcurrentSnapshotCompactions int `toml:"series-file-max-concurrent-snapshot-compactions"`

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

		MaxConcurrentCompactions: DefaultMaxConcurrentCompactions,

		WALMaxWriteDelay: 10 * time.Minute,

		MaxIndexLogFileSize:  toml.Size(DefaultMaxIndexLogFileSize),
		SeriesIDSetCacheSize: DefaultSeriesIDSetCacheSize,

		SeriesFileMaxConcurrentSnapshotCompactions: DefaultSeriesFileMaxConcurrentSnapshotCompactions,

		TraceLoggingEnabled: false,
		TSMWillNeed:         false,
	}
}

// Validate validates the configuration hold by c.
func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Data.Dir must be specified")
	} else if c.WALDir == "" {
		return errors.New("Data.WALDir must be specified")
	}

	if c.MaxConcurrentCompactions < 0 {
		return errors.New("max-concurrent-compactions must be non-negative")
	}

	if c.SeriesIDSetCacheSize < 0 {
		return errors.New("series-id-set-cache-size must be non-negative")
	}

	if c.SeriesFileMaxConcurrentSnapshotCompactions < 0 {
		return errors.New("series-file-max-concurrent-compactions must be non-negative")
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
