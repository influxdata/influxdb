package tsm1

import (
	"runtime"
	"time"

	"github.com/influxdata/influxdb/v2/toml"
)

var DefaultMaxConcurrentOpens = runtime.GOMAXPROCS(0)

const (
	DefaultMADVWillNeed = false

	// DefaultLargeSeriesWriteThreshold is the number of series per write
	// that requires the series index be pregrown before insert.
	DefaultLargeSeriesWriteThreshold = 10000
)

// Config contains all of the configuration necessary to run a tsm1 engine.
type Config struct {
	// MacConcurrentOpens controls the concurrency of opening tsm files during
	// engine opening.
	MaxConcurrentOpens int `toml:"max-concurrent-opens"`

	// MADVWillNeed controls whether we hint to the kernel that we intend to page
	// in mmap'd sections of TSM files. This setting defaults to off, as it has
	// been found to be problematic in some cases. It may help users who have
	// slow disks.
	MADVWillNeed bool `toml:"use-madv-willneed"`

	// LargeSeriesWriteThreshold is the threshold before a write requires
	// preallocation to improve throughput. Currently used in the series file.
	LargeSeriesWriteThreshold int `toml:"large-series-write-threshold"`

	Compaction CompactionConfig `toml:"compaction"`
	Cache      CacheConfig      `toml:"cache"`
}

// NewConfig constructs a Config with the default values.
func NewConfig() Config {
	return Config{
		MaxConcurrentOpens:        DefaultMaxConcurrentOpens,
		MADVWillNeed:              DefaultMADVWillNeed,
		LargeSeriesWriteThreshold: DefaultLargeSeriesWriteThreshold,

		Cache: NewCacheConfig(),
		Compaction: CompactionConfig{
			FullWriteColdDuration: toml.Duration(DefaultCompactFullWriteColdDuration),
			Throughput:            toml.Size(DefaultCompactThroughput),
			ThroughputBurst:       toml.Size(DefaultCompactThroughputBurst),
			MaxConcurrent:         DefaultCompactMaxConcurrent,
		},
	}
}

const (
	DefaultCompactFullWriteColdDuration = time.Duration(4 * time.Hour)
	DefaultCompactThroughput            = 48 * 1024 * 1024
	DefaultCompactThroughputBurst       = 48 * 1024 * 1024
	DefaultCompactMaxConcurrent         = 0
)

// CompactionConfing holds all of the configuration for compactions. Eventually we want
// to move this out of tsm1 so that it can be scheduled more intelligently.
type CompactionConfig struct {
	// FullWriteColdDuration is the duration at which the engine will compact all TSM
	// files in a shard if it hasn't received a write or delete
	FullWriteColdDuration toml.Duration `toml:"full-write-cold-duration"`

	// Throughput is the rate limit in bytes per second that we will allow TSM compactions
	// to write to disk. Not that short bursts are allowed to happen at a possibly larger
	// value, set by CompactThroughputBurst. A value of 0 here will disable compaction rate
	// limiting
	Throughput toml.Size `toml:"throughput"`

	// ThroughputBurst is the rate limit in bytes per second that we will allow TSM compactions
	// to write to disk. If this is not set, the burst value will be set to equal the normal
	// throughput
	ThroughputBurst toml.Size `toml:"throughput-burst"`

	// MaxConcurrent is the maximum number of concurrent full and level compactions that can
	// run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.
	MaxConcurrent int `toml:"max-concurrent"`
}

// Default Cache configuration values.
const (
	DefaultCacheMaxMemorySize             = toml.Size(1024 << 20)           // 1GB
	DefaultCacheSnapshotMemorySize        = toml.Size(25 << 20)             // 25MB
	DefaultCacheSnapshotAgeDuration       = toml.Duration(0)                // Defaults to off.
	DefaultCacheSnapshotWriteColdDuration = toml.Duration(10 * time.Minute) // Ten minutes
)

// CacheConfig holds all of the configuration for the in memory cache of values that
// are waiting to be snapshot.
type CacheConfig struct {
	// MaxMemorySize is the maximum size a shard's cache can reach before it starts
	// rejecting writes.
	MaxMemorySize toml.Size `toml:"max-memory-size"`

	// SnapshotMemorySize is the size at which the engine will snapshot the cache and
	// write it to a TSM file, freeing up memory
	SnapshotMemorySize toml.Size `toml:"snapshot-memory-size"`

	// SnapshotAgeDuration, when set, will ensure that the cache is always snapshotted
	// if it's age is greater than this duration, regardless of the cache's size.
	SnapshotAgeDuration toml.Duration `toml:"snapshot-age-duration"`

	// SnapshotWriteColdDuration is the length of time at which the engine will snapshot
	// the cache and write it to a new TSM file if the shard hasn't received writes or
	// deletes.
	//
	// SnapshotWriteColdDuration should not be larger than SnapshotAgeDuration
	SnapshotWriteColdDuration toml.Duration `toml:"snapshot-write-cold-duration"`
}

// NewCacheConfig initialises a new CacheConfig with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
		MaxMemorySize:             DefaultCacheMaxMemorySize,
		SnapshotMemorySize:        DefaultCacheSnapshotMemorySize,
		SnapshotAgeDuration:       DefaultCacheSnapshotAgeDuration,
		SnapshotWriteColdDuration: DefaultCacheSnapshotWriteColdDuration,
	}
}

// Default WAL configuration values.
const (
	DefaultWALEnabled    = true
	DefaultWALFsyncDelay = time.Duration(0)
)

// WALConfig holds all of the configuration about the WAL.
type WALConfig struct {
	// Enabled controls if the WAL is enabled.
	Enabled bool `toml:"enabled"`

	// WALFsyncDelay is the amount of time that a write will wait before fsyncing.  A
	// duration greater than 0 can be used to batch up multiple fsync calls.  This is
	// useful for slower disks or when WAL write contention is seen.  A value of 0 fsyncs
	// every write to the WAL.
	FsyncDelay toml.Duration `toml:"fsync-delay"`
}

func NewWALConfig() WALConfig {
	return WALConfig{
		Enabled:    DefaultWALEnabled,
		FsyncDelay: toml.Duration(DefaultWALFsyncDelay),
	}
}
