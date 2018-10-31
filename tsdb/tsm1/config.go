package tsm1

import (
	"runtime"
	"time"

	"github.com/influxdata/platform/toml"
)

var (
	// TODO(jeff): document
	DefaultMaxConcurrentOpens = runtime.GOMAXPROCS(0)
)

const (
	// TODO(jeff): document
	DefaultMADVWillNeed = false

	// TODO(jeff): document
	DefaultTraceLoggingEnabled = false
)

// Config contains all of the configuration necessary to run a tsm1 engine.
type Config struct {
	MaxConcurrentOpens  int  `toml:"max-concurrent-opens"`
	MADVWillNeed        bool `toml:"use-madv-willneed"`
	TraceLoggingEnabled bool `toml:"trace-logging-enabled"`

	FileStoreObserver FileStoreObserver `toml:"-"`

	Compaction CompactionConfig `toml:"compaction"`
	Cache      CacheConfig      `toml:"cache"`
	WAL        WALConfig        `toml:"wal"`
}

// NewConfig constructs a Config with the default values.
func NewConfig() Config {
	return Config{
		MaxConcurrentOpens:  DefaultMaxConcurrentOpens,
		MADVWillNeed:        DefaultMADVWillNeed,
		TraceLoggingEnabled: DefaultTraceLoggingEnabled,

		Compaction: CompactionConfig{
			FullWriteColdDuration: toml.Duration(DefaultCompactFullWriteColdDuration),
			Throughput:            toml.Size(DefaultCompactThroughput),
			ThroughputBurst:       toml.Size(DefaultCompactThroughputBurst),
			MaxConcurrent:         DefaultCompactMaxConcurrent,
		},
		Cache: CacheConfig{
			SnapshotMemorySize:        toml.Size(DefaultCacheSnapshotMemorySize),
			MaxMemorySize:             toml.Size(DefaultCacheMaxMemorySize),
			SnapshotWriteColdDuration: toml.Duration(DefaultCacheSnapshotWriteColdDuration),
		},
		WAL: WALConfig{
			Enabled:    DefaultWALEnabled,
			Path:       DefaultWALPath,
			FsyncDelay: toml.Duration(DefaultWALFsyncDelay),
		},
	}
}

const (
	// DefaultCompactFullWriteColdDuration is the duration at which the engine
	// will compact all TSM files in a shard if it hasn't received a write or delete
	DefaultCompactFullWriteColdDuration = time.Duration(4 * time.Hour)

	// DefaultCompactThroughput is the rate limit in bytes per second that we
	// will allow TSM compactions to write to disk. Not that short bursts are allowed
	// to happen at a possibly larger value, set by CompactThroughputBurst.
	// A value of 0 here will disable compaction rate limiting
	DefaultCompactThroughput = 48 * 1024 * 1024

	// DefaultCompactThroughputBurst is the rate limit in bytes per second that we
	// will allow TSM compactions to write to disk. If this is not set, the burst value
	// will be set to equal the normal throughput
	DefaultCompactThroughputBurst = 48 * 1024 * 1024

	// DefaultCompactMaxConcurrent is the maximum number of concurrent full and level compactions
	// that can run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.
	DefaultCompactMaxConcurrent = 0
)

// CompactionConfing holds all of the configuration for compactions. Eventually we want
// to move this out of tsm1 so that it can be scheduled more intelligently.
type CompactionConfig struct {
	FullWriteColdDuration toml.Duration `toml:"full-write-cold-duration"`
	Throughput            toml.Size     `toml:"throughput"`
	ThroughputBurst       toml.Size     `toml:"throughput-burst"`
	MaxConcurrent         int           `toml:"max-concurrent"`

	PlannerCreator func(*FileStore, CompactionConfig) CompactionPlanner `toml:"-"`
}

const (
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
)

// CacheConfig holds all of the configuration for the in memory cache of values that
// are waiting to be snapshot.
type CacheConfig struct {
	MaxMemorySize             toml.Size     `toml:"max-memory-size"`
	SnapshotMemorySize        toml.Size     `toml:"snapshot-memory-size"`
	SnapshotWriteColdDuration toml.Duration `toml:"snapshot-write-cold-duration"`
}

const (
	// TODO(jeff): document
	DefaultWALEnabled = true

	// TODO(jeff): document
	DefaultWALPath = "../wal"

	// TODO(jeff): document
	DefaultWALFsyncDelay = time.Duration(0)
)

// WALConfig holds all of the configuration about the WAL.
type WALConfig struct {
	Enabled    bool          `toml:"enabled"`
	Path       string        `toml:"path"`
	FsyncDelay toml.Duration `toml:"fsync-delay"`
}
