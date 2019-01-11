// package compat helps with migrating toml files from influxdb.
//
// Platform has a different config layout that doesn't match the
// layout in the old toml files. Since we may want the ability to
// load those old files and still have them apply their relevant
// settings, this package allows one to convert from an old config
// struct to a new one.
package compat

import (
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/influxdb/tsdb/tsm1"
)

// Config matches the old toml layout from the influxdb repo, so that we can read
// in those files and convert them to the new config layout.
type Config struct {
	Dir                            string        `toml:"dir"`
	WALDir                         string        `toml:"wal-dir"`
	WALFsyncDelay                  toml.Duration `toml:"wal-fsync-delay"`
	ValidateKeys                   bool          `toml:"validate-keys"`
	CacheMaxMemorySize             toml.Size     `toml:"cache-max-memory-size"`
	CacheSnapshotMemorySize        toml.Size     `toml:"cache-snapshot-memory-size"`
	CacheSnapshotWriteColdDuration toml.Duration `toml:"cache-snapshot-write-cold-duration"`
	CompactFullWriteColdDuration   toml.Duration `toml:"compact-full-write-cold-duration"`
	CompactThroughput              toml.Size     `toml:"compact-throughput"`
	CompactThroughputBurst         toml.Size     `toml:"compact-throughput-burst"`
	MaxConcurrentCompactions       int           `toml:"max-concurrent-compactions"`
	TraceLoggingEnabled            bool          `toml:"trace-logging-enabled"`
	TSMWillNeed                    bool          `toml:"tsm-use-madv-willneed"`
}

// NewConfig constructs an old Config struct with appropriate defaults for a new Config.
func NewConfig() Config {
	return Config{
		WALFsyncDelay:                  toml.Duration(tsm1.DefaultWALFsyncDelay),
		ValidateKeys:                   storage.DefaultValidateKeys,
		CacheMaxMemorySize:             toml.Size(tsm1.DefaultCacheMaxMemorySize),
		CacheSnapshotMemorySize:        toml.Size(tsm1.DefaultCacheSnapshotMemorySize),
		CacheSnapshotWriteColdDuration: toml.Duration(tsm1.DefaultCacheSnapshotWriteColdDuration),
		CompactFullWriteColdDuration:   toml.Duration(tsm1.DefaultCompactFullWriteColdDuration),
		CompactThroughput:              toml.Size(tsm1.DefaultCompactThroughput),
		CompactThroughputBurst:         toml.Size(tsm1.DefaultCompactThroughputBurst),
		MaxConcurrentCompactions:       tsm1.DefaultCompactMaxConcurrent,
		TraceLoggingEnabled:            storage.DefaultTraceLoggingEnabled,
		TSMWillNeed:                    tsm1.DefaultMADVWillNeed,
	}
}

// Convert takes an old Config and converts it into a new Config. It also returns the value
// of the Dir key so that it can be passed through appropriately to the storage engine constructor.
func Convert(oldConfig Config) (string, storage.Config) {
	newConfig := storage.NewConfig()
	newConfig.TraceLoggingEnabled = oldConfig.TraceLoggingEnabled
	newConfig.ValidateKeys = oldConfig.ValidateKeys
	newConfig.Engine.MADVWillNeed = oldConfig.TSMWillNeed
	newConfig.Engine.Cache.MaxMemorySize = oldConfig.CacheMaxMemorySize
	newConfig.Engine.Cache.SnapshotMemorySize = oldConfig.CacheSnapshotMemorySize
	newConfig.Engine.Cache.SnapshotWriteColdDuration = oldConfig.CacheSnapshotWriteColdDuration
	newConfig.Engine.Compaction.FullWriteColdDuration = oldConfig.CompactFullWriteColdDuration
	newConfig.Engine.Compaction.Throughput = oldConfig.CompactThroughput
	newConfig.Engine.Compaction.ThroughputBurst = oldConfig.CompactThroughputBurst
	newConfig.Engine.Compaction.MaxConcurrent = oldConfig.MaxConcurrentCompactions
	newConfig.WALPath = oldConfig.WALDir
	newConfig.WAL.FsyncDelay = oldConfig.WALFsyncDelay
	return oldConfig.Dir, newConfig
}
