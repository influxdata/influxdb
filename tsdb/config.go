package tsdb

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultMaxWALSize is the default size of the WAL before it is flushed.
	DefaultMaxWALSize = 100 * 1024 * 1024 // 100MB

	// DefaultWALFlushInterval is the frequency the WAL will get flushed if
	// it doesn't reach its size threshold.
	DefaultWALFlushInterval = 10 * time.Minute

	// DefaultWALPartitionFlushDelay is the sleep time between WAL partition flushes.
	DefaultWALPartitionFlushDelay = 2 * time.Second

	// tsdb/engine/wal configuration options

	// DefaultReadySeriesSize of 32KB specifies when a series is eligible to be flushed
	DefaultReadySeriesSize = 30 * 1024

	// DefaultCompactionThreshold flush and compact a partition once this ratio of keys are over the flush size
	DefaultCompactionThreshold = 0.5

	// DefaultMaxSeriesSize specifies the size at which a series will be forced to flush
	DefaultMaxSeriesSize = 1024 * 1024

	// DefaultFlushColdInterval specifies how long after a partition has been cold
	// for writes that a full flush and compaction are forced
	DefaultFlushColdInterval = 5 * time.Minute

	// DefaultParititionSizeThreshold specifies when a partition gets to this size in
	// memory, we should slow down writes until it gets a chance to compact.
	// This will force clients to get backpressure if they're writing too fast. We need
	// this because the WAL can take writes much faster than the index. So eventually
	// we'll need to create backpressure, otherwise we'll fill up the memory and die.
	// This number multiplied by the parition count is roughly the max possible memory
	// size for the in-memory WAL cache.
	DefaultPartitionSizeThreshold = 20 * 1024 * 1024 // 20MB
)

type Config struct {
	Dir string `toml:"dir"`

	// WAL config options for b1 (introduced in 0.9.2)
	MaxWALSize             int           `toml:"max-wal-size"`
	WALFlushInterval       toml.Duration `toml:"wal-flush-interval"`
	WALPartitionFlushDelay toml.Duration `toml:"wal-partition-flush-delay"`

	// WAL configuration options for bz1 (introduced in 0.9.3)
	WALDir                    string        `toml:"wal-dir"`
	WALEnableLogging          bool          `toml:"wal-enable-logging"`
	WALReadySeriesSize        int           `toml:"wal-ready-series-size"`
	WALCompactionThreshold    float64       `toml:"wal-compaction-threshold"`
	WALMaxSeriesSize          int           `toml:"wal-max-series-size"`
	WALFlushColdInterval      toml.Duration `toml:"wal-flush-cold-interval"`
	WALPartitionSizeThreshold uint64        `toml:"wal-partition-size-threshold"`
}

func NewConfig() Config {
	return Config{
		MaxWALSize:             DefaultMaxWALSize,
		WALFlushInterval:       toml.Duration(DefaultWALFlushInterval),
		WALPartitionFlushDelay: toml.Duration(DefaultWALPartitionFlushDelay),

		WALEnableLogging:          true,
		WALReadySeriesSize:        DefaultReadySeriesSize,
		WALCompactionThreshold:    DefaultCompactionThreshold,
		WALMaxSeriesSize:          DefaultMaxSeriesSize,
		WALFlushColdInterval:      toml.Duration(DefaultFlushColdInterval),
		WALPartitionSizeThreshold: DefaultPartitionSizeThreshold,
	}
}
