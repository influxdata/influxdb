package tsdb

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultEngine is the default engine for new shards
	DefaultEngine = "bz1"

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
	DefaultFlushColdInterval = 5 * time.Second

	// DefaultParititionSizeThreshold specifies when a partition gets to this size in
	// memory, we should slow down writes until it gets a chance to compact.
	// This will force clients to get backpressure if they're writing too fast. We need
	// this because the WAL can take writes much faster than the index. So eventually
	// we'll need to create backpressure, otherwise we'll fill up the memory and die.
	// This number multiplied by the parition count is roughly the max possible memory
	// size for the in-memory WAL cache.
	DefaultPartitionSizeThreshold = 50 * 1024 * 1024 // 50MB

	// Default WAL settings for the TSM1 WAL
	DefaultFlushMemorySizeThreshold    = 5 * 1024 * 1024   // 5MB
	DefaultMaxMemorySizeThreshold      = 100 * 1024 * 1024 // 100MB
	DefaultIndexCompactionAge          = time.Minute
	DefaultIndexMinCompactionInterval  = time.Minute
	DefaultIndexMinCompactionFileCount = 5
	DefaultIndexCompactionFullAge      = 5 * time.Minute
)

// compatDuration is a wrapper type used as a bridge to using toml.Duration where
// time.Duration was used previously.
type compatDuration toml.Duration

func (c *compatDuration) UnmarshalText(text []byte) error {
	// Check if the value is an integer
	n, err := strconv.Atoi(string(text))
	if err == nil {
		// Previously an integer was interpreted as time in seconds.
		*c = compatDuration(time.Duration(n) * time.Second)
		return nil
	}

	d := toml.Duration(*c)
	err = d.UnmarshalText(text)
	if err != nil {
		return err
	}
	*c = compatDuration(d)
	return nil
}

type Config struct {
	Dir    string `toml:"dir"`
	Engine string `toml:"engine"`

	// WAL config options for b1 (introduced in 0.9.2)
	MaxWALSize             int           `toml:"max-wal-size"`
	WALFlushInterval       toml.Duration `toml:"wal-flush-interval"`
	WALPartitionFlushDelay toml.Duration `toml:"wal-partition-flush-delay"`

	// WAL configuration options for bz1 (introduced in 0.9.3)
	WALDir                    string        `toml:"wal-dir"`
	WALLoggingEnabled         bool          `toml:"wal-logging-enabled"`
	WALReadySeriesSize        int           `toml:"wal-ready-series-size"`
	WALCompactionThreshold    float64       `toml:"wal-compaction-threshold"`
	WALMaxSeriesSize          int           `toml:"wal-max-series-size"`
	WALFlushColdInterval      toml.Duration `toml:"wal-flush-cold-interval"`
	WALPartitionSizeThreshold uint64        `toml:"wal-partition-size-threshold"`

	// WAL configuration options for tsm1 introduced in 0.9.5
	WALFlushMemorySizeThreshold int `toml:"wal-flush-memory-size-threshold"`
	WALMaxMemorySizeThreshold   int `toml:"wal-max-memory-size-threshold"`

	// compaction options for tsm1 introduced in 0.9.5

	// IndexCompactionAge specifies the duration after the data file creation time
	// at which it is eligible to be compacted
	IndexCompactionAge compatDuration `toml:"index-compaction-age"`

	// IndexMinimumCompactionInterval specifies the minimum amount of time that must
	// pass after a compaction before another compaction is run
	IndexMinCompactionInterval compatDuration `toml:"index-min-compaction-interval"`

	// IndexCompactionFileCount specifies the minimum number of data files that
	// must be eligible for compaction before actually running one
	IndexMinCompactionFileCount int `toml:"index-compaction-min-file-count"`

	// IndexCompactionFullAge specifies how long after the last write was received
	// in the WAL that a full compaction should be performed.
	IndexCompactionFullAge compatDuration `toml:"index-compaction-full-age"`

	// Query logging
	QueryLogEnabled bool `toml:"query-log-enabled"`
}

func NewConfig() Config {
	defaultEngine := DefaultEngine
	if engine := os.Getenv("INFLUXDB_DATA_ENGINE"); engine != "" {
		log.Println("TSDB engine selected via environment variable:", engine)
		defaultEngine = engine
	}

	return Config{
		Engine:                 defaultEngine,
		MaxWALSize:             DefaultMaxWALSize,
		WALFlushInterval:       toml.Duration(DefaultWALFlushInterval),
		WALPartitionFlushDelay: toml.Duration(DefaultWALPartitionFlushDelay),

		WALLoggingEnabled:           true,
		WALReadySeriesSize:          DefaultReadySeriesSize,
		WALCompactionThreshold:      DefaultCompactionThreshold,
		WALMaxSeriesSize:            DefaultMaxSeriesSize,
		WALFlushColdInterval:        toml.Duration(DefaultFlushColdInterval),
		WALPartitionSizeThreshold:   DefaultPartitionSizeThreshold,
		WALFlushMemorySizeThreshold: DefaultFlushMemorySizeThreshold,
		WALMaxMemorySizeThreshold:   DefaultMaxMemorySizeThreshold,
		IndexCompactionAge:          compatDuration(DefaultIndexCompactionAge),
		IndexMinCompactionFileCount: DefaultIndexMinCompactionFileCount,
		IndexCompactionFullAge:      compatDuration(DefaultIndexCompactionFullAge),
		IndexMinCompactionInterval:  compatDuration(DefaultIndexMinCompactionInterval),

		QueryLogEnabled: true,
	}
}

func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Data.Dir must be specified")
	} else if c.WALDir == "" {
		return errors.New("Data.WALDir must be specified")
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

	return nil
}
