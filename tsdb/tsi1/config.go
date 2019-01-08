package tsi1

import "github.com/influxdata/influxdb/toml"

// DefaultMaxIndexLogFileSize is the default threshold, in bytes, when an index
// write-ahead log file will compact into an index file.
const DefaultMaxIndexLogFileSize = 1 * 1024 * 1024 // 1MB

// DefaultSeriesIDSetCacheSize is the default number of series ID sets to cache.
const DefaultSeriesIDSetCacheSize = 1000

// Config holds configurable Index options.
type Config struct {
	// MaxIndexLogFileSize is the threshold, in bytes, when an index write-ahead log file will
	// compact into an index file. Lower sizes will cause log files to be compacted more quickly
	// and result in lower heap usage at the expense of write throughput. Higher sizes will
	// be compacted less frequently, store more series in-memory, and provide higher write throughput.
	MaxIndexLogFileSize toml.Size `toml:"max-index-log-file-size"`

	// SeriesIDSetCacheSize determines the size taken up by the cache of series ID
	// sets in the index. Since a series id set is a compressed bitmap of all series ids
	// matching a tag key/value pair, setting this size does not necessarily limit the
	// size on heap the cache takes up. Care should be taken.
	//
	// The cache uses an LRU strategy for eviction. Setting the value to 0 will
	// disable the cache.
	SeriesIDSetCacheSize uint64
}

// NewConfig returns a new Config.
func NewConfig() Config {
	return Config{
		MaxIndexLogFileSize:  toml.Size(DefaultMaxIndexLogFileSize),
		SeriesIDSetCacheSize: DefaultSeriesIDSetCacheSize,
	}
}
