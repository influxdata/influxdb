package tsi1

import "github.com/influxdata/platform/toml"

// DefaultMaxIndexLogFileSize is the default threshold, in bytes, when an index
// write-ahead log file will compact into an index file.
const DefaultMaxIndexLogFileSize = 1 * 1024 * 1024 // 1MB

// DefaultIndexDirectoryName is the default name of the directory holding the
// index data.
const DefaultIndexDirectoryName = "index"

// Config holds configurable Index options.
type Config struct {
	// MaxIndexLogFileSize is the threshold, in bytes, when an index write-ahead log file will
	// compact into an index file. Lower sizes will cause log files to be compacted more quickly
	// and result in lower heap usage at the expense of write throughput. Higher sizes will
	// be compacted less frequently, store more series in-memory, and provide higher write throughput.
	MaxIndexLogFileSize toml.Size `toml:"max-index-log-file-size"`
}

// NewConfig returns a new Config.
func NewConfig() Config {
	return Config{
		MaxIndexLogFileSize: toml.Size(DefaultMaxIndexLogFileSize),
	}
}
