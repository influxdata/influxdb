package engine

import "github.com/influxdb/influxdb/protocol"

// Passed to a shard (local datastore or whatever) that gets yielded points from series.
type Processor interface {
	// (true, nil) if the query should continue. False if processing
	// should stop, because of an error in which case error isn't nil or
	// because the desired data was read succesfully and no more data is
	// needed.
	Yield(s *protocol.Series) (bool, error)
	Name() string

	// Flush any data that could be in the queue
	Close() error
}
