package coordinator

// This implements the SeriesWriter interface for use with continuous queries to write their output back into the db

import (
	"github.com/influxdb/influxdb/protocol"
)

type ContinuousQueryWriter struct {
	yield func(*protocol.Series) error
}

func NewContinuousQueryWriter(yield func(*protocol.Series) error) *ContinuousQueryWriter {
	return &ContinuousQueryWriter{yield}
}

func (self *ContinuousQueryWriter) Write(series *protocol.Series) error {
	return self.yield(series)
}

func (self *ContinuousQueryWriter) Close() {
}
