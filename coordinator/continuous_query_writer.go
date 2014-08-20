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

func (self *ContinuousQueryWriter) Yield(series *protocol.Series) (bool, error) {
	err := self.yield(series)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (self *ContinuousQueryWriter) Close() error {
	return nil
}

func (self *ContinuousQueryWriter) Name() string {
	return "ContinuousQueryWriter"
}
