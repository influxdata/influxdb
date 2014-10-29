package http

// This implements the SeriesWriter interface for use with the API

import (
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/protocol"
)

type SeriesWriter struct {
	yield func(*protocol.Series) error
}

func NewSeriesWriter(yield func(*protocol.Series) error) *SeriesWriter {
	return &SeriesWriter{yield}
}

func (self *SeriesWriter) Yield(series *protocol.Series) (bool, error) {
	err := self.yield(series)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (self *SeriesWriter) Close() error {
	return nil
}

func (self *SeriesWriter) Name() string {
	return "SeriesWriter"
}

func (self *SeriesWriter) Next() engine.Processor {
	return nil
}
