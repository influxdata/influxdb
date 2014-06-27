package http

// This implements the SeriesWriter interface for use with the API

import (
	"github.com/influxdb/influxdb/protocol"
)

type SeriesWriter struct {
	yield func(*protocol.Series) error
}

func NewSeriesWriter(yield func(*protocol.Series) error) *SeriesWriter {
	return &SeriesWriter{yield}
}

func (self *SeriesWriter) Write(series *protocol.Series) error {
	return self.yield(series)
}

func (self *SeriesWriter) Close() {
}
