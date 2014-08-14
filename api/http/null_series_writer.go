package http

import "github.com/influxdb/influxdb/protocol"

var nullSeriesWriter = NewSeriesWriter(func(_ *protocol.Series) error { return nil })
