package coordinator

import "github.com/influxdb/influxdb/protocol"

type SeriesWriter interface {
	Write(*protocol.Series) error
	Close()
}
