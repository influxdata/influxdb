package engine

import (
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/protocol"
)

type EngineI interface {
	RunQuery(user common.User, database string, query string, localOnly bool, yield func(*protocol.Series) error) error
}
