package mock

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

type MetaClient struct {
	ShardsByTimeRangeFn func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error)
}

func (m *MetaClient) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
	return m.ShardsByTimeRangeFn(sources, tmin, tmax)
}
