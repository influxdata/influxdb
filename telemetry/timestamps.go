package telemetry

import (
	"time"

	"github.com/influxdata/influxdb/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	// just in case the definition of time.Nanosecond changes from 1.
	nsPerMillisecond = int64(time.Millisecond / time.Nanosecond)
)

var _ prometheus.Transformer = (*AddTimestamps)(nil)

// AddTimestamps enriches prometheus metrics by adding timestamps.
type AddTimestamps struct {
	now func() time.Time
}

// Transform adds now as a timestamp to all metrics.
func (a *AddTimestamps) Transform(mfs []*dto.MetricFamily) []*dto.MetricFamily {
	now := a.now
	if now == nil {
		now = time.Now
	}
	nowMilliseconds := now().UnixNano() / nsPerMillisecond

	for i := range mfs {
		for j := range mfs[i].Metric {
			mfs[i].Metric[j].TimestampMs = &nowMilliseconds
		}
	}
	return mfs
}
