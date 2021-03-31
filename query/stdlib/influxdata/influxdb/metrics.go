package influxdb

import (
	"context"
	"fmt"
	"time"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	orgLabel = "org"
	opLabel  = "op"
)

type metrics struct {
	ctxLabelKeys []string
	requestDur   *prometheus.HistogramVec
}

// NewMetrics produces a new metrics objects for an influxdb source.
// Currently it just collects the duration of read requests into a histogram.
// ctxLabelKeys is a list of labels to add to the produced metrics.
// The value for a given key will be read off the context.
// The context value must be a string or an implementation of the Stringer interface.
// In addition, produced metrics will be labeled with the orgID and type of operation requested.
func NewMetrics(ctxLabelKeys []string) *metrics {
	labelKeys := make([]string, len(ctxLabelKeys)+2)
	copy(labelKeys, ctxLabelKeys)
	labelKeys[len(labelKeys)-2] = orgLabel
	labelKeys[len(labelKeys)-1] = opLabel

	m := new(metrics)
	m.requestDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "query",
		Subsystem: "influxdb_source",
		Name:      "read_request_duration_seconds",
		Help:      "Histogram of times spent in read requests",
		Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
	}, labelKeys)
	m.ctxLabelKeys = ctxLabelKeys
	return m
}

// PrometheusCollectors satisfies the PrometheusCollector interface.
func (m *metrics) PrometheusCollectors() []prometheus.Collector {
	if m == nil {
		// if metrics happens to be nil here (such as for a test), then let's not panic.
		return nil
	}
	return []prometheus.Collector{
		m.requestDur,
	}
}

func (m *metrics) getLabelValues(ctx context.Context, orgID platform2.ID, op string) []string {
	if m == nil {
		return nil
	}
	labelValues := make([]string, len(m.ctxLabelKeys)+2)
	for i, k := range m.ctxLabelKeys {
		value := ctx.Value(k)
		var str string
		switch v := value.(type) {
		case string:
			str = v
		case fmt.Stringer:
			str = v.String()
		}
		labelValues[i] = str
	}
	labelValues[len(labelValues)-2] = orgID.String()
	labelValues[len(labelValues)-1] = op
	return labelValues
}

func (m *metrics) recordMetrics(labelValues []string, start time.Time) {
	if m == nil {
		return
	}
	m.requestDur.WithLabelValues(labelValues...).Observe(time.Since(start).Seconds())
}
