package prometheus

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/prometheus/client_golang/prometheus"
)

// EventRecorder implements http/metric.EventRecorder. It is used to collect
// http api metrics.
type EventRecorder struct {
	count         *prometheus.CounterVec
	requestBytes  *prometheus.CounterVec
	responseBytes *prometheus.CounterVec
}

// NewEventRecorder returns an instance of a metric event recorder. Subsystem is expected to be
// descriptive of the type of metric being recorded. Possible values may include write, query,
// task, dashboard, etc.
//
// # The general structure of the metrics produced from the metric recorder should be
//
// http_<subsystem>_request_count{org_id=<org_id>, status=<status>, endpoint=<endpoint>} ...
// http_<subsystem>_request_bytes{org_id=<org_id>, status=<status>, endpoint=<endpoint>} ...
// http_<subsystem>_response_bytes{org_id=<org_id>, status=<status>, endpoint=<endpoint>} ...
func NewEventRecorder(subsystem string) *EventRecorder {
	const namespace = "http"

	labels := []string{"org_id", "status", "endpoint"}

	count := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "request_count",
		Help:      "Total number of query requests",
	}, labels)

	requestBytes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "request_bytes",
		Help:      "Count of bytes received",
	}, labels)

	responseBytes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "response_bytes",
		Help:      "Count of bytes returned",
	}, labels)

	return &EventRecorder{
		count:         count,
		requestBytes:  requestBytes,
		responseBytes: responseBytes,
	}
}

// Record metric records the request count, response bytes, and request bytes with labels
// for the org, endpoint, and status.
func (r *EventRecorder) Record(ctx context.Context, e metric.Event) {
	labels := prometheus.Labels{
		"org_id":   e.OrgID.String(),
		"endpoint": e.Endpoint,
		"status":   fmt.Sprintf("%d", e.Status),
	}
	r.count.With(labels).Inc()
	r.requestBytes.With(labels).Add(float64(e.RequestBytes))
	r.responseBytes.With(labels).Add(float64(e.ResponseBytes))
}

// PrometheusCollectors exposes the prometheus collectors associated with a metric recorder.
func (r *EventRecorder) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		r.count,
		r.requestBytes,
		r.responseBytes,
	}
}
