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

	// userRequestBytes is nil unless WithUserRequestBytes was given.
	userRequestBytes *prometheus.CounterVec
	// userResponseBytes is nil unless WithUserResponseBytes was given.
	userResponseBytes *prometheus.CounterVec
}

// EventRecorderOption configures an EventRecorder.
type EventRecorderOption func(*EventRecorder, string)

// WithUserResponseBytes additionally records response bytes per user as
//
// http_<subsystem>_user_response_bytes{user_id=<user_id>, endpoint=<endpoint>} ...
//
// It is opt-in because the series count grows with the number of users.
func WithUserResponseBytes() EventRecorderOption {
	return func(r *EventRecorder, subsystem string) {
		r.userResponseBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "http",
			Subsystem: subsystem,
			Name:      "user_response_bytes",
			Help:      "Count of bytes returned, per user",
		}, []string{"user_id", "endpoint"})
	}
}

// WithUserRequestBytes additionally records request bytes per user as
//
// http_<subsystem>_user_request_bytes{user_id=<user_id>, endpoint=<endpoint>} ...
//
// It is opt-in because the series count grows with the number of users.
func WithUserRequestBytes() EventRecorderOption {
	return func(r *EventRecorder, subsystem string) {
		r.userRequestBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "http",
			Subsystem: subsystem,
			Name:      "user_request_bytes",
			Help:      "Count of bytes received, per user",
		}, []string{"user_id", "endpoint"})
	}
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
func NewEventRecorder(subsystem string, opts ...EventRecorderOption) *EventRecorder {
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

	r := &EventRecorder{
		count:         count,
		requestBytes:  requestBytes,
		responseBytes: responseBytes,
	}
	for _, opt := range opts {
		opt(r, subsystem)
	}
	return r
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

	// Events without a valid user (e.g. an authorizer with no user ID such
	// as a JWT lacking a uid claim) are not attributable; they are counted
	// in the aggregate series above only.
	if !e.UserID.Valid() {
		return
	}
	if r.userRequestBytes == nil && r.userResponseBytes == nil {
		return
	}
	userLabels := prometheus.Labels{
		"user_id":  e.UserID.String(),
		"endpoint": e.Endpoint,
	}
	if r.userRequestBytes != nil {
		r.userRequestBytes.With(userLabels).Add(float64(e.RequestBytes))
	}
	if r.userResponseBytes != nil {
		r.userResponseBytes.With(userLabels).Add(float64(e.ResponseBytes))
	}
}

// PrometheusCollectors exposes the prometheus collectors associated with a metric recorder.
func (r *EventRecorder) PrometheusCollectors() []prometheus.Collector {
	cs := []prometheus.Collector{
		r.count,
		r.requestBytes,
		r.responseBytes,
	}
	if r.userRequestBytes != nil {
		cs = append(cs, r.userRequestBytes)
	}
	if r.userResponseBytes != nil {
		cs = append(cs, r.userResponseBytes)
	}
	return cs
}
