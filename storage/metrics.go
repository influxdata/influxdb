package storage

import (
	"sort"

	"github.com/prometheus/client_golang/prometheus"
)

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

const retentionSubsystem = "retention" // sub-system associated with metrics for writing points.

// retentionMetrics is a set of metrics concerned with tracking data about retention policies.
type retentionMetrics struct {
	labels        prometheus.Labels
	Checks        *prometheus.CounterVec
	CheckDuration *prometheus.HistogramVec
	Unprocessable *prometheus.CounterVec
	Series        *prometheus.CounterVec
}

func newRetentionMetrics(labels prometheus.Labels) *retentionMetrics {
	var names []string
	for k := range labels {
		names = append(names, k)
	}
	names = append(names, "status") // All metrics include status
	sort.Strings(names)

	return &retentionMetrics{
		labels: labels,
		Checks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: retentionSubsystem,
			Name:      "checks_total",
			Help:      "Number of retention check operations performed.",
		}, names),

		CheckDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: retentionSubsystem,
			Name:      "check_duration_seconds",
			Help:      "Time taken to perform a successful retention check.",
			// 25 buckets spaced exponentially between 10s and ~2h
			Buckets: prometheus.ExponentialBuckets(10, 1.32, 25),
		}, names),

		Unprocessable: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: retentionSubsystem,
			Name:      "unprocessable_buckets_total",
			Help:      "Number of buckets that could not be operated on.",
		}, names),

		Series: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: retentionSubsystem,
			Name:      "series_total",
			Help:      "Number of series that a delete was applied to.",
		}, names),
	}
}

// Labels returns a copy of labels for use with retention metrics.
func (m *retentionMetrics) Labels() prometheus.Labels {
	l := make(map[string]string, len(m.labels))
	for k, v := range m.labels {
		l[k] = v
	}
	return l
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (rm *retentionMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		rm.Checks,
		rm.CheckDuration,
		rm.Unprocessable,
		rm.Series,
	}
}
