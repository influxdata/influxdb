package storage

import (
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// The following package variables act as singletons, to be shared by all
// storage.Engine instantiations. This allows multiple Engines to be
// monitored within the same process.
var (
	rms *retentionMetrics
	mmu sync.RWMutex
)

// RetentionPrometheusCollectors returns all prometheus metrics for retention.
func RetentionPrometheusCollectors() []prometheus.Collector {
	mmu.RLock()
	defer mmu.RUnlock()

	var collectors []prometheus.Collector
	if rms != nil {
		collectors = append(collectors, rms.PrometheusCollectors()...)
	}
	return collectors
}

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

const retentionSubsystem = "retention" // sub-system associated with metrics for writing points.

// retentionMetrics is a set of metrics concerned with tracking data about retention policies.
type retentionMetrics struct {
	labels        prometheus.Labels
	Checks        *prometheus.CounterVec
	CheckDuration *prometheus.HistogramVec
}

func newRetentionMetrics(labels prometheus.Labels) *retentionMetrics {
	var names []string
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	checksNames := append(append([]string(nil), names...), "status")
	sort.Strings(checksNames)

	checkDurationNames := append(append([]string(nil), names...), "status")
	sort.Strings(checkDurationNames)

	return &retentionMetrics{
		labels: labels,
		Checks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: retentionSubsystem,
			Name:      "checks_total",
			Help:      "Number of retention check operations performed.",
		}, checksNames),

		CheckDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: retentionSubsystem,
			Name:      "check_duration_seconds",
			Help:      "Time taken to perform a successful retention check.",
			// 25 buckets spaced exponentially between 10s and ~2h
			Buckets: prometheus.ExponentialBuckets(10, 1.32, 25),
		}, checkDurationNames),
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
	}
}
