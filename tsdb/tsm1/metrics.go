package tsm1

import (
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
)

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

const blockSubsystem = "block" // sub-system associated with metrics for block storage.

// blockMetrics are a set of metrics concerned with tracking data about block storage.
type blockMetrics struct {
	labels prometheus.Labels // Read only.

	Compactions        *prometheus.CounterVec
	CompactionsActive  *prometheus.GaugeVec
	CompactionDuration *prometheus.HistogramVec
	CompactionQueue    *prometheus.GaugeVec
}

// newBlockMetrics initialises the prometheus metrics for the block subsystem.
func newBlockMetrics(labels prometheus.Labels) *blockMetrics {
	compactionNames := []string{"level"} // All compaction metrics have a `level` label.
	for k := range labels {
		compactionNames = append(compactionNames, k)
	}
	sort.Strings(compactionNames)
	totalCompactionsNames := append(compactionNames, "status")
	sort.Strings(totalCompactionsNames)

	return &blockMetrics{
		labels: labels,
		Compactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: blockSubsystem,
			Name:      "compactions_total",
			Help:      "Number of times cache snapshotted or TSM compaction attempted.",
		}, totalCompactionsNames),
		CompactionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: blockSubsystem,
			Name:      "compactions_active",
			Help:      "Number of active compactions.",
		}, compactionNames),
		CompactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: blockSubsystem,
			Name:      "compaction_duration_seconds",
			Help:      "Time taken for a successful compaction or snapshot.",
			// 30 buckets spaced exponentially between 5s and ~53 minutes.
			Buckets: prometheus.ExponentialBuckets(5.0, 1.25, 30),
		}, compactionNames),
		CompactionQueue: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: blockSubsystem,
			Name:      "compactions_queued",
			Help:      "Number of queued compactions.",
		}, compactionNames),
	}
}

// CompactionLabels returns a copy of labels for use with compaction metrics.
func (b *blockMetrics) CompactionLabels(level compactionLevel) prometheus.Labels {
	l := make(map[string]string, len(b.labels))
	for k, v := range b.labels {
		l[k] = v
	}
	l["level"] = fmt.Sprint(level)
	return l
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (b *blockMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		b.Compactions,
		b.CompactionsActive,
		b.CompactionDuration,
		b.CompactionQueue,
	}
}
