package tsm1

import (
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
)

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

const compactionSubsystem = "compactions" // sub-system associated with metrics for compactions
const fileStoreSubsystem = "tsm_files"    // sub-system associated with metrics for compactions

// blockMetrics are a set of metrics concerned with tracking data about block storage.
type blockMetrics struct {
	labels prometheus.Labels
	*compactionMetrics
	*fileMetrics
}

// newBlockMetrics initialises the prometheus metrics for the block subsystem.
func newBlockMetrics(labels prometheus.Labels) *blockMetrics {
	return &blockMetrics{
		labels:            labels,
		compactionMetrics: newCompactionMetrics(labels),
		fileMetrics:       newFileMetrics(labels),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *blockMetrics) PrometheusCollectors() []prometheus.Collector {
	var metrics []prometheus.Collector
	metrics = append(metrics, m.compactionMetrics.PrometheusCollectors()...)
	metrics = append(metrics, m.fileMetrics.PrometheusCollectors()...)
	return metrics
}

// compactionMetrics are a set of metrics concerned with tracking data about compactions.
type compactionMetrics struct {
	labels             prometheus.Labels // Read Only
	Compactions        *prometheus.CounterVec
	CompactionsActive  *prometheus.GaugeVec
	CompactionDuration *prometheus.HistogramVec
	CompactionQueue    *prometheus.GaugeVec
}

// newCompactionMetrics initialises the prometheus metrics for compactions.
func newCompactionMetrics(labels prometheus.Labels) *compactionMetrics {
	compactionNames := []string{"level"} // All compaction metrics have a `level` label.
	for k := range labels {
		compactionNames = append(compactionNames, k)
	}
	sort.Strings(compactionNames)
	totalCompactionsNames := append(compactionNames, "status")
	sort.Strings(totalCompactionsNames)

	return &compactionMetrics{
		labels: labels,
		Compactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: compactionSubsystem,
			Name:      "total",
			Help:      "Number of times cache snapshotted or TSM compaction attempted.",
		}, totalCompactionsNames),
		CompactionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: compactionSubsystem,
			Name:      "active",
			Help:      "Number of active compactions.",
		}, compactionNames),
		CompactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: compactionSubsystem,
			Name:      "duration_seconds",
			Help:      "Time taken for a successful compaction or snapshot.",
			// 30 buckets spaced exponentially between 5s and ~53 minutes.
			Buckets: prometheus.ExponentialBuckets(5.0, 1.25, 30),
		}, compactionNames),
		CompactionQueue: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: compactionSubsystem,
			Name:      "queued",
			Help:      "Number of queued compactions.",
		}, compactionNames),
	}
}

// Labels returns a copy of labels for use with compaction metrics.
func (m *compactionMetrics) Labels(level compactionLevel) prometheus.Labels {
	l := make(map[string]string, len(m.labels))
	for k, v := range m.labels {
		l[k] = v
	}
	l["level"] = fmt.Sprint(level)
	return l
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *compactionMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.Compactions,
		m.CompactionsActive,
		m.CompactionDuration,
		m.CompactionQueue,
	}
}

// fileMetrics are a set of metrics concerned with tracking data about compactions.
type fileMetrics struct {
	labels   prometheus.Labels
	DiskSize *prometheus.GaugeVec
	Files    *prometheus.GaugeVec
}

// newFileMetrics initialises the prometheus metrics for tracking files on disk.
func newFileMetrics(labels prometheus.Labels) *fileMetrics {
	var names []string
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	return &fileMetrics{
		labels: labels,
		DiskSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: fileStoreSubsystem,
			Name:      "disk_bytes",
			Help:      "Number of bytes TSM files using on disk.",
		}, names),
		Files: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: fileStoreSubsystem,
			Name:      "total",
			Help:      "Number of files.",
		}, names),
	}
}

// Labels returns a copy of labels for use with file metrics.
func (m *fileMetrics) Labels() prometheus.Labels {
	l := make(map[string]string, len(m.labels))
	for k, v := range m.labels {
		l[k] = v
	}
	return l
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *fileMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.DiskSize,
		m.Files,
	}
}
