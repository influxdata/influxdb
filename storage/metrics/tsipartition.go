package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sort"
)

const partitionSubsystem = "tsi_index" // sub-system associated with the TSI index.

// PartitionMetrics wrap all collectors of metrics from TSI index partitions.
type PartitionMetrics struct {
	SeriesCreated         *prometheus.CounterVec   // Number of series created in Series File.
	SeriesCreatedDuration *prometheus.HistogramVec // Distribution of time to insert series.
	SeriesDropped         *prometheus.CounterVec   // Number of series removed from index.
	Series                *prometheus.GaugeVec     // Number of series.
	Measurements          *prometheus.GaugeVec     // Number of measurements.
	DiskSize              *prometheus.GaugeVec     // Size occupied on disk.

	// This metrics has a "type" = {index, log}
	FilesTotal *prometheus.GaugeVec // files on disk.

	// This metric has a "level" metric.
	CompactionsActive *prometheus.GaugeVec // Number of active compactions.

	// These metrics have a "level" metric.
	// The following metrics include a "status" = {ok, error}` label
	CompactionDuration *prometheus.HistogramVec // Duration of compactions.
	Compactions        *prometheus.CounterVec   // Total number of compactions.
}

// newPartitionMetrics initialises the prometheus metrics for tracking the TSI partitions.
func newPartitionMetrics(labelKeys []string) *PartitionMetrics {
	baseKeys := append(labelKeys, "index_partition") // All metrics have a partition
	sort.Strings(baseKeys)

	// type = {"index", "log"}
	fileNames := append(baseKeys, "type")
	sort.Strings(fileNames)

	// level = [0, 7]
	compactionNames := append(baseKeys, "level")
	sort.Strings(compactionNames)

	// status = {"ok", "error"}
	attemptedCompactionNames := append(baseKeys, "status")
	sort.Strings(attemptedCompactionNames)

	return &PartitionMetrics{
		SeriesCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "series_created",
			Help:      "Number of series created in the partition.",
		}, baseKeys),
		SeriesCreatedDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "series_created_duration_ns",
			Help:      "Time taken in nanosecond to create single series.",
			// 30 buckets spaced exponentially between 100ns and ~19 us.
			Buckets: prometheus.ExponentialBuckets(100.0, 1.2, 30),
		}, baseKeys),
		SeriesDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "series_dropped",
			Help:      "Number of series dropped from the partition.",
		}, baseKeys),
		Series: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "series_total",
			Help:      "Number of series in the partition.",
		}, baseKeys),
		Measurements: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "measurements_total",
			Help:      "Number of series in the partition.",
		}, baseKeys),
		FilesTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "files_total",
			Help:      "Number of files in the partition.",
		}, fileNames),
		DiskSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "disk_bytes",
			Help:      "Number of bytes TSI partition is using on disk.",
		}, baseKeys),
		CompactionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "compactions_active",
			Help:      "Number of active partition compactions.",
		}, compactionNames),
		CompactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "compactions_duration_seconds",
			Help:      "Time taken for a successful compaction of partition.",
			// 30 buckets spaced exponentially between 1s and ~10 minutes.
			Buckets: prometheus.ExponentialBuckets(1.0, 1.25, 30),
		}, compactionNames),
		Compactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "compactions_total",
			Help:      "Number of compactions.",
		}, attemptedCompactionNames),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *PartitionMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.SeriesCreated,
		m.SeriesCreatedDuration,
		m.SeriesDropped,
		m.Series,
		m.Measurements,
		m.FilesTotal,
		m.DiskSize,
		m.CompactionsActive,
		m.CompactionDuration,
		m.Compactions,
	}
}
