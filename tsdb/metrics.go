package tsdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"sort"
)

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

// Metrics wrap all collectors of storage-related metrics.
type Metrics struct {
	GlobalLabels *prometheus.Labels
	TSICache     *TSICacheMetrics
	TSIPartition *TSIPartitionMetrics
}

func NewMetrics(globalLabels *prometheus.Labels) *Metrics {
	labelKeys := make([]string, len(*globalLabels))
	for k := range *globalLabels {
		labelKeys = append(labelKeys, k)
	}

	return &Metrics{
		GlobalLabels: globalLabels,
		TSICache:     newTSICacheMetrics(labelKeys),
		TSIPartition: newTsiPartitionMetrics(labelKeys),
	}
}

func (m *Metrics) PrometheusCollectors() []prometheus.Collector {
	var metrics []prometheus.Collector
	metrics = append(metrics, m.TSICache.PrometheusCollectors()...)
	metrics = append(metrics, m.TSIPartition.PrometheusCollectors()...)
	return metrics
}

const tsiCacheSubsystem = "tsi_cache" // sub-system associated with TSI index cache.

// TSICacheMetrics wrap all collectors of metrics from the TSI index cache.
type TSICacheMetrics struct {
	Size *prometheus.GaugeVec // Size of the cache.

	// These metrics have an extra label status = {"hit", "miss"}
	Gets      *prometheus.CounterVec // Number of times item retrieved.
	Puts      *prometheus.CounterVec // Number of times item inserted.
	Deletes   *prometheus.CounterVec // Number of times item deleted.
	Evictions *prometheus.CounterVec // Number of times item deleted.
}

// newTSICacheMetrics initialises the prometheus metrics for tracking the TSI cache.
func newTSICacheMetrics(globalLabelKeys []string) *TSICacheMetrics {
	baseKeys := make([]string, len(globalLabelKeys))
	copy(baseKeys, globalLabelKeys)
	sort.Strings(baseKeys)

	keysWithStatus := append(baseKeys, "status")
	sort.Strings(keysWithStatus)

	return &TSICacheMetrics{
		Size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: tsiCacheSubsystem,
			Name:      "size",
			Help:      "Number of items residing in the cache.",
		}, baseKeys),
		Gets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tsiCacheSubsystem,
			Name:      "get_total",
			Help:      "Total number of gets on cache.",
		}, keysWithStatus),
		Puts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tsiCacheSubsystem,
			Name:      "put_total",
			Help:      "Total number of insertions in cache.",
		}, keysWithStatus),
		Deletes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tsiCacheSubsystem,
			Name:      "deletes_total",
			Help:      "Total number of deletions in cache.",
		}, keysWithStatus),
		Evictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tsiCacheSubsystem,
			Name:      "evictions_total",
			Help:      "Total number of cache evictions.",
		}, baseKeys),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *TSICacheMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.Size,
		m.Gets,
		m.Puts,
		m.Deletes,
		m.Evictions,
	}
}

const tsiPartitionSubsystem = "tsi_index" // sub-system associated with the TSI index.

// TSIPartitionMetrics wrap all collectors of metrics from TSI index partitions.
type TSIPartitionMetrics struct {
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

// newTsiPartitionMetrics initialises the prometheus metrics for tracking the TSI partitions.
func newTsiPartitionMetrics(globalLabelKeys []string) *TSIPartitionMetrics {
	baseKeys := append(globalLabelKeys, "index_partition") // All metrics have a partition
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

	return &TSIPartitionMetrics{
		SeriesCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "series_created",
			Help:      "Number of series created in the partition.",
		}, baseKeys),
		SeriesCreatedDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "series_created_duration_ns",
			Help:      "Time taken in nanosecond to create single series.",
			// 30 buckets spaced exponentially between 100ns and ~19 us.
			Buckets: prometheus.ExponentialBuckets(100.0, 1.2, 30),
		}, baseKeys),
		SeriesDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "series_dropped",
			Help:      "Number of series dropped from the partition.",
		}, baseKeys),
		Series: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "series_total",
			Help:      "Number of series in the partition.",
		}, baseKeys),
		Measurements: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "measurements_total",
			Help:      "Number of series in the partition.",
		}, baseKeys),
		FilesTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "files_total",
			Help:      "Number of files in the partition.",
		}, fileNames),
		DiskSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "disk_bytes",
			Help:      "Number of bytes TSI partition is using on disk.",
		}, baseKeys),
		CompactionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "compactions_active",
			Help:      "Number of active partition compactions.",
		}, compactionNames),
		CompactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "compactions_duration_seconds",
			Help:      "Time taken for a successful compaction of partition.",
			// 30 buckets spaced exponentially between 1s and ~10 minutes.
			Buckets: prometheus.ExponentialBuckets(1.0, 1.25, 30),
		}, compactionNames),
		Compactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tsiPartitionSubsystem,
			Name:      "compactions_total",
			Help:      "Number of compactions.",
		}, attemptedCompactionNames),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *TSIPartitionMetrics) PrometheusCollectors() []prometheus.Collector {
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
