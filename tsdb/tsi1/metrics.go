package tsi1

import (
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// The following package variables act as singletons, to be shared by all
// storage.Engine instantiations. This allows multiple TSI indexes to be
// monitored within the same process.
var (
	cms *cacheMetrics     // TSI index cache metrics
	pms *partitionMetrics // TSI partition metrics
	mmu sync.RWMutex
)

// PrometheusCollectors returns all prometheus metrics for the tsm1 package.
func PrometheusCollectors() []prometheus.Collector {
	mmu.RLock()
	defer mmu.RUnlock()

	var collectors []prometheus.Collector
	if cms != nil {
		collectors = append(collectors, cms.PrometheusCollectors()...)
	}
	if pms != nil {
		collectors = append(collectors, pms.PrometheusCollectors()...)
	}
	return collectors
}

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

const cacheSubsystem = "tsi_cache"     // sub-system associated with TSI index cache.
const partitionSubsystem = "tsi_index" // sub-system associated with the TSI index.

type cacheMetrics struct {
	Size *prometheus.GaugeVec // Size of the cache.

	// These metrics have an extra label status = {"hit", "miss"}
	Gets      *prometheus.CounterVec // Number of times item retrieved.
	Puts      *prometheus.CounterVec // Number of times item inserted.
	Deletes   *prometheus.CounterVec // Number of times item deleted.
	Evictions *prometheus.CounterVec // Number of times item deleted.
}

// newCacheMetrics initialises the prometheus metrics for tracking the Series File.
func newCacheMetrics(labels prometheus.Labels) *cacheMetrics {
	var names []string
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	statusNames := append(append([]string(nil), names...), "status")
	sort.Strings(statusNames)

	return &cacheMetrics{
		Size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "size",
			Help:      "Number of items residing in the cache.",
		}, names),
		Gets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "get_total",
			Help:      "Total number of gets on cache.",
		}, statusNames),
		Puts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "put_total",
			Help:      "Total number of insertions in cache.",
		}, statusNames),
		Deletes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "deletes_total",
			Help:      "Total number of deletions in cache.",
		}, statusNames),
		Evictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "evictions_total",
			Help:      "Total number of cache evictions.",
		}, names),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *cacheMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.Size,
		m.Gets,
		m.Puts,
		m.Deletes,
		m.Evictions,
	}
}

type partitionMetrics struct {
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
func newPartitionMetrics(labels prometheus.Labels) *partitionMetrics {
	names := []string{"index_partition"} // All metrics have a partition
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	// type = {"index", "log"}
	fileNames := append(append([]string(nil), names...), "type")
	sort.Strings(fileNames)

	// level = [0, 7]
	compactionNames := append(append([]string(nil), names...), "level")
	sort.Strings(compactionNames)

	// status = {"ok", "error"}
	attemptedCompactionNames := append(append([]string(nil), compactionNames...), "status")
	sort.Strings(attemptedCompactionNames)

	return &partitionMetrics{
		SeriesCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "series_created",
			Help:      "Number of series created in the partition.",
		}, names),
		SeriesCreatedDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "series_created_duration_ns",
			Help:      "Time taken in nanosecond to create single series.",
			// 30 buckets spaced exponentially between 100ns and ~19 us.
			Buckets: prometheus.ExponentialBuckets(100.0, 1.2, 30),
		}, names),
		SeriesDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "series_dropped",
			Help:      "Number of series dropped from the partition.",
		}, names),
		Series: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "series_total",
			Help:      "Number of series in the partition.",
		}, names),
		Measurements: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: partitionSubsystem,
			Name:      "measurements_total",
			Help:      "Number of series in the partition.",
		}, names),
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
		}, names),
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
func (m *partitionMetrics) PrometheusCollectors() []prometheus.Collector {
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
