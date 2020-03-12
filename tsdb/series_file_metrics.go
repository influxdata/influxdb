package tsdb

import (
	"sort"
	"sync"

	"github.com/influxdata/influxdb/pkg/rhh"
	"github.com/prometheus/client_golang/prometheus"
)

// The following package variables act as singletons, to be shared by all
// storage.Engine instantiations. This allows multiple Series Files to be
// monitored within the same process.
var (
	sms *seriesFileMetrics // main metrics
	ims *rhh.Metrics       // hashmap specific metrics
	mmu sync.RWMutex
)

// PrometheusCollectors returns all the metrics associated with the tsdb package.
func PrometheusCollectors() []prometheus.Collector {
	mmu.RLock()
	defer mmu.RUnlock()

	var collectors []prometheus.Collector
	if sms != nil {
		collectors = append(collectors, sms.PrometheusCollectors()...)
	}

	if ims != nil {
		collectors = append(collectors, ims.PrometheusCollectors()...)
	}
	return collectors
}

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

const seriesFileSubsystem = "series_file" // sub-system associated with metrics for the Series File.

type seriesFileMetrics struct {
	SeriesCreated *prometheus.CounterVec // Number of series created in Series File.
	Series        *prometheus.GaugeVec   // Number of series.
	DiskSize      *prometheus.GaugeVec   // Size occupied on disk.
	Segments      *prometheus.GaugeVec   // Number of segment files.

	CompactionsActive  *prometheus.GaugeVec     // Number of active compactions.
	CompactionDuration *prometheus.HistogramVec // Duration of compactions.
	// The following metrics include a ``"status" = {ok, error}` label
	Compactions *prometheus.CounterVec // Total number of compactions.
}

// newSeriesFileMetrics initialises the prometheus metrics for tracking the Series File.
func newSeriesFileMetrics(labels prometheus.Labels) *seriesFileMetrics {
	names := []string{"series_file_partition"} // All metrics have this label.
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	totalCompactions := append(append([]string(nil), names...), "status")
	sort.Strings(totalCompactions)

	durationCompaction := append(append([]string(nil), names...), "component")
	sort.Strings(durationCompaction)

	return &seriesFileMetrics{
		SeriesCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: seriesFileSubsystem,
			Name:      "series_created",
			Help:      "Number of series created in Series File.",
		}, names),
		Series: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: seriesFileSubsystem,
			Name:      "series_total",
			Help:      "Number of series in Series File.",
		}, names),
		DiskSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: seriesFileSubsystem,
			Name:      "disk_bytes",
			Help:      "Number of bytes Series File is using on disk.",
		}, names),
		Segments: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: seriesFileSubsystem,
			Name:      "segments_total",
			Help:      "Number of segment files in Series File.",
		}, names),
		CompactionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: seriesFileSubsystem,
			Name:      "index_compactions_active",
			Help:      "Number of active index compactions.",
		}, durationCompaction),
		CompactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: seriesFileSubsystem,
			Name:      "index_compactions_duration_seconds",
			Help:      "Time taken for a successful compaction of index.",
			// 30 buckets spaced exponentially between 5s and ~53 minutes.
			Buckets: prometheus.ExponentialBuckets(5.0, 1.25, 30),
		}, durationCompaction),
		Compactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: seriesFileSubsystem,
			Name:      "compactions_total",
			Help:      "Number of compactions.",
		}, totalCompactions),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *seriesFileMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.SeriesCreated,
		m.Series,
		m.DiskSize,
		m.Segments,
		m.CompactionsActive,
		m.CompactionDuration,
		m.Compactions,
	}
}
