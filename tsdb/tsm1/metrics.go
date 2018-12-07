package tsm1

import (
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// The following package variables act as singletons, to be shared by all Engine
// instantiations. This allows multiple Engines to be instantiated within the
// same process.
var (
	bms *blockMetrics
	mmu sync.RWMutex
)

// PrometheusCollectors returns all prometheus metrics for the tsm1 package.
func PrometheusCollectors() []prometheus.Collector {
	mmu.RLock()
	defer mmu.RUnlock()

	var collectors []prometheus.Collector
	if bms != nil {
		collectors = append(collectors, bms.compactionMetrics.PrometheusCollectors()...)
		collectors = append(collectors, bms.fileMetrics.PrometheusCollectors()...)
		collectors = append(collectors, bms.cacheMetrics.PrometheusCollectors()...)
		collectors = append(collectors, bms.walMetrics.PrometheusCollectors()...)
	}
	return collectors
}

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

const compactionSubsystem = "compactions" // sub-system associated with metrics for compactions.
const fileStoreSubsystem = "tsm_files"    // sub-system associated with metrics for TSM files.
const cacheSubsystem = "cache"            // sub-system associated with metrics for the cache.
const walSubsystem = "wal"                // sub-system associated with metrics for the WAL.

// blockMetrics are a set of metrics concerned with tracking data about block storage.
type blockMetrics struct {
	labels prometheus.Labels
	*compactionMetrics
	*fileMetrics
	*cacheMetrics
	*walMetrics
}

// newBlockMetrics initialises the prometheus metrics for the block subsystem.
func newBlockMetrics(labels prometheus.Labels) *blockMetrics {
	return &blockMetrics{
		labels:            labels,
		compactionMetrics: newCompactionMetrics(labels),
		fileMetrics:       newFileMetrics(labels),
		cacheMetrics:      newCacheMetrics(labels),
		walMetrics:        newWALMetrics(labels),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *blockMetrics) PrometheusCollectors() []prometheus.Collector {
	var metrics []prometheus.Collector
	metrics = append(metrics, m.compactionMetrics.PrometheusCollectors()...)
	metrics = append(metrics, m.fileMetrics.PrometheusCollectors()...)
	metrics = append(metrics, m.cacheMetrics.PrometheusCollectors()...)
	metrics = append(metrics, m.walMetrics.PrometheusCollectors()...)
	return metrics
}

// compactionMetrics are a set of metrics concerned with tracking data about compactions.
type compactionMetrics struct {
	CompactionsActive  *prometheus.GaugeVec
	CompactionDuration *prometheus.HistogramVec
	CompactionQueue    *prometheus.GaugeVec

	// The following metrics include a ``"status" = {ok, error}` label
	Compactions *prometheus.CounterVec
}

// newCompactionMetrics initialises the prometheus metrics for compactions.
func newCompactionMetrics(labels prometheus.Labels) *compactionMetrics {
	names := []string{"level"} // All compaction metrics have a `level` label.
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	totalCompactionsNames := append(append([]string(nil), names...), "status")
	sort.Strings(totalCompactionsNames)

	return &compactionMetrics{
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
		}, names),
		CompactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: compactionSubsystem,
			Name:      "duration_seconds",
			Help:      "Time taken for a successful compaction or snapshot.",
			// 30 buckets spaced exponentially between 5s and ~53 minutes.
			Buckets: prometheus.ExponentialBuckets(5.0, 1.25, 30),
		}, names),
		CompactionQueue: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: compactionSubsystem,
			Name:      "queued",
			Help:      "Number of queued compactions.",
		}, names),
	}
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

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *fileMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.DiskSize,
		m.Files,
	}
}

// cacheMetrics are a set of metrics concerned with tracking data about the TSM Cache.
type cacheMetrics struct {
	MemSize          *prometheus.GaugeVec
	DiskSize         *prometheus.GaugeVec
	SnapshotsActive  *prometheus.GaugeVec
	Age              *prometheus.GaugeVec
	SnapshottedBytes *prometheus.CounterVec

	// The following metrics include a ``"status" = {ok, error, dropped}` label
	WrittenBytes *prometheus.CounterVec
	Writes       *prometheus.CounterVec
}

// newCacheMetrics initialises the prometheus metrics for compactions.
func newCacheMetrics(labels prometheus.Labels) *cacheMetrics {
	var names []string
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	writeNames := append(append([]string(nil), names...), "status")
	sort.Strings(writeNames)

	return &cacheMetrics{
		MemSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "inuse_bytes",
			Help:      "In-memory size of cache.",
		}, names),
		DiskSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "disk_bytes",
			Help:      "Number of bytes on disk used by snapshot data.",
		}, names),
		SnapshotsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "snapshots_active",
			Help:      "Number of active concurrent snapshots (>1 when splitting the cache).",
		}, names),
		Age: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "age_seconds",
			Help:      "Age in seconds of the current cache (time since last snapshot or initialisation).",
		}, names),
		SnapshottedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "snapshot_bytes",
			Help:      "Number of bytes snapshotted.",
		}, names),
		WrittenBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "written_bytes",
			Help:      "Number of bytes successfully written to the Cache.",
		}, writeNames),
		Writes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "writes_total",
			Help:      "Number of writes to the Cache.",
		}, writeNames),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *cacheMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.MemSize,
		m.DiskSize,
		m.SnapshotsActive,
		m.Age,
		m.SnapshottedBytes,
		m.WrittenBytes,
		m.Writes,
	}
}

// walMetrics are a set of metrics concerned with tracking data about compactions.
type walMetrics struct {
	OldSegmentBytes     *prometheus.GaugeVec
	CurrentSegmentBytes *prometheus.GaugeVec
	Segments            *prometheus.GaugeVec
	Writes              *prometheus.CounterVec
}

// newWALMetrics initialises the prometheus metrics for tracking the WAL.
func newWALMetrics(labels prometheus.Labels) *walMetrics {
	var names []string
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	writeNames := append(append([]string(nil), names...), "status")
	sort.Strings(writeNames)

	return &walMetrics{
		OldSegmentBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: walSubsystem,
			Name:      "old_segment_bytes",
			Help:      "Number of bytes old WAL segments using on disk.",
		}, names),
		CurrentSegmentBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: walSubsystem,
			Name:      "current_segment_bytes",
			Help:      "Number of bytes TSM files using on disk.",
		}, names),
		Segments: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: walSubsystem,
			Name:      "segments_total",
			Help:      "Number of WAL segment files on disk.",
		}, names),
		Writes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: walSubsystem,
			Name:      "writes_total",
			Help:      "Number of writes to the WAL.",
		}, writeNames),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *walMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.OldSegmentBytes,
		m.CurrentSegmentBytes,
		m.Segments,
		m.Writes,
	}
}
