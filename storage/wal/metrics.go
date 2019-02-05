package wal

import (
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// The following package variables act as singletons, to be shared by all
// storage.Engine instantiations. This allows multiple WALs to be monitored
// within the same process.
var (
	wms *walMetrics // main metrics
	mmu sync.RWMutex
)

// PrometheusCollectors returns all the metrics associated with the tsdb package.
func PrometheusCollectors() []prometheus.Collector {
	mmu.RLock()
	defer mmu.RUnlock()

	var collectors []prometheus.Collector
	if wms != nil {
		collectors = append(collectors, wms.PrometheusCollectors()...)
	}

	return collectors
}

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

const walSubsystem = "wal" // sub-system associated with metrics for the WAL.

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
