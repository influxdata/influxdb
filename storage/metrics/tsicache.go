package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sort"
)

const cacheSubsystem = "tsi_cache"     // sub-system associated with TSI index cache.

// CacheMetrics wrap all collectors of metrics from the TSI index cache.
type CacheMetrics struct {
	Size *prometheus.GaugeVec // Size of the cache.

	// These metrics have an extra label status = {"hit", "miss"}
	Gets      *prometheus.CounterVec // Number of times item retrieved.
	Puts      *prometheus.CounterVec // Number of times item inserted.
	Deletes   *prometheus.CounterVec // Number of times item deleted.
	Evictions *prometheus.CounterVec // Number of times item deleted.
}

// newCacheMetrics initialises the prometheus metrics for tracking the TSI cache.
func newCacheMetrics(labelKeys []string) *CacheMetrics {
	baseKeys := make([]string, len(labelKeys))
	copy(baseKeys, labelKeys)
	sort.Strings(baseKeys)

	keysWithStatus := append(baseKeys, "status")
	sort.Strings(keysWithStatus)

	return &CacheMetrics{
		Size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "size",
			Help:      "Number of items residing in the cache.",
		}, baseKeys),
		Gets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "get_total",
			Help:      "Total number of gets on cache.",
		}, keysWithStatus),
		Puts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "put_total",
			Help:      "Total number of insertions in cache.",
		}, keysWithStatus),
		Deletes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "deletes_total",
			Help:      "Total number of deletions in cache.",
		}, keysWithStatus),
		Evictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: cacheSubsystem,
			Name:      "evictions_total",
			Help:      "Total number of cache evictions.",
		}, baseKeys),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *CacheMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.Size,
		m.Gets,
		m.Puts,
		m.Deletes,
		m.Evictions,
	}
}
