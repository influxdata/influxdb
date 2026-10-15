package tsi1

import (
	"sort"
	"sync"

	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	storageNamespace = "storage"
	cacheSubsystem   = "tsi1_cache"
)

var (
	cacheLabelNames = func() []string {
		names := tsdb.EngineLabelNames()
		sort.Strings(names)
		return names
	}()

	cacheHitsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(storageNamespace, cacheSubsystem, "hits_total"),
		"Total number of TagValueSeriesIDCache hits",
		cacheLabelNames,
		nil,
	)

	cacheMissesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(storageNamespace, cacheSubsystem, "misses_total"),
		"Total number of TagValueSeriesIDCache misses",
		cacheLabelNames,
		nil,
	)

	cacheEvictionsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(storageNamespace, cacheSubsystem, "evictions_total"),
		"Total number of forced evictions in the TagValueSeriesIDCache",
		cacheLabelNames,
		nil,
	)

	cacheShrinkEvictionsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(storageNamespace, cacheSubsystem, "shrink_evictions_total"),
		"Total number of shrink evictions in the TagValueSeriesIDCache",
		cacheLabelNames,
		nil,
	)

	cacheSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(storageNamespace, cacheSubsystem, "size"),
		"Current number of elements in the TagValueSeriesIDCache",
		cacheLabelNames,
		nil,
	)

	cacheCapacityDesc = prometheus.NewDesc(
		prometheus.BuildFQName(storageNamespace, cacheSubsystem, "capacity"),
		"Current capacity of the TagValueSeriesIDCache",
		cacheLabelNames,
		nil,
	)
)

var globalCacheRegistry = &cacheRegistry{
	caches: make(map[*TagValueSeriesIDCache]prometheus.Labels),
}

type cacheRegistry struct {
	mu     sync.RWMutex
	caches map[*TagValueSeriesIDCache]prometheus.Labels
}

func (r *cacheRegistry) register(c *TagValueSeriesIDCache, tags tsdb.EngineTags) {
	if c == nil {
		return
	}

	labels := tags.GetLabels()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.caches[c] = labels
}

func (r *cacheRegistry) deregister(c *TagValueSeriesIDCache) {
	if c == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.caches, c)
}

func (r *cacheRegistry) Describe(ch chan<- *prometheus.Desc) {
	ch <- cacheHitsDesc
	ch <- cacheMissesDesc
	ch <- cacheEvictionsDesc
	ch <- cacheShrinkEvictionsDesc
	ch <- cacheSizeDesc
	ch <- cacheCapacityDesc
}

func (r *cacheRegistry) Collect(ch chan<- prometheus.Metric) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for c, labels := range r.caches {
		labelValues := make([]string, 0, len(cacheLabelNames))

		for _, name := range cacheLabelNames {
			labelValues = append(labelValues, labels[name])
		}

		ch <- prometheus.MustNewConstMetric(
			cacheHitsDesc,
			prometheus.CounterValue,
			float64(c.stats.Hits.Load()),
			labelValues...,
		)

		ch <- prometheus.MustNewConstMetric(
			cacheMissesDesc,
			prometheus.CounterValue,
			float64(c.stats.Misses.Load()),
			labelValues...,
		)

		ch <- prometheus.MustNewConstMetric(
			cacheEvictionsDesc,
			prometheus.CounterValue,
			float64(c.stats.Evictions.Load()),
			labelValues...,
		)

		ch <- prometheus.MustNewConstMetric(
			cacheShrinkEvictionsDesc,
			prometheus.CounterValue,
			float64(c.stats.ShrinkEvictions.Load()),
			labelValues...,
		)

		ch <- prometheus.MustNewConstMetric(
			cacheSizeDesc,
			prometheus.GaugeValue,
			float64(c.stats.Size.Load()),
			labelValues...,
		)

		ch <- prometheus.MustNewConstMetric(
			cacheCapacityDesc,
			prometheus.GaugeValue,
			float64(c.capacity.Load()),
			labelValues...,
		)
	}
}

func PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{globalCacheRegistry}
}
