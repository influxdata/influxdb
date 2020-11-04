package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// namespace is the leading part of all published metrics for the Storage service.
const namespace = "storage"

// StorageMetrics wrap all collectors of storage-related metrics.
type StorageMetrics struct {
	DefaultLabels *prometheus.Labels
	Cache         *CacheMetrics
	Partition     *PartitionMetrics
}

func NewStorageMetrics(defaultLabels *prometheus.Labels) *StorageMetrics {
	labelKeys := make([]string, len(*defaultLabels))
	for k := range *defaultLabels {
		labelKeys = append(labelKeys, k)
	}

	return &StorageMetrics{
		DefaultLabels: defaultLabels,
		Cache:         newCacheMetrics(labelKeys),
		Partition:     newPartitionMetrics(labelKeys),
	}
}

func (m *StorageMetrics) PrometheusCollectors() []prometheus.Collector {
	var metrics []prometheus.Collector
	metrics = append(metrics, m.Cache.PrometheusCollectors()...)
	metrics = append(metrics, m.Partition.PrometheusCollectors()...)
	return metrics
}
