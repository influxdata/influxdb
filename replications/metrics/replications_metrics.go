package metrics

import (
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/prometheus/client_golang/prometheus"
)

type ReplicationsMetrics struct {
	TotalPointsQueued  *prometheus.CounterVec
	TotalBytesQueued   *prometheus.CounterVec
	CurrentBytesQueued *prometheus.GaugeVec
}

func NewReplicationsMetrics() *ReplicationsMetrics {
	const namespace = "replications"
	const subsystem = "queue"

	return &ReplicationsMetrics{
		TotalPointsQueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_points_queued",
			Help:      "Sum of all points that have been added to the replication stream queue",
		}, []string{"replicationID"}),
		TotalBytesQueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_bytes_queued",
			Help:      "Sum of all bytes that have been added to the replication stream queue",
		}, []string{"replicationID"}),
		CurrentBytesQueued: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "current_bytes_queued",
			Help:      "Current number of bytes in the replication stream queue",
		}, []string{"replicationID"}),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (rm *ReplicationsMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		rm.TotalPointsQueued,
		rm.TotalBytesQueued,
		rm.CurrentBytesQueued,
	}
}

// EnqueueData updates the metrics when adding new data to a replication queue.
func (rm *ReplicationsMetrics) EnqueueData(replicationID platform.ID, numBytes, numPoints int, queueSizeOnDisk int64) {
	rm.TotalPointsQueued.WithLabelValues(replicationID.String()).Add(float64(numPoints))
	rm.TotalBytesQueued.WithLabelValues(replicationID.String()).Add(float64(numBytes))
	rm.CurrentBytesQueued.WithLabelValues(replicationID.String()).Set(float64(queueSizeOnDisk))
}

// Dequeue updates the metrics when data has been removed from the queue.
func (rm *ReplicationsMetrics) Dequeue(replicationID platform.ID, queueSizeOnDisk int64) {
	rm.CurrentBytesQueued.WithLabelValues(replicationID.String()).Set(float64(queueSizeOnDisk))
}
