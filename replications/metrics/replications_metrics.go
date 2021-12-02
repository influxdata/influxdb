package metrics

import (
	"strconv"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/prometheus/client_golang/prometheus"
)

type ReplicationsMetrics struct {
	TotalPointsQueued       *prometheus.CounterVec
	TotalBytesQueued        *prometheus.CounterVec
	CurrentBytesQueued      *prometheus.GaugeVec
	RemoteWriteErrors       *prometheus.CounterVec
	RemoteWriteBytesSent    *prometheus.CounterVec
	RemoteWriteBytesDropped *prometheus.CounterVec
	PointsFailedToQueue     *prometheus.CounterVec
	BytesFailedToQueue      *prometheus.CounterVec
}

func NewReplicationsMetrics() *ReplicationsMetrics {
	const namespace = "replications"
	const subsystem = "queue"

	return &ReplicationsMetrics{
		TotalPointsQueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_points_queued",
			Help:      "Sum of all points that have been successfully added to the replication stream queue",
		}, []string{"replicationID"}),
		TotalBytesQueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_bytes_queued",
			Help:      "Sum of all bytes that have been successfully added to the replication stream queue",
		}, []string{"replicationID"}),
		CurrentBytesQueued: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "current_bytes_queued",
			Help:      "Current number of bytes in the replication stream queue remaining to be processed",
		}, []string{"replicationID"}),
		RemoteWriteErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "remote_write_errors",
			Help:      "Error codes returned from attempted remote writes",
		}, []string{"replicationID", "code"}),
		RemoteWriteBytesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "remote_write_bytes_sent",
			Help:      "Bytes of data successfully sent to the remote by the replication stream",
		}, []string{"replicationID"}),
		RemoteWriteBytesDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "remote_write_bytes_dropped",
			Help:      "Bytes of data dropped due to remote write failures",
		}, []string{"replicationID"}),
		PointsFailedToQueue: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "points_failed_to_queue",
			Help:      "Sum of all points that could not be added to the local replication queue",
		}, []string{"replicationID"}),
		BytesFailedToQueue: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "bytes_failed_to_queue",
			Help:      "Sum of all bytes that could not be added to the local replication queue",
		}, []string{"replicationID"}),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (rm *ReplicationsMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		rm.TotalPointsQueued,
		rm.TotalBytesQueued,
		rm.CurrentBytesQueued,
		rm.RemoteWriteErrors,
		rm.RemoteWriteBytesSent,
		rm.RemoteWriteBytesDropped,
		rm.PointsFailedToQueue,
		rm.BytesFailedToQueue,
	}
}

// EnqueueData updates the metrics when adding new data to a replication queue.
func (rm *ReplicationsMetrics) EnqueueData(replicationID platform.ID, numBytes, numPoints int, queueSize int64) {
	rm.TotalPointsQueued.WithLabelValues(replicationID.String()).Add(float64(numPoints))
	rm.TotalBytesQueued.WithLabelValues(replicationID.String()).Add(float64(numBytes))
	rm.CurrentBytesQueued.WithLabelValues(replicationID.String()).Set(float64(queueSize))
}

// Dequeue updates the metrics when data has been removed from the queue.
func (rm *ReplicationsMetrics) Dequeue(replicationID platform.ID, queueSize int64) {
	rm.CurrentBytesQueued.WithLabelValues(replicationID.String()).Set(float64(queueSize))
}

// EnqueueError updates the metrics when data fails to be added to the replication queue.
func (rm *ReplicationsMetrics) EnqueueError(replicationID platform.ID, numBytes, numPoints int) {
	rm.PointsFailedToQueue.WithLabelValues(replicationID.String()).Add(float64(numPoints))
	rm.BytesFailedToQueue.WithLabelValues(replicationID.String()).Add(float64(numBytes))
}

// RemoteWriteError increments the error code counter for the replication.
func (rm *ReplicationsMetrics) RemoteWriteError(replicationID platform.ID, errorCode int) {
	rm.RemoteWriteErrors.WithLabelValues(replicationID.String(), strconv.Itoa(errorCode)).Inc()
}

// RemoteWriteSent increases the total count of bytes sent following a successful remote write
func (rm *ReplicationsMetrics) RemoteWriteSent(replicationID platform.ID, bytes int) {
	rm.RemoteWriteBytesSent.WithLabelValues(replicationID.String()).Add(float64(bytes))
}

// RemoteWriteDropped increases the total count of bytes dropped when data is dropped
func (rm *ReplicationsMetrics) RemoteWriteDropped(replicationID platform.ID, bytes int) {
	rm.RemoteWriteBytesDropped.WithLabelValues(replicationID.String()).Add(float64(bytes))
}
