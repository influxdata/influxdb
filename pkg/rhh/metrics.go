package rhh

import (
	"sort"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	LoadFactor         *prometheus.GaugeVec     // Load factor of the hashmap.
	Size               *prometheus.GaugeVec     // Number of items in hashmap.
	GetDuration        *prometheus.HistogramVec // Sample of get times.
	LastGetDuration    *prometheus.GaugeVec     // Sample of most recent get time.
	InsertDuration     *prometheus.HistogramVec // Sample of insertion times.
	LastInsertDuration *prometheus.GaugeVec     // Sample of most recent insertion time.
	LastGrowDuration   *prometheus.GaugeVec     // Most recent growth time.
	MeanProbeCount     *prometheus.GaugeVec     // Average number of probes for each element.

	// These metrics have an extra label status = {"hit", "miss"}
	Gets *prometheus.CounterVec // Number of times item retrieved.
	Puts *prometheus.CounterVec // Number of times item inserted.
}

// NewMetrics initialises prometheus metrics for tracking an RHH hashmap.
func NewMetrics(namespace, subsystem string, labels prometheus.Labels) *Metrics {
	var names []string
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	getPutNames := append(append([]string(nil), names...), "status")
	sort.Strings(getPutNames)

	return &Metrics{
		LoadFactor: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "load_percent",
			Help:      "Load factor of the hashmap.",
		}, names),
		Size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "size",
			Help:      "Number of items in the hashmap.",
		}, names),
		GetDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "get_duration_ns",
			Help:      "Times taken to retrieve elements in nanoseconds (sampled every 10% of retrievals).",
			// 15 buckets spaced exponentially between 100 and ~30,000.
			Buckets: prometheus.ExponentialBuckets(100., 1.5, 15),
		}, names),
		LastGetDuration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "get_duration_last_ns",
			Help:      "Last retrieval duration in nanoseconds (sampled every 10% of retrievals)",
		}, names),
		InsertDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "put_duration_ns",
			Help:      "Times taken to insert elements in nanoseconds (sampled every 10% of insertions).",
			// 15 buckets spaced exponentially between 100 and ~30,000.
			Buckets: prometheus.ExponentialBuckets(100., 1.5, 15),
		}, names),
		LastInsertDuration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "put_duration_last_ns",
			Help:      "Last insertion duration in nanoseconds (sampled every 10% of insertions)",
		}, names),
		LastGrowDuration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "grow_duration_s",
			Help:      "Time in seconds to last grow the hashmap.",
		}, names),
		MeanProbeCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "mean_probes",
			Help:      "Average probe count of all elements (sampled every 0.5% of insertions).",
		}, names),

		Gets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "get_total",
			Help:      "Number of times elements retrieved.",
		}, getPutNames),
		Puts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "put_total",
			Help:      "Number of times elements inserted.",
		}, getPutNames),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *Metrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.LoadFactor,
		m.Size,
		m.GetDuration,
		m.LastGetDuration,
		m.InsertDuration,
		m.LastInsertDuration,
		m.LastGrowDuration,
		m.MeanProbeCount,
		m.Gets,
		m.Puts,
	}
}
