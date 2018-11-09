package rhh

import (
	"sort"

	"github.com/prometheus/client_golang/prometheus"
)

type rhhMetrics struct {
	labels prometheus.Labels
}

// newRHHMetrics initialises prometheus metrics for tracking an RHH hashmap.
func newRHHMetrics(namespace, subsystem string, labels prometheus.Labels) *rhhMetrics {
	var names []string
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)

	return &rhhMetrics{
		labels: labels,
	}
}

// Labels returns a copy of labels for use with RHH metrics.
func (m *rhhMetrics) Labels() prometheus.Labels {
	l := make(map[string]string, len(m.labels))
	for k, v := range m.labels {
		l[k] = v
	}
	return l
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (m *rhhMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{}
}
