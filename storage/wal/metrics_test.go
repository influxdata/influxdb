package wal

import (
	"testing"

	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_WAL(t *testing.T) {
	// metrics to be shared by multiple file stores.
	metrics := newWALMetrics(prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newWALTracker(metrics, prometheus.Labels{"engine_id": "0", "node_id": "0"})
	t2 := newWALTracker(metrics, prometheus.Labels{"engine_id": "1", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	base := namespace + "_" + walSubsystem + "_"

	// All the metric names
	gauges := []string{
		base + "old_segment_bytes",
		base + "current_segment_bytes",
		base + "segments_total",
	}

	counters := []string{
		base + "writes_total",
	}

	// Generate some measurements.
	for i, tracker := range []*walTracker{t1, t2} {
		tracker.SetOldSegmentSize(uint64(i + len(gauges[0])))
		tracker.SetCurrentSegmentSize(uint64(i + len(gauges[1])))
		tracker.SetSegments(uint64(i + len(gauges[2])))

		labels := tracker.Labels()
		labels["status"] = "ok"
		tracker.metrics.Writes.With(labels).Add(float64(i + len(counters[0])))
	}

	// Test that all the correct metrics are present.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	// The label variants for the two caches.
	labelVariants := []prometheus.Labels{
		prometheus.Labels{"engine_id": "0", "node_id": "0"},
		prometheus.Labels{"engine_id": "1", "node_id": "0"},
	}

	for i, labels := range labelVariants {
		for _, name := range gauges {
			exp := float64(i + len(name))
			metric := promtest.MustFindMetric(t, mfs, name, labels)
			if got := metric.GetGauge().GetValue(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}

		for _, name := range counters {
			exp := float64(i + len(name))

			labels["status"] = "ok"
			metric := promtest.MustFindMetric(t, mfs, name, labels)
			if got := metric.GetCounter().GetValue(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}
	}
}
