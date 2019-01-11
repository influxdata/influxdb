package rhh

import (
	"testing"

	"github.com/influxdata/influxdb/kit/prom/promtest"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_Metrics(t *testing.T) {
	// metrics to be shared by multiple file stores.
	metrics := NewMetrics("test", "sub", prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newRHHTracker(metrics, prometheus.Labels{"engine_id": "0", "node_id": "0"})
	t2 := newRHHTracker(metrics, prometheus.Labels{"engine_id": "1", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	base := "test_sub_"

	// All the metric names
	gauges := []string{
		base + "load_percent",
		base + "size",
		base + "get_duration_last_ns",
		base + "put_duration_last_ns",
		base + "grow_duration_s",
		base + "mean_probes",
	}

	counters := []string{
		base + "get_total",
		base + "put_total",
	}

	histograms := []string{
		base + "get_duration_ns",
		base + "put_duration_ns",
	}

	// Generate some measurements.
	for i, tracker := range []*rhhTracker{t1, t2} {
		tracker.SetLoadFactor(float64(i + len(gauges[0])))
		tracker.SetSize(uint64(i + len(gauges[1])))

		labels := tracker.Labels()
		tracker.metrics.LastGetDuration.With(labels).Set(float64(i + len(gauges[2])))
		tracker.metrics.LastInsertDuration.With(labels).Set(float64(i + len(gauges[3])))
		tracker.metrics.LastGrowDuration.With(labels).Set(float64(i + len(gauges[4])))
		tracker.SetProbeCount(float64(i + len(gauges[5])))

		labels = tracker.Labels()
		labels["status"] = "ok"
		tracker.metrics.Gets.With(labels).Add(float64(i + len(counters[0])))
		tracker.metrics.Puts.With(labels).Add(float64(i + len(counters[1])))

		labels = tracker.Labels()
		tracker.metrics.GetDuration.With(labels).Observe(float64(i + len(histograms[0])))
		tracker.metrics.InsertDuration.With(labels).Observe(float64(i + len(histograms[1])))
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

			// Make a copy since we need to add a label
			l := make(prometheus.Labels, len(labels))
			for k, v := range labels {
				l[k] = v
			}
			l["status"] = "ok"

			metric := promtest.MustFindMetric(t, mfs, name, l)
			if got := metric.GetCounter().GetValue(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}

		for _, name := range histograms {
			exp := float64(i + len(name))
			metric := promtest.MustFindMetric(t, mfs, name, labels)
			if got := metric.GetHistogram().GetSampleSum(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}
	}
}
