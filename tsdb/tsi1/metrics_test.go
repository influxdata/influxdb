package tsi1

import (
	"testing"

	"github.com/influxdata/influxdb/kit/prom/promtest"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMetrics_Cache(t *testing.T) {
	// metrics to be shared by multiple file stores.
	metrics := newCacheMetrics(prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newCacheTracker(metrics, prometheus.Labels{"engine_id": "0", "node_id": "0"})
	t2 := newCacheTracker(metrics, prometheus.Labels{"engine_id": "1", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	base := namespace + "_" + cacheSubsystem + "_"

	// All the metric names
	gauges := []string{base + "size"}

	counters := []string{
		base + "get_total",
		base + "put_total",
		base + "deletes_total",
		base + "evictions_total",
	}

	// Generate some measurements.
	for i, tracker := range []*cacheTracker{t1, t2} {
		tracker.SetSize(uint64(i + len(gauges[0])))

		labels := tracker.Labels()
		labels["status"] = "hit"
		tracker.metrics.Gets.With(labels).Add(float64(i + len(counters[0])))
		tracker.metrics.Puts.With(labels).Add(float64(i + len(counters[1])))
		tracker.metrics.Deletes.With(labels).Add(float64(i + len(counters[2])))

		tracker.metrics.Evictions.With(tracker.Labels()).Add(float64(i + len(counters[3])))
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

		var metric *dto.Metric
		for _, name := range counters {
			exp := float64(i + len(name))

			if name != counters[3] {
				// Make a copy since we need to add a label
				l := make(prometheus.Labels, len(labels))
				for k, v := range labels {
					l[k] = v
				}
				l["status"] = "hit"

				metric = promtest.MustFindMetric(t, mfs, name, l)
			} else {
				metric = promtest.MustFindMetric(t, mfs, name, labels)
			}

			if got := metric.GetCounter().GetValue(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}
	}
}

func TestMetrics_Partition(t *testing.T) {
	// metrics to be shared by multiple file stores.
	metrics := newPartitionMetrics(prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newPartitionTracker(metrics, prometheus.Labels{"engine_id": "0", "index_partition": "0", "node_id": "0"})
	t2 := newPartitionTracker(metrics, prometheus.Labels{"engine_id": "1", "index_partition": "0", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	base := namespace + "_" + partitionSubsystem + "_"

	// All the metric names
	gauges := []string{
		base + "series_total",
		base + "measurements_total",
		base + "files_total",
		base + "disk_bytes",
		base + "compactions_active",
	}

	counters := []string{
		base + "series_created",
		base + "series_dropped",
		base + "compactions_total",
	}

	histograms := []string{
		base + "series_created_duration_ns",
		base + "compactions_duration_seconds",
	}

	// Generate some measurements.
	for i, tracker := range []*partitionTracker{t1, t2} {
		tracker.SetSeries(uint64(i + len(gauges[0])))
		tracker.SetMeasurements(uint64(i + len(gauges[1])))
		labels := tracker.Labels()
		labels["type"] = "index"
		tracker.metrics.FilesTotal.With(labels).Add(float64(i + len(gauges[2])))
		tracker.SetDiskSize(uint64(i + len(gauges[3])))
		labels = tracker.Labels()
		labels["level"] = "2"
		tracker.metrics.CompactionsActive.With(labels).Add(float64(i + len(gauges[4])))

		tracker.metrics.SeriesCreated.With(tracker.Labels()).Add(float64(i + len(counters[0])))
		tracker.AddSeriesDropped(uint64(i + len(counters[1])))
		labels = tracker.Labels()
		labels["level"] = "2"
		labels["status"] = "ok"
		tracker.metrics.Compactions.With(labels).Add(float64(i + len(counters[2])))

		tracker.metrics.SeriesCreatedDuration.With(tracker.Labels()).Observe(float64(i + len(histograms[0])))
		labels = tracker.Labels()
		labels["level"] = "2"
		tracker.metrics.CompactionDuration.With(labels).Observe(float64(i + len(histograms[1])))
	}

	// Test that all the correct metrics are present.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	// The label variants for the two caches.
	labelVariants := []prometheus.Labels{
		prometheus.Labels{"engine_id": "0", "index_partition": "0", "node_id": "0"},
		prometheus.Labels{"engine_id": "1", "index_partition": "0", "node_id": "0"},
	}

	for j, labels := range labelVariants {
		var metric *dto.Metric

		for i, name := range gauges {
			exp := float64(j + len(name))

			if i == 2 {
				l := make(prometheus.Labels, len(labels))
				for k, v := range labels {
					l[k] = v
				}
				l["type"] = "index"
				metric = promtest.MustFindMetric(t, mfs, name, l)
			} else if i == 4 {
				l := make(prometheus.Labels, len(labels))
				for k, v := range labels {
					l[k] = v
				}
				l["level"] = "2"
				metric = promtest.MustFindMetric(t, mfs, name, l)
			} else {
				metric = promtest.MustFindMetric(t, mfs, name, labels)
			}

			if got := metric.GetGauge().GetValue(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}

		for i, name := range counters {
			exp := float64(j + len(name))

			if i == 2 {
				// Make a copy since we need to add a label
				l := make(prometheus.Labels, len(labels))
				for k, v := range labels {
					l[k] = v
				}
				l["status"] = "ok"
				l["level"] = "2"

				metric = promtest.MustFindMetric(t, mfs, name, l)
			} else {
				metric = promtest.MustFindMetric(t, mfs, name, labels)
			}

			if got := metric.GetCounter().GetValue(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}

		for i, name := range histograms {
			exp := float64(j + len(name))

			if i == 1 {
				// Make a copy since we need to add a label
				l := make(prometheus.Labels, len(labels))
				for k, v := range labels {
					l[k] = v
				}
				l["level"] = "2"

				metric = promtest.MustFindMetric(t, mfs, name, l)
			} else {
				metric = promtest.MustFindMetric(t, mfs, name, labels)
			}

			if got := metric.GetHistogram().GetSampleSum(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}
	}
}
