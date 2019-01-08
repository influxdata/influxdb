package tsm1

import (
	"testing"

	"github.com/influxdata/influxdb/kit/prom/promtest"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_Filestore(t *testing.T) {
	// metrics to be shared by multiple file stores.
	metrics := newFileMetrics(prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newFileTracker(metrics, prometheus.Labels{"engine_id": "0", "node_id": "0"})
	t2 := newFileTracker(metrics, prometheus.Labels{"engine_id": "1", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	// Generate some measurements.
	t1.AddBytes(100)
	t1.SetFileCount(3)

	t2.AddBytes(200)
	t2.SetFileCount(4)

	// Test that all the correct metrics are present.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	base := namespace + "_" + fileStoreSubsystem + "_"
	m1Bytes := promtest.MustFindMetric(t, mfs, base+"disk_bytes", prometheus.Labels{"engine_id": "0", "node_id": "0"})
	m2Bytes := promtest.MustFindMetric(t, mfs, base+"disk_bytes", prometheus.Labels{"engine_id": "1", "node_id": "0"})
	m1Files := promtest.MustFindMetric(t, mfs, base+"total", prometheus.Labels{"engine_id": "0", "node_id": "0"})
	m2Files := promtest.MustFindMetric(t, mfs, base+"total", prometheus.Labels{"engine_id": "1", "node_id": "0"})

	if m, got, exp := m1Bytes, m1Bytes.GetGauge().GetValue(), 100.0; got != exp {
		t.Errorf("[%s] got %v, expected %v", m, got, exp)
	}

	if m, got, exp := m1Files, m1Files.GetGauge().GetValue(), 3.0; got != exp {
		t.Errorf("[%s] got %v, expected %v", m, got, exp)
	}

	if m, got, exp := m2Bytes, m2Bytes.GetGauge().GetValue(), 200.0; got != exp {
		t.Errorf("[%s] got %v, expected %v", m, got, exp)
	}

	if m, got, exp := m2Files, m2Files.GetGauge().GetValue(), 4.0; got != exp {
		t.Errorf("[%s] got %v, expected %v", m, got, exp)
	}

}

func TestMetrics_Cache(t *testing.T) {
	// metrics to be shared by multiple file stores.
	metrics := newCacheMetrics(prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newCacheTracker(metrics, prometheus.Labels{"engine_id": "0", "node_id": "0"})
	t2 := newCacheTracker(metrics, prometheus.Labels{"engine_id": "1", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	base := namespace + "_" + cacheSubsystem + "_"

	// All the metric names
	gauges := []string{
		base + "inuse_bytes",
		base + "disk_bytes",
		base + "age_seconds",
		base + "snapshots_active",
	}

	counters := []string{
		base + "snapshot_bytes",
		base + "written_bytes",
		base + "writes_total",
	}

	// Generate some measurements.
	for i, tracker := range []*cacheTracker{t1, t2} {
		tracker.SetMemBytes(uint64(i + len(gauges[0])))
		tracker.SetDiskBytes(uint64(i + len(gauges[1])))
		tracker.metrics.Age.With(tracker.Labels()).Set(float64(i + len(gauges[2])))
		tracker.SetSnapshotsActive(uint64(i + len(gauges[3])))

		tracker.AddSnapshottedBytes(uint64(i + len(counters[0])))
		tracker.AddWrittenBytesOK(uint64(i + len(counters[1])))

		labels := tracker.Labels()
		labels["status"] = "ok"
		tracker.metrics.Writes.With(labels).Add(float64(i + len(counters[2])))
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

			if name == counters[1] || name == counters[2] {
				labels["status"] = "ok"
			}
			metric := promtest.MustFindMetric(t, mfs, name, labels)
			if got := metric.GetCounter().GetValue(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}
	}
}

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

func TestMetrics_Compactions(t *testing.T) {
	// metrics to be shared by multiple file stores.
	metrics := newCompactionMetrics(prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newCompactionTracker(metrics, prometheus.Labels{"engine_id": "0", "node_id": "0"})
	t2 := newCompactionTracker(metrics, prometheus.Labels{"engine_id": "1", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	base := namespace + "_" + compactionSubsystem + "_"

	// All the metric names
	gauges := []string{
		base + "active",
		base + "queued",
	}

	counters := []string{base + "total"}
	histograms := []string{base + "duration_seconds"}

	// Generate some measurements.
	for i, tracker := range []*compactionTracker{t1, t2} {
		labels := tracker.Labels(2)
		tracker.metrics.CompactionsActive.With(labels).Add(float64(i + len(gauges[0])))
		tracker.SetQueue(2, uint64(i+len(gauges[1])))

		labels = tracker.Labels(2)
		labels["status"] = "ok"
		tracker.metrics.Compactions.With(labels).Add(float64(i + len(counters[0])))

		labels = tracker.Labels(2)
		tracker.metrics.CompactionDuration.With(labels).Observe(float64(i + len(histograms[0])))
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
		labels["level"] = "2"

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
