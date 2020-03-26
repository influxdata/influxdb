package seriesfile

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/influxdata/influxdb/kit/prom/promtest"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMetrics_SeriesPartition(t *testing.T) {
	// metrics to be shared by multiple file stores.
	metrics := newSeriesFileMetrics(prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newSeriesPartitionTracker(metrics, prometheus.Labels{"series_file_partition": "0", "engine_id": "0", "node_id": "0"})
	t2 := newSeriesPartitionTracker(metrics, prometheus.Labels{"series_file_partition": "0", "engine_id": "1", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	base := namespace + "_" + seriesFileSubsystem + "_"

	// All the metric names
	gauges := []string{
		base + "series_total",
		base + "disk_bytes",
		base + "segments_total",
		base + "index_compactions_active",
	}

	counters := []string{
		base + "series_created",
		base + "compactions_total",
	}

	histograms := []string{
		base + "index_compactions_duration_seconds",
	}

	// Generate some measurements.
	for i, tracker := range []*seriesPartitionTracker{t1, t2} {
		tracker.SetSeries(uint64(i + len(gauges[0])))
		tracker.SetDiskSize(uint64(i + len(gauges[1])))
		tracker.SetSegments(uint64(i + len(gauges[2])))

		labels := tracker.Labels()
		labels["component"] = "index"
		tracker.metrics.CompactionsActive.With(labels).Add(float64(i + len(gauges[3])))

		tracker.AddSeriesCreated(uint64(i + len(counters[0])))
		labels = tracker.Labels()
		labels["status"] = "ok"
		tracker.metrics.Compactions.With(labels).Add(float64(i + len(counters[1])))

		labels = tracker.Labels()
		labels["component"] = "index"
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
		labels["series_file_partition"] = "0"
		var metric *dto.Metric

		for _, name := range gauges {
			exp := float64(i + len(name))

			if name == base+"index_compactions_active" {
				// Make a copy since we need to add a label
				l := make(prometheus.Labels, len(labels))
				for k, v := range labels {
					l[k] = v
				}
				l["component"] = "index"
				metric = promtest.MustFindMetric(t, mfs, name, l)
			} else {
				metric = promtest.MustFindMetric(t, mfs, name, labels)
			}

			if got := metric.GetGauge().GetValue(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}

		for _, name := range counters {
			exp := float64(i + len(name))

			if name == base+"compactions_total" {
				// Make a copy since we need to add a label
				l := make(prometheus.Labels, len(labels))
				for k, v := range labels {
					l[k] = v
				}
				l["status"] = "ok"

				metric = promtest.MustFindMetric(t, mfs, name, l)
			} else {
				metric = promtest.MustFindMetric(t, mfs, name, labels)
			}

			if got := metric.GetCounter().GetValue(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}

		for _, name := range histograms {
			// Make a copy since we need to add a label
			l := make(prometheus.Labels, len(labels))
			for k, v := range labels {
				l[k] = v
			}
			l["component"] = "index"

			exp := float64(i + len(name))
			metric := promtest.MustFindMetric(t, mfs, name, l)
			if got := metric.GetHistogram().GetSampleSum(); got != exp {
				t.Errorf("[%s %d] got %v, expected %v", name, i, got, exp)
			}
		}
	}
}

// This test ensures that disabling metrics works even if a series file has been created before.
func TestMetrics_Disabled(t *testing.T) {
	// This test messes with global state. Gotta fix it up otherwise other tests panic. I really
	// am beginning to wonder about our metrics.
	defer func() {
		mmu.Lock()
		sms = nil
		ims = nil
		mmu.Unlock()
	}()

	path, err := ioutil.TempDir("", "sfile-metrics-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	// Step 1. make a series file with metrics and some labels
	sfile := NewSeriesFile(path)
	sfile.SetDefaultMetricLabels(prometheus.Labels{"foo": "bar"})
	if err := sfile.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sfile.Close(); err != nil {
		t.Fatal(err)
	}

	// Step 2. open the series file again, but disable metrics
	sfile = NewSeriesFile(path)
	sfile.DisableMetrics()
	if err := sfile.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer sfile.Close()

	// Step 3. add a series
	points := []models.Point{models.MustNewPoint("a", models.Tags{}, models.Fields{"f": 1.0}, time.Now())}
	if err := sfile.CreateSeriesListIfNotExists(tsdb.NewSeriesCollection(points)); err != nil {
		t.Fatal(err)
	}
}
