package gather

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/mock"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestScheduler(t *testing.T) {
	publisher, subscriber := mock.NewNats()
	totalGatherJobs := 3

	// Create top level logger
	logger := influxlogger.New(os.Stdout)
	ts := httptest.NewServer(&mockHTTPHandler{
		responseMap: map[string]string{
			"/metrics": sampleRespSmall,
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage := &mockStorage{
		Metrics: make(map[time.Time]Metrics),
		Targets: []influxdb.ScraperTarget{
			{
				ID:       influxdbtesting.MustIDBase16("3a0d0a6365646120"),
				Type:     influxdb.PrometheusScraperType,
				URL:      ts.URL + "/metrics",
				OrgID:    *orgID,
				BucketID: *bucketID,
			},
		},
		TotalGatherJobs: make(chan struct{}, totalGatherJobs),
	}

	subscriber.Subscribe(MetricsSubject, "", &RecorderHandler{
		Logger:   logger,
		Recorder: storage,
	})

	scheduler, err := NewScheduler(10, logger,
		storage, publisher, subscriber, time.Millisecond, time.Microsecond)

	go func() {
		err = scheduler.run(ctx)
		if err != nil {
			t.Error(err)
		}
	}()

	go func(scheduler *Scheduler) {
		// let scheduler gather #{totalGatherJobs} metrics.
		for i := 0; i < totalGatherJobs; i++ {
			// make sure timestamp don't overwrite each other
			time.Sleep(time.Millisecond * 10)
			scheduler.gather <- struct{}{}
		}
	}(scheduler)

	// make sure all jobs are done
	for i := 0; i < totalGatherJobs; i++ {
		<-storage.TotalGatherJobs
	}

	want := Metrics{
		Name: "go_goroutines",
		Type: MetricTypeGauge,
		Tags: map[string]string{},
		Fields: map[string]interface{}{
			"gauge": float64(36),
		},
	}

	if len(storage.Metrics) < totalGatherJobs {
		t.Fatalf("metrics stored less than expected, got len %d", len(storage.Metrics))
	}

	for _, v := range storage.Metrics {
		if diff := cmp.Diff(v, want, metricsCmpOption); diff != "" {
			t.Fatalf("scraper parse metrics want %v, got %v", want, v)
		}
	}
	ts.Close()
}

const sampleRespSmall = `
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 36
`
