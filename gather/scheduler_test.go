// +build !race

package gather

import (
	"context"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/nats"
	"go.uber.org/zap"
)

func TestScheduler(t *testing.T) {
	_, publisher, subscriber := newTestingNats(t)

	// Create top level logger
	logger := influxlogger.New(os.Stdout)
	ts := httptest.NewServer(&mockHTTPHandler{
		responseMap: map[string]string{
			"/metrics": sampleRespSmall,
		},
	})

	storage := &mockStorage{
		Metrics: make(map[int64]Metrics),
		Targets: []platform.ScraperTarget{
			{
				Type: platform.PrometheusScraperType,
				URL:  ts.URL + "/metrics",
			},
		},
	}
	subscriber.Subscribe(MetricsSubject, "", &StorageHandler{
		Logger:  logger,
		Storage: storage,
	})

	scheduler, err := NewScheduler(10, logger,
		storage, publisher, subscriber, time.Millisecond*25, time.Microsecond*15)

	go func() {
		err = scheduler.Run(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
	}()

	// let scheduler run for 80 miliseconds.
	<-time.After(time.Millisecond * 80)
	want := Metrics{
		Name: "go_goroutines",
		Type: MetricTypeGauge,
		Tags: map[string]string{},
		Fields: map[string]interface{}{
			"gauge": float64(36),
		},
	}

	if len(storage.Metrics) < 3 {
		t.Fatalf("non metrics stored, len %d", len(storage.Metrics))
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

func newTestingNats(t *testing.T) (
	server *nats.Server,
	publisher *nats.AsyncPublisher,
	subscriber *nats.QueueSubscriber,
) {
	dir, err := ioutil.TempDir("", "influxdata-platform-nats-")
	if err != nil {
		t.Fatal("unable to open temporary nats folder")
	}
	// NATS streaming server
	server = nats.NewServer(nats.Config{FilestoreDir: dir})
	if err := server.Open(); err != nil {
		t.Fatal("failed to start nats streaming server ", dir, zap.Error(err))
	}
	publisher = nats.NewAsyncPublisher("nats-testing-publisher")
	if err = publisher.Open(); err != nil {
		t.Fatal("failed to connect to streaming server", zap.Error(err))
	}

	subscriber = nats.NewQueueSubscriber("nats-testing-subscriber")
	if err := subscriber.Open(); err != nil {
		t.Fatal("failed to connect to streaming server", zap.Error(err))
	}

	return server, publisher, subscriber
}
