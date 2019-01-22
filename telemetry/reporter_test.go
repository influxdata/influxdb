package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	pr "github.com/influxdata/influxdb/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap/zaptest"
)

func TestReport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	logger := zaptest.NewLogger(t)
	store := newReportingStore()
	timestamps := &AddTimestamps{
		now: func() time.Time {
			return time.Unix(0, 0)
		},
	}

	gw := NewPushGateway(logger, store, timestamps)
	gw.Encoder = &pr.JSON{}

	ts := httptest.NewServer(http.HandlerFunc(gw.Handler))
	defer ts.Close()

	mfs := []*dto.MetricFamily{NewCounter("influxdb_buckets_total", 1.0)}
	gatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		return mfs, nil
	})

	pusher := NewPusher(gatherer)
	pusher.URL = ts.URL

	reporter := &Reporter{
		Pusher:   pusher,
		Logger:   logger,
		Interval: 30 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		reporter.Report(ctx)
	}()

	got := <-store.ch

	// Encode to JSON to make it easier to compare
	want, _ := pr.EncodeJSON(timestamps.Transform(mfs))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Reporter.Report() = %s, want %s", got, want)
	}

	cancel()
}

func newReportingStore() *reportingStore {
	return &reportingStore{
		ch: make(chan []byte, 1),
	}
}

type reportingStore struct {
	ch chan []byte
}

func (s *reportingStore) WriteMessage(ctx context.Context, data []byte) error {
	s.ch <- data
	return nil
}
