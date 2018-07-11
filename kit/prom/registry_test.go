package prom_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/influxdata/platform/kit/prom"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRegistry_Logger(t *testing.T) {
	reg := prom.NewRegistry()

	// Normal use: HTTP handler is created immediately...
	s := httptest.NewServer(reg.HTTPHandler())
	defer s.Close()

	// ... and then WithLogger is called.
	core, logs := observer.New(zap.DebugLevel)
	logger := zap.New(core)
	reg.WithLogger(logger)

	// Force an error with a fake collector.
	reg.MustRegister(errorCollector{})
	resp, err := http.Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	foundLog := false
	for _, le := range logs.All() {
		if strings.Contains(le.Message, "invalid metric from errorCollector") {
			foundLog = true
			break
		}
	}

	if !foundLog {
		t.Fatalf("registry logger did not log error from metric collection")
	}
}

type errorCollector struct{}

var _ prometheus.Collector = errorCollector{}

var ecDesc = prometheus.NewDesc("error_collector_desc", "A required description for the error collector", nil, nil)

func (errorCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- ecDesc
}

func (errorCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.NewInvalidMetric(
		ecDesc,
		errors.New("invalid metric from errorCollector"),
	)
}
