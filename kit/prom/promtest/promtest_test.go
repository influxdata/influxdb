package promtest_test

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/influxdata/platform/kit/prom"
	"github.com/influxdata/platform/kit/prom/promtest"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func helperCollectors() []prometheus.Collector {
	myCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "my",
		Subsystem: "random",
		Name:      "counter",
		Help:      "Just a random counter.",
	})
	myGaugeVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "my",
		Subsystem: "random",
		Name:      "gaugevec",
		Help:      "Just a random gauge vector.",
	}, []string{"label1", "label2"})

	myCounter.Inc()
	myGaugeVec.WithLabelValues("one", "two").Set(3)

	return []prometheus.Collector{myCounter, myGaugeVec}
}

func TestFindMetric(t *testing.T) {
	reg := prom.NewRegistry()
	reg.MustRegister(helperCollectors()...)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	c := promtest.MustFindMetric(t, mfs, "my_random_counter", nil)
	if got := c.GetCounter().GetValue(); got != 1 {
		t.Fatalf("expected counter to be 1, got %v", got)
	}

	g := promtest.MustFindMetric(t, mfs, "my_random_gaugevec", map[string]string{"label1": "one", "label2": "two"})
	if got := g.GetGauge().GetValue(); got != 3 {
		t.Fatalf("expected gauge to be 3, got %v", got)
	}
}

// fakeT helps us to assert that MustFindMetric calls FailNow when the metric isn't found.
type fakeT struct {
	// Embed a T so we don't have to reimplement everything.
	// It's fine to leave T nil - fakeT will panic if calling a method we haven't implemented.
	*testing.T

	logBuf bytes.Buffer

	failed bool
}

func (t *fakeT) Helper() {}

func (t *fakeT) Log(args ...interface{}) {
	fmt.Fprint(&t.logBuf, args...)
	t.logBuf.WriteString("\n")
}

func (t *fakeT) Logf(format string, args ...interface{}) {
	fmt.Fprintf(&t.logBuf, format, args...)
	t.logBuf.WriteString("\n")
}

func (t *fakeT) FailNow() {
	t.failed = true
}

func (t *fakeT) Fatalf(format string, args ...interface{}) {
	t.Logf(format, args...)
	t.FailNow()
}

func TestMustFindMetric(t *testing.T) {
	reg := prom.NewRegistry()
	reg.MustRegister(helperCollectors()...)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	ft := new(fakeT)

	// Doesn't log when metric is found.
	_ = promtest.MustFindMetric(ft, mfs, "my_random_counter", nil)
	if ft.failed {
		t.Fatalf("MustFindMetric failed when it should not have. message: %s", ft.logBuf.String())
	}

	// Logs and fails when family name not found.
	ft = new(fakeT)
	_ = promtest.MustFindMetric(ft, mfs, "missing_name", nil)
	if !ft.failed {
		t.Fatal("MustFindMetric should have failed but didn't")
	}
	logged := ft.logBuf.String()
	if !strings.Contains(logged, `name "missing_name" not found`) {
		t.Fatalf("did not log the looked up name which was not found. message: %s", logged)
	}
	if !strings.Contains(logged, "my_random_counter") || !strings.Contains(logged, "my_random_gaugevec") {
		t.Fatalf("did not log the available metric names. message: %s", logged)
	}

	// Logs and fails when family name found but metric labels mismatch.
	ft = new(fakeT)
	_ = promtest.MustFindMetric(ft, mfs, "my_random_counter", map[string]string{"unknown": "label"})
	if !ft.failed {
		t.Fatal("MustFindMetric should have failed but didn't")
	}

	ft = new(fakeT)
	_ = promtest.MustFindMetric(ft, mfs, "my_random_gaugevec", map[string]string{"unknown": "label"})
	if !ft.failed {
		t.Fatal("MustFindMetric should have failed but didn't")
	}
	logged = ft.logBuf.String()
	if !strings.Contains(logged, `"label1": "one"`) || !strings.Contains(logged, `"label2": "two"`) {
		t.Fatalf("did not log the available label names. message: %s", logged)
	}

	ft = new(fakeT)
	_ = promtest.MustFindMetric(ft, mfs, "my_random_gaugevec", map[string]string{"label1": "one", "label2": "two", "label3": "imaginary"})
	if !ft.failed {
		t.Fatal("MustFindMetric should have failed but didn't")
	}
	logged = ft.logBuf.String()
	if !strings.Contains(logged, `"label1": "one"`) || !strings.Contains(logged, `"label2": "two"`) {
		t.Fatalf("did not log the available label names. message: %s", logged)
	}
}

func TestMustGather(t *testing.T) {
	expErr := errors.New("failed to gather")
	g := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		return nil, expErr
	})

	ft := new(fakeT)
	_ = promtest.MustGather(ft, g)
	if !ft.failed {
		t.Fatal("MustGather should have failed but didn't")
	}
	logged := ft.logBuf.String()
	if !strings.HasPrefix(logged, "error while gathering metrics:") || !strings.Contains(logged, expErr.Error()) {
		t.Fatalf("did not log the expected error message: %s", logged)
	}

	expMF := []*dto.MetricFamily{} // Use a non-nil, zero-length slice for a simple-ish check.
	g = prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		return expMF, nil
	})
	ft = new(fakeT)
	gotMF := promtest.MustGather(ft, g)
	if ft.failed {
		t.Fatalf("MustGather should not have failed")
	}
	if gotMF == nil || len(gotMF) != 0 {
		t.Fatalf("exp: %v, got: %v", expMF, gotMF)
	}
}

func TestFromHTTPResponse(t *testing.T) {
	reg := prom.NewRegistry()
	reg.MustRegister(helperCollectors()...)

	s := httptest.NewServer(reg.HTTPHandler())
	defer s.Close()

	resp, err := http.Get(s.URL) // Didn't specify a path for the handler, so any path should be fine.
	if err != nil {
		t.Fatal(err)
	}

	mfs, err := promtest.FromHTTPResponse(resp)
	if err != nil {
		t.Fatal(err)
	}

	if len(mfs) != 2 {
		t.Fatalf("expected 2 metrics but got %d", len(mfs))
	}

	c := promtest.MustFindMetric(t, mfs, "my_random_counter", nil)
	if got := c.GetCounter().GetValue(); got != 1 {
		t.Fatalf("expected counter to be 1, got %v", got)
	}

	g := promtest.MustFindMetric(t, mfs, "my_random_gaugevec", map[string]string{"label1": "one", "label2": "two"})
	if got := g.GetGauge().GetValue(); got != 3 {
		t.Fatalf("expected gauge to be 3, got %v", got)
	}
}
