package gather

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
)

var (
	orgID, _    = platform.IDFromString("020f755c3c082000")
	bucketID, _ = platform.IDFromString("020f755c3c082001")
)

func TestPrometheusScraper(t *testing.T) {
	cases := []struct {
		name    string
		ms      []Metrics
		handler *mockHTTPHandler
		hasErr  bool
	}{
		{
			name:   "bad request",
			hasErr: true,
		},
		{
			name: "empty request",
			handler: &mockHTTPHandler{
				responseMap: map[string]string{
					"/metrics": "",
				},
			},
			hasErr: true,
		},
		{
			name: "regular metrics",
			handler: &mockHTTPHandler{
				responseMap: map[string]string{
					"/metrics": sampleResp,
				},
			},
			ms: []Metrics{
				{
					Name: "go_gc_duration_seconds",
					Type: MetricTypeSummary,
					Fields: map[string]interface{}{
						"count": float64(326),
						"sum":   0.07497837,
						"0":     3.6257e-05,
						"0.25":  0.0001434,
						"0.5":   0.000194491,
						"0.75":  0.000270339,
						"1":     0.000789365,
					},
					Tags: map[string]string{},
				},
				{
					Name: "go_goroutines",
					Type: MetricTypeGauge,
					Tags: map[string]string{},
					Fields: map[string]interface{}{
						"gauge": float64(36),
					},
				},
				{
					Name: "go_info",
					Type: MetricTypeGauge,
					Tags: map[string]string{
						"version": "go1.10.3",
					},
					Fields: map[string]interface{}{
						"gauge": float64(1),
					},
				},
				{
					Name: "go_memstats_alloc_bytes",
					Type: MetricTypeGauge,
					Tags: map[string]string{},
					Fields: map[string]interface{}{
						"gauge": 2.0091312e+07,
					},
				},
				{
					Name: "go_memstats_alloc_bytes_total",
					Type: MetricTypeCounter,
					Fields: map[string]interface{}{
						"counter": 4.183173328e+09,
					},
					Tags: map[string]string{},
				},
				{
					Name: "go_memstats_buck_hash_sys_bytes",
					Type: MetricTypeGauge,
					Tags: map[string]string{},
					Fields: map[string]interface{}{
						"gauge": 1.533852e+06,
					},
				},
				{
					Name: "go_memstats_frees_total",
					Type: MetricTypeCounter,
					Tags: map[string]string{},
					Fields: map[string]interface{}{
						"counter": 1.8944339e+07,
					},
				},
				{
					Name: "go_memstats_gc_cpu_fraction",
					Type: MetricTypeGauge,
					Tags: map[string]string{},
					Fields: map[string]interface{}{
						"gauge": 1.972734963012756e-05,
					},
				},
			},
			hasErr: false,
		},
	}
	for _, c := range cases {
		scraper := newPrometheusScraper()
		var url string
		if c.handler != nil {
			ts := httptest.NewServer(c.handler)
			defer ts.Close()
			url = ts.URL
		}
		results, err := scraper.Gather(context.Background(), influxdb.ScraperTarget{
			URL:      url + "/metrics",
			OrgID:    *orgID,
			BucketID: *bucketID,
		})
		if err != nil && !c.hasErr {
			t.Fatalf("scraper parse err in testing %s: %v", c.name, err)
		}
		if len(c.ms) != len(results.MetricsSlice) {
			t.Fatalf("scraper parse metrics incorrect length, want %d, got %d",
				len(c.ms), len(results.MetricsSlice))
		}
		for _, m := range results.MetricsSlice {
			for _, cm := range c.ms {
				if m.Name == cm.Name {
					if diff := cmp.Diff(m, cm, metricsCmpOption); diff != "" {
						t.Fatalf("scraper parse metrics want %v, got %v", cm, m)
					}
				}
			}

		}
	}
}

const sampleResp = `
# 	HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 3.6257e-05
go_gc_duration_seconds{quantile="0.25"} 0.0001434
go_gc_duration_seconds{quantile="0.5"} 0.000194491
go_gc_duration_seconds{quantile="0.75"} 0.000270339
go_gc_duration_seconds{quantile="1"} 0.000789365
go_gc_duration_seconds_sum 0.07497837
go_gc_duration_seconds_count 326
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 36
# HELP go_info Information about the Go environment.
# TYPE go_info gauge
go_info{version="go1.10.3"} 1
# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.
# TYPE go_memstats_alloc_bytes gauge
go_memstats_alloc_bytes 2.0091312e+07
# HELP go_memstats_alloc_bytes_total Total number of bytes allocated, even if freed.
# TYPE go_memstats_alloc_bytes_total counter
go_memstats_alloc_bytes_total 4.183173328e+09
# HELP go_memstats_buck_hash_sys_bytes Number of bytes used by the profiling bucket hash table.
# TYPE go_memstats_buck_hash_sys_bytes gauge
go_memstats_buck_hash_sys_bytes 1.533852e+06
# HELP go_memstats_frees_total Total number of frees.
# TYPE go_memstats_frees_total counter
go_memstats_frees_total 1.8944339e+07
# HELP go_memstats_gc_cpu_fraction The fraction of this program's available CPU time used by the GC since the program started.
# TYPE go_memstats_gc_cpu_fraction gauge
go_memstats_gc_cpu_fraction 1.972734963012756e-05
`

// mockStorage implement recorder interface
// and influxdb.ScraperTargetStoreService interface.
type mockStorage struct {
	sync.RWMutex
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
	TotalGatherJobs chan struct{}
	Metrics         map[time.Time]Metrics
	Targets         []influxdb.ScraperTarget
}

func (s *mockStorage) Record(collected MetricsCollection) error {
	s.Lock()
	defer s.Unlock()
	for _, m := range collected.MetricsSlice {
		s.Metrics[m.Timestamp] = m
	}
	s.TotalGatherJobs <- struct{}{}
	return nil
}

func (s *mockStorage) ListTargets(ctx context.Context, filter influxdb.ScraperTargetFilter) (targets []influxdb.ScraperTarget, err error) {
	s.RLock()
	defer s.RUnlock()
	if s.Targets == nil {
		s.Lock()
		s.Targets = make([]influxdb.ScraperTarget, 0)
		s.Unlock()
	}
	return s.Targets, nil
}

func (s *mockStorage) AddTarget(ctx context.Context, t *influxdb.ScraperTarget, userID platform.ID) error {
	s.Lock()
	defer s.Unlock()
	if s.Targets == nil {
		s.Targets = make([]influxdb.ScraperTarget, 0)
	}
	s.Targets = append(s.Targets, *t)
	return nil
}

func (s *mockStorage) RemoveTarget(ctx context.Context, id platform.ID) error {
	s.Lock()
	defer s.Unlock()

	if s.Targets == nil {
		return nil
	}
	for k, v := range s.Targets {
		if v.ID == id {
			s.Targets = append(s.Targets[:k], s.Targets[k+1:]...)
			break
		}
	}
	return nil
}

func (s *mockStorage) GetTargetByID(ctx context.Context, id platform.ID) (target *influxdb.ScraperTarget, err error) {
	s.RLock()
	defer s.RUnlock()

	for k, v := range s.Targets {
		if v.ID == id {
			target = &s.Targets[k]
			break
		}
	}

	return target, err

}

func (s *mockStorage) UpdateTarget(ctx context.Context, update *influxdb.ScraperTarget, userID platform.ID) (target *influxdb.ScraperTarget, err error) {
	s.Lock()
	defer s.Unlock()

	for k, v := range s.Targets {
		if v.ID.String() == update.ID.String() {
			s.Targets[k] = *update
			break
		}
	}

	return update, err
}

type mockHTTPHandler struct {
	unauthorized bool
	noContent    bool
	responseMap  map[string]string
}

func (h mockHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.unauthorized {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if h.noContent {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s, ok := h.responseMap[r.URL.Path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write([]byte(s))
}

var metricsCmpOption = cmp.Options{
	cmp.Comparer(func(x, y Metrics) bool {
		return x.Name == y.Name &&
			x.Type == y.Type &&
			reflect.DeepEqual(x.Tags, y.Tags) &&
			reflect.DeepEqual(x.Fields, y.Fields)
	}),
}
