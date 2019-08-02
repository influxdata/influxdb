package influxdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/kit/prom/promtest"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxdb/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

type mockTableIterator struct {
}

func (mockTableIterator) Do(f func(flux.Table) error) error {
	return nil
}

func (mockTableIterator) Statistics() cursors.CursorStats {
	return cursors.CursorStats{}
}

type mockReader struct {
}

func (mockReader) ReadFilter(ctx context.Context, spec influxdb.ReadFilterSpec, alloc *memory.Allocator) (influxdb.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) ReadGroup(ctx context.Context, spec influxdb.ReadGroupSpec, alloc *memory.Allocator) (influxdb.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) ReadTagKeys(ctx context.Context, spec influxdb.ReadTagKeysSpec, alloc *memory.Allocator) (influxdb.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) ReadTagValues(ctx context.Context, spec influxdb.ReadTagValuesSpec, alloc *memory.Allocator) (influxdb.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) Close() {
}

type mockAdministration struct {
	DependenciesFn func() execute.Dependencies
}

func (mockAdministration) Context() context.Context {
	return context.Background()
}

func (mockAdministration) ResolveTime(qt flux.Time) execute.Time {
	return 0
}

func (mockAdministration) StreamContext() execute.StreamContext {
	return nil
}

func (mockAdministration) Allocator() *memory.Allocator {
	return &memory.Allocator{}
}

func (mockAdministration) Parents() []execute.DatasetID {
	return nil
}

func (a mockAdministration) Dependencies() execute.Dependencies {
	return a.DependenciesFn()
}

const (
	labelKey   = "key1"
	labelValue = "value1"
)

// TestMetrics ensures that the metrics collected by an influxdb source are recorded.
func TestMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	orgID, err := platform.IDFromString("deadbeefbeefdead")
	if err != nil {
		t.Fatal(err)
	}

	var a execute.Administration
	{
		fromDeps := influxdb.Dependencies{
			Reader:             &mockReader{},
			BucketLookup:       mock.BucketLookup{},
			OrganizationLookup: mock.OrganizationLookup{},
			Metrics:            influxdb.NewMetrics([]string{labelKey}),
		}
		a = &mockAdministration{
			DependenciesFn: func() execute.Dependencies {
				deps := make(map[string]interface{})
				deps[influxdb.FromKind] = fromDeps
				return deps
			},
		}
	}

	for _, v := range a.Dependencies() {
		if pc, ok := v.(prom.PrometheusCollector); ok {
			reg.MustRegister(pc.PrometheusCollectors()...)
		}
	}

	// This key/value pair added to the context will appear as a label in the prometheus histogram.
	ctx := context.WithValue(context.Background(), labelKey, labelValue)
	rfs := influxdb.ReadFilterSource(
		execute.DatasetID(uuid.FromTime(time.Now())),
		&mockReader{},
		influxdb.ReadFilterSpec{
			OrganizationID: *orgID,
		},
		a,
	)
	rfs.Run(ctx)

	// Verify that we sampled the execution of the source by checking the prom registry.
	mfs := promtest.MustGather(t, reg)
	expectedLabels := map[string]string{
		"org":  "deadbeefbeefdead",
		"key1": "value1",
		"op":   "readFilter",
	}
	m := promtest.MustFindMetric(t, mfs, "query_influxdb_source_read_request_duration_seconds", expectedLabels)
	if want, got := uint64(1), *(m.Histogram.SampleCount); want != got {
		t.Fatalf("expected sample count of %v, got %v", want, got)
	}
}
