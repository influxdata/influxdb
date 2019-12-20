package influxdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/dependenciestest"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxdb/v2/uuid"
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
	Ctx context.Context
}

func (a mockAdministration) Context() context.Context {
	return a.Ctx
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

	deps := influxdb.Dependencies{
		FluxDeps: dependenciestest.Default(),
		StorageDeps: influxdb.StorageDependencies{
			FromDeps: influxdb.FromDependencies{
				Reader:             &mockReader{},
				BucketLookup:       mock.BucketLookup{},
				OrganizationLookup: mock.OrganizationLookup{},
				Metrics:            influxdb.NewMetrics([]string{labelKey}),
			},
		},
	}
	reg.MustRegister(deps.PrometheusCollectors()...)

	// This key/value pair added to the context will appear as a label in the prometheus histogram.
	ctx := context.WithValue(context.Background(), labelKey, labelValue) //lint:ignore SA1029 this is a temporary ignore until we have time to create an appropriate type
	// Injecting deps
	ctx = deps.Inject(ctx)
	a := &mockAdministration{Ctx: ctx}
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
