package influxdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/dependenciestest"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/stdlib/universe"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
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

func (mockReader) ReadFilter(ctx context.Context, spec query.ReadFilterSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) ReadGroup(ctx context.Context, spec query.ReadGroupSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) ReadTagKeys(ctx context.Context, spec query.ReadTagKeysSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) ReadTagValues(ctx context.Context, spec query.ReadTagValuesSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) ReadWindowAggregate(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) ReadSeriesCardinality(ctx context.Context, spec query.ReadSeriesCardinalitySpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return &mockTableIterator{}, nil
}

func (mockReader) SupportReadSeriesCardinality(ctx context.Context) bool {
	return false
}

func (mockReader) Close() {
}

type mockAdministration struct {
	Ctx          context.Context
	StreamBounds *execute.Bounds
}

func (a mockAdministration) Context() context.Context {
	return a.Ctx
}

func (mockAdministration) ResolveTime(qt flux.Time) execute.Time {
	return 0
}

func (a mockAdministration) StreamContext() execute.StreamContext {
	return a
}

func (a mockAdministration) Bounds() *execute.Bounds {
	return a.StreamBounds
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
		query.ReadFilterSpec{
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

type TableIterator struct {
	Tables []*executetest.Table
}

func (t *TableIterator) Do(f func(flux.Table) error) error {
	for _, table := range t.Tables {
		if err := f(table); err != nil {
			return err
		}
	}
	return nil
}

func (t *TableIterator) Statistics() cursors.CursorStats {
	return cursors.CursorStats{}
}

func TestReadWindowAggregateSource(t *testing.T) {
	t.Skip("test panics in CI; issue: https://github.com/influxdata/influxdb/issues/17847")

	orgID, bucketID := platform.ID(1), platform.ID(2)
	executetest.RunSourceHelper(t,
		[]*executetest.Table{
			{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "host", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
				},
				KeyCols: []string{"_measurement", "_field", "host"},
				Data: [][]interface{}{
					{execute.Time(0), "cpu", "usage_user", "server01", 2.0},
					{execute.Time(10), "cpu", "usage_user", "server01", 1.5},
					{execute.Time(20), "cpu", "usage_user", "server01", 5.0},
				},
			},
			{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "host", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
				},
				KeyCols: []string{"_measurement", "_field", "host"},
				Data: [][]interface{}{
					{execute.Time(0), "cpu", "usage_system", "server01", 8.0},
					{execute.Time(10), "cpu", "usage_system", "server01", 3.0},
					{execute.Time(20), "cpu", "usage_system", "server01", 6.0},
				},
			},
		},
		nil,
		func(id execute.DatasetID) execute.Source {
			pspec := &influxdb.ReadWindowAggregatePhysSpec{
				ReadRangePhysSpec: influxdb.ReadRangePhysSpec{
					BucketID: bucketID.String(),
				},
				WindowEvery: flux.ConvertDuration(10 * time.Nanosecond),
				Aggregates: []plan.ProcedureKind{
					universe.SumKind,
				},
			}
			reader := &mock.StorageReader{
				ReadWindowAggregateFn: func(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error) {
					if want, got := orgID, spec.OrganizationID; want != got {
						t.Errorf("unexpected organization id -want/+got:\n\t- %s\n\t+ %s", want, got)
					}
					if want, got := bucketID, spec.BucketID; want != got {
						t.Errorf("unexpected bucket id -want/+got:\n\t- %s\n\t+ %s", want, got)
					}
					if want, got := (execute.Bounds{Start: 0, Stop: 30}), spec.Bounds; want != got {
						t.Errorf("unexpected bounds -want/+got:\n%s", cmp.Diff(want, got))
					}
					if want, got := int64(10), spec.WindowEvery; want != got {
						t.Errorf("unexpected window every value -want/+got:\n\t- %d\n\t+ %d", want, got)
					}
					if want, got := []plan.ProcedureKind{universe.SumKind}, spec.Aggregates; !cmp.Equal(want, got) {
						t.Errorf("unexpected aggregates -want/+got:\n%s", cmp.Diff(want, got))
					}
					return &TableIterator{
						Tables: []*executetest.Table{
							{
								ColMeta: []flux.ColMeta{
									{Label: "_time", Type: flux.TTime},
									{Label: "_measurement", Type: flux.TString},
									{Label: "_field", Type: flux.TString},
									{Label: "host", Type: flux.TString},
									{Label: "_value", Type: flux.TFloat},
								},
								KeyCols: []string{"_measurement", "_field", "host"},
								Data: [][]interface{}{
									{execute.Time(0), "cpu", "usage_user", "server01", 2.0},
									{execute.Time(10), "cpu", "usage_user", "server01", 1.5},
									{execute.Time(20), "cpu", "usage_user", "server01", 5.0},
								},
							},
							{
								ColMeta: []flux.ColMeta{
									{Label: "_time", Type: flux.TTime},
									{Label: "_measurement", Type: flux.TString},
									{Label: "_field", Type: flux.TString},
									{Label: "host", Type: flux.TString},
									{Label: "_value", Type: flux.TFloat},
								},
								KeyCols: []string{"_measurement", "_field", "host"},
								Data: [][]interface{}{
									{execute.Time(0), "cpu", "usage_system", "server01", 8.0},
									{execute.Time(10), "cpu", "usage_system", "server01", 3.0},
									{execute.Time(20), "cpu", "usage_system", "server01", 6.0},
								},
							},
						},
					}, nil
				},
			}

			metrics := influxdb.NewMetrics(nil)
			deps := influxdb.StorageDependencies{
				FromDeps: influxdb.FromDependencies{
					Reader:  reader,
					Metrics: metrics,
				},
			}
			ctx := deps.Inject(context.Background())
			ctx = query.ContextWithRequest(ctx, &query.Request{
				OrganizationID: orgID,
			})
			a := mockAdministration{
				Ctx: ctx,
				StreamBounds: &execute.Bounds{
					Start: execute.Time(0),
					Stop:  execute.Time(30),
				},
			}

			s, err := influxdb.CreateReadWindowAggregateSource(pspec, id, a)
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
	)
}
