package storageflux_test

import (
	"context"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/generate"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	"github.com/influxdata/influxdb/v2/storage/readservice"
	"go.uber.org/zap/zaptest"
)

type SetupFunc func(org, bucket influxdb.ID) (gen.SeriesGenerator, gen.TimeRange)

type StorageReader struct {
	Org    influxdb.ID
	Bucket influxdb.ID
	Bounds execute.Bounds
	Close  func()
	query.StorageReader
}

func NewStorageReader(tb testing.TB, setupFn SetupFunc) *StorageReader {
	logger := zaptest.NewLogger(tb)
	rootDir, err := ioutil.TempDir("", "storage-flux-test")
	if err != nil {
		tb.Fatal(err)
	}
	close := func() { _ = os.RemoveAll(rootDir) }

	idgen := mock.NewMockIDGenerator()
	org, bucket := idgen.ID(), idgen.ID()
	sg, tr := setupFn(org, bucket)

	generator := generate.Generator{}
	if _, err := generator.Run(context.Background(), rootDir, sg); err != nil {
		tb.Fatal(err)
	}

	enginePath := filepath.Join(rootDir, "engine")
	engine := storage.NewEngine(enginePath, storage.NewConfig())
	engine.WithLogger(logger)

	if err := engine.Open(context.Background()); err != nil {
		tb.Fatal(err)
	}
	reader := storageflux.NewReader(readservice.NewStore(engine))
	return &StorageReader{
		Org:    org,
		Bucket: bucket,
		Bounds: execute.Bounds{
			Start: values.ConvertTime(tr.Start),
			Stop:  values.ConvertTime(tr.End),
		},
		Close:         close,
		StorageReader: reader,
	}
}

func (r *StorageReader) ReadWindowAggregate(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	wr := r.StorageReader.(query.WindowAggregateReader)
	return wr.ReadWindowAggregate(ctx, spec, alloc)
}

func TestStorageReader_ReadWindowAggregate(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket influxdb.ID) (gen.SeriesGenerator, gen.TimeRange) {
		tagsSpec := &gen.TagsSpec{
			Tags: []*gen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() gen.CountableSequence {
						return gen.NewCounterByteSequence("a-%s", 0, 3)
					},
				},
			},
		}
		spec := gen.Spec{
			OrgID:    org,
			BucketID: bucket,
			Measurements: []gen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &gen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: gen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 10 * time.Second,
						},
						DataType: models.Float,
						Values: func(spec gen.TimeSequenceSpec) gen.TimeValuesSequence {
							return gen.NewTimeFloatValuesSequence(
								spec.Count,
								gen.NewTimestampSequenceFromSpec(spec),
								gen.NewFloatArrayValuesSequence([]float64{1.0, 2.0, 3.0, 4.0}),
							)
						},
					},
				},
			},
		}
		tr := gen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:02:00Z"),
		}
		return gen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		WindowEvery: int64(30 * time.Second),
		Aggregates: []plan.ProcedureKind{
			universe.CountKind,
		},
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	windowEvery := values.ConvertDuration(30 * time.Second)
	makeWindowTable := func(t0 string, start execute.Time, value interface{}) *executetest.Table {
		valueType := flux.ColumnType(values.New(value).Type())
		stop := start.Add(windowEvery)
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_value", Type: valueType},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, value, "f0", "m0", t0},
			},
		}
	}

	var want []*executetest.Table
	for _, t0 := range []string{"a-0", "a-1", "a-2"} {
		for i := 0; i < 4; i++ {
			offset := windowEvery.Mul(i)
			start := reader.Bounds.Start.Add(offset)
			want = append(want, makeWindowTable(t0, start, int64(3)))
		}
	}
	executetest.NormalizeTables(want)
	sort.Sort(executetest.SortedTables(want))

	var got []*executetest.Table
	if err := ti.Do(func(table flux.Table) error {
		t, err := executetest.ConvertTable(table)
		if err != nil {
			return err
		}
		got = append(got, t)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	executetest.NormalizeTables(got)
	sort.Sort(executetest.SortedTables(got))

	// compare these two
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected results -want/+got:\n%s", diff)
	}
}

func TestStorageReader_ReadWindowAggregate_CreateEmpty(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket influxdb.ID) (gen.SeriesGenerator, gen.TimeRange) {
		tagsSpec := &gen.TagsSpec{
			Tags: []*gen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() gen.CountableSequence {
						return gen.NewCounterByteSequence("a-%s", 0, 3)
					},
				},
			},
		}
		spec := gen.Spec{
			OrgID:    org,
			BucketID: bucket,
			Measurements: []gen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &gen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: gen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 15 * time.Second,
						},
						DataType: models.Float,
						Values: func(spec gen.TimeSequenceSpec) gen.TimeValuesSequence {
							return gen.NewTimeFloatValuesSequence(
								spec.Count,
								gen.NewTimestampSequenceFromSpec(spec),
								gen.NewFloatArrayValuesSequence([]float64{1.0, 2.0, 3.0, 4.0}),
							)
						},
					},
				},
			},
		}
		tr := gen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:02:00Z"),
		}
		return gen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		WindowEvery: int64(10 * time.Second),
		Aggregates: []plan.ProcedureKind{
			universe.CountKind,
		},
		CreateEmpty: true,
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	windowEvery := values.ConvertDuration(10 * time.Second)
	makeWindowTable := func(t0 string, start execute.Time, value interface{}, isNull bool) *executetest.Table {
		valueType := flux.ColumnType(values.New(value).Type())
		stop := start.Add(windowEvery)
		if isNull {
			value = nil
		}
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_value", Type: valueType},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, value, "f0", "m0", t0},
			},
		}
	}

	var want []*executetest.Table
	for _, t0 := range []string{"a-0", "a-1", "a-2"} {
		for i := 0; i < 12; i++ {
			offset := windowEvery.Mul(i)
			start := reader.Bounds.Start.Add(offset)
			isNull := (i+1)%3 == 0
			want = append(want, makeWindowTable(t0, start, int64(1), isNull))
		}
	}
	executetest.NormalizeTables(want)
	sort.Sort(executetest.SortedTables(want))

	var got []*executetest.Table
	if err := ti.Do(func(table flux.Table) error {
		t, err := executetest.ConvertTable(table)
		if err != nil {
			return err
		}
		got = append(got, t)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	executetest.NormalizeTables(got)
	sort.Sort(executetest.SortedTables(got))

	// compare these two
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected results -want/+got:\n%s", diff)
	}
}

func TestStorageReader_ReadWindowAggregate_TruncatedBounds(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket influxdb.ID) (gen.SeriesGenerator, gen.TimeRange) {
		tagsSpec := &gen.TagsSpec{
			Tags: []*gen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() gen.CountableSequence {
						return gen.NewCounterByteSequence("a-%s", 0, 3)
					},
				},
			},
		}
		spec := gen.Spec{
			OrgID:    org,
			BucketID: bucket,
			Measurements: []gen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &gen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: gen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 5 * time.Second,
						},
						DataType: models.Float,
						Values: func(spec gen.TimeSequenceSpec) gen.TimeValuesSequence {
							return gen.NewTimeFloatValuesSequence(
								spec.Count,
								gen.NewTimestampSequenceFromSpec(spec),
								gen.NewFloatArrayValuesSequence([]float64{1.0, 2.0, 3.0, 4.0}),
							)
						},
					},
				},
			},
		}
		tr := gen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return gen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds: execute.Bounds{
				Start: values.ConvertTime(mustParseTime("2019-11-25T00:00:05Z")),
				Stop:  values.ConvertTime(mustParseTime("2019-11-25T00:00:25Z")),
			},
		},
		WindowEvery: int64(10 * time.Second),
		Aggregates: []plan.ProcedureKind{
			universe.CountKind,
		},
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	makeWindowTable := func(t0 string, start, stop time.Duration, value interface{}) *executetest.Table {
		startT := reader.Bounds.Start.Add(values.ConvertDuration(start))
		stopT := reader.Bounds.Start.Add(values.ConvertDuration(stop))
		valueType := flux.ColumnType(values.New(value).Type())
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_value", Type: valueType},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{startT, stopT, value, "f0", "m0", t0},
			},
		}
	}

	var want []*executetest.Table
	for _, t0 := range []string{"a-0", "a-1", "a-2"} {
		want = append(want,
			makeWindowTable(t0, 5*time.Second, 10*time.Second, int64(1)),
			makeWindowTable(t0, 10*time.Second, 20*time.Second, int64(2)),
			makeWindowTable(t0, 20*time.Second, 25*time.Second, int64(1)),
		)
	}
	executetest.NormalizeTables(want)
	sort.Sort(executetest.SortedTables(want))

	var got []*executetest.Table
	if err := ti.Do(func(table flux.Table) error {
		t, err := executetest.ConvertTable(table)
		if err != nil {
			return err
		}
		got = append(got, t)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	executetest.NormalizeTables(got)
	sort.Sort(executetest.SortedTables(got))

	// compare these two
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected results -want/+got:\n%s", diff)
	}
}

func BenchmarkReadFilter(b *testing.B) {
	setupFn := func(org, bucket influxdb.ID) (gen.SeriesGenerator, gen.TimeRange) {
		tagsSpec := &gen.TagsSpec{
			Tags: []*gen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() gen.CountableSequence {
						return gen.NewCounterByteSequence("a-%s", 0, 5)
					},
				},
				{
					TagKey: "t1",
					Values: func() gen.CountableSequence {
						return gen.NewCounterByteSequence("b-%s", 0, 1000)
					},
				},
			},
		}
		spec := gen.Spec{
			OrgID:    org,
			BucketID: bucket,
			Measurements: []gen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &gen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: gen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec gen.TimeSequenceSpec) gen.TimeValuesSequence {
							r := rand.New(rand.NewSource(10))
							return gen.NewTimeFloatValuesSequence(
								spec.Count,
								gen.NewTimestampSequenceFromSpec(spec),
								gen.NewFloatRandomValuesSequence(0, 90, r),
							)
						},
					},
				},
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &gen.FieldValuesSpec{
						Name: "f1",
						TimeSequenceSpec: gen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec gen.TimeSequenceSpec) gen.TimeValuesSequence {
							r := rand.New(rand.NewSource(11))
							return gen.NewTimeFloatValuesSequence(
								spec.Count,
								gen.NewTimestampSequenceFromSpec(spec),
								gen.NewFloatRandomValuesSequence(0, 180, r),
							)
						},
					},
				},
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &gen.FieldValuesSpec{
						Name: "f1",
						TimeSequenceSpec: gen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec gen.TimeSequenceSpec) gen.TimeValuesSequence {
							r := rand.New(rand.NewSource(12))
							return gen.NewTimeFloatValuesSequence(
								spec.Count,
								gen.NewTimestampSequenceFromSpec(spec),
								gen.NewFloatRandomValuesSequence(10, 10000, r),
							)
						},
					},
				},
			},
		}
		tr := gen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-26T00:00:00Z"),
		}
		return gen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	}
	benchmarkRead(b, setupFn, func(r *StorageReader) error {
		mem := &memory.Allocator{}
		tables, err := r.ReadFilter(context.Background(), query.ReadFilterSpec{
			OrganizationID: r.Org,
			BucketID:       r.Bucket,
			Bounds:         r.Bounds,
		}, mem)
		if err != nil {
			return err
		}
		return tables.Do(func(table flux.Table) error {
			table.Done()
			return nil
		})
	})
}

func benchmarkRead(b *testing.B, setupFn SetupFunc, f func(r *StorageReader) error) {
	reader := NewStorageReader(b, setupFn)
	defer reader.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := f(reader); err != nil {
			b.Fatal(err)
		}
	}
}

func mustParseTime(s string) time.Time {
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return ts
}
