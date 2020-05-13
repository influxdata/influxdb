package storageflux_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
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

func BenchmarkReadFilter(b *testing.B) {
	idgen := mock.NewMockIDGenerator()
	tagsSpec := &gen.TagsSpec{
		Tags: []*gen.TagValuesSpec{
			{
				TagKey: "t0",
				Values: func() gen.CountableSequence {
					return gen.NewCounterByteSequence("a-%d", 0, 5)
				},
			},
			{
				TagKey: "t1",
				Values: func() gen.CountableSequence {
					return gen.NewCounterByteSequence("b-%d", 0, 1000)
				},
			},
		},
	}
	spec := gen.Spec{
		OrgID:    idgen.ID(),
		BucketID: idgen.ID(),
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
	sg := gen.NewSeriesGeneratorFromSpec(&spec, tr)
	benchmarkRead(b, sg, func(r query.StorageReader) error {
		mem := &memory.Allocator{}
		tables, err := r.ReadFilter(context.Background(), query.ReadFilterSpec{
			OrganizationID: spec.OrgID,
			BucketID:       spec.BucketID,
			Bounds: execute.Bounds{
				Start: values.ConvertTime(tr.Start),
				Stop:  values.ConvertTime(tr.End),
			},
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

func BenchmarkReadFilterVsReadGroup(b *testing.B) {
	idgen := mock.NewMockIDGenerator()
	tagsSpec := &gen.TagsSpec{
		Tags: []*gen.TagValuesSpec{
			{
				TagKey: "t0",
				Values: func() gen.CountableSequence {
					return gen.NewCounterByteSequence("a-%d", 0, 5)
				},
			},
			{
				TagKey: "t1",
				Values: func() gen.CountableSequence {
					return gen.NewCounterByteSequence("b-%d", 0, 1000)
				},
			},
		},
	}
	spec := gen.Spec{
		OrgID:    idgen.ID(),
		BucketID: idgen.ID(),
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
		},
	}
	tr := gen.TimeRange{
		Start: mustParseTime("2019-11-25T00:00:00Z"),
		End:   mustParseTime("2019-11-26T00:00:00Z"),
	}
	sg := gen.NewSeriesGeneratorFromSpec(&spec, tr)

	testcases := []struct{
		name string
		readFilterSpec query.ReadFilterSpec
		readGroupSpec query.ReadGroupSpec
	}{
		{
			name:       "group by t0",
			readFilterSpec: query.ReadFilterSpec{
				OrganizationID: spec.OrgID,
				BucketID:       spec.BucketID,
			},
			readGroupSpec: query.ReadGroupSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: spec.OrgID,
					BucketID:       spec.BucketID,
				},
				GroupMode: query.GroupModeBy,
				GroupKeys: []string{"t0"},
				AggregateMethod: "COUNT",
			},
		},
		{
			name:       "group by t1",
			readFilterSpec: query.ReadFilterSpec{
				OrganizationID: spec.OrgID,
				BucketID:       spec.BucketID,
			},
			readGroupSpec: query.ReadGroupSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: spec.OrgID,
					BucketID:       spec.BucketID,
				},
				GroupMode: query.GroupModeBy,
				GroupKeys: []string{"t1"},
				AggregateMethod: "COUNT",
			},
		},
	}
	reader := NewTempStorageReader(b, sg)
	defer reader.Close()
	for _, tc := range testcases {
		// baseline
		b.Run(tc.name + "_filter", func(b *testing.B) {
			rf := tc.readFilterSpec
			rf.Bounds = execute.Bounds{
				Start: values.ConvertTime(tr.Start),
				Stop:  values.ConvertTime(tr.End),
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				mem := &memory.Allocator{}
				tables, err := reader.ReadFilter(context.Background(), tc.readFilterSpec, mem)
				if err != nil {
					b.Fatal(err)
				}
				if err := tables.Do(func(table flux.Table) error {
					table.Done()
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
		// Use manually windows calls to ReadGroup
		for _, numWindows := range []int{24, 240} {
			name := fmt.Sprintf("%s_group_%d_windows", tc.name, numWindows)
			b.Run(name, func(b *testing.B) {
				bounds := splitRange(tr, numWindows)
				rgs := make([]query.ReadGroupSpec, numWindows)
				for i, winBounds := range bounds {
					rg := tc.readGroupSpec
					rg.Bounds = winBounds
					rgs[i] = rg
				}

				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < 5; i++ {
					var wg sync.WaitGroup
					for _, rg := range rgs {
						rg := rg
						wg.Add(1)
						go func() {
							defer wg.Done()
							mem := &memory.Allocator{}
							tables, err := reader.ReadGroup(context.Background(), rg, mem)
							if err != nil {
								b.Fatal(err)
							}
							if err := tables.Do(func(table flux.Table) error {
								table.Done()
								return nil
							}); err != nil {
								b.Fatal(err)
							}
						}()
					}
					wg.Wait()
				}
			})
		}
	}
}

func splitRange(tr gen.TimeRange, n int) []execute.Bounds {
	bounds := make([]execute.Bounds, n)
	totalDur := tr.End.UnixNano() - tr.Start.UnixNano()
	dur := totalDur / int64(n)
	start := tr.Start.UnixNano()
	for i := 0; i < n; i++ {
		end := start + dur
		bounds[i] = execute.Bounds{
			Start: values.ConvertTime(time.Unix(0, start)),
			Stop: values.ConvertTime(time.Unix(0, end)),
		}
		start = end
	}
	return bounds
}

type TempStorageReader struct {
	query.StorageReader
	rootDir string
}

func (tsr *TempStorageReader) Close() {
	tsr.StorageReader.Close()
	_ = os.RemoveAll(tsr.rootDir)
}

func NewTempStorageReader(b *testing.B, sg gen.SeriesGenerator) *TempStorageReader {
	logger := zaptest.NewLogger(b)
	rootDir, err := ioutil.TempDir("", "storage-reads-test")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(rootDir) }()

	generator := generate.Generator{}
	if _, err := generator.Run(context.Background(), rootDir, sg); err != nil {
		_ = os.RemoveAll(rootDir)
		b.Fatal(err)
	}

	enginePath := filepath.Join(rootDir, "engine")
	engine := storage.NewEngine(enginePath, storage.NewConfig())
	engine.WithLogger(logger)

	if err := engine.Open(context.Background()); err != nil {
		_ = os.RemoveAll(rootDir)
		b.Fatal(err)
	}
	reader := &TempStorageReader{
		StorageReader: storageflux.NewReader(readservice.NewStore(engine)),
		rootDir: rootDir,
	}
	return reader
}

func benchmarkRead(b *testing.B, sg gen.SeriesGenerator, f func(r query.StorageReader) error) {
	reader := NewTempStorageReader(b, sg)
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
