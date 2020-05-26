package storageflux_test

import (
	"context"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
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
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
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
	benchmarkRead(b, sg, func(r influxdb.Reader) error {
		mem := &memory.Allocator{}
		tables, err := r.ReadFilter(context.Background(), influxdb.ReadFilterSpec{
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

func benchmarkRead(b *testing.B, sg gen.SeriesGenerator, f func(r influxdb.Reader) error) {
	logger := zaptest.NewLogger(b)
	rootDir, err := ioutil.TempDir("", "storage-reads-test")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(rootDir) }()

	generator := generate.Generator{}
	if _, err := generator.Run(context.Background(), rootDir, sg); err != nil {
		b.Fatal(err)
	}

	enginePath := filepath.Join(rootDir, "engine")
	engine := storage.NewEngine(enginePath, storage.NewConfig())
	engine.WithLogger(logger)

	if err := engine.Open(context.Background()); err != nil {
		b.Fatal(err)
	}
	reader := storageflux.NewReader(readservice.NewStore(engine))

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
