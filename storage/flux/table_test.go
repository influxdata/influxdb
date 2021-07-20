package storageflux_test

import (
	"context"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	arrowmem "github.com/apache/arrow/go/arrow/memory"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/execute/table"
	"github.com/influxdata/flux/execute/table/static"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/internal/shard"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/models"
	datagen "github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	storageproto "github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	storagev1 "github.com/influxdata/influxdb/v2/v1/services/storage"
)

type SetupFunc func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange)

type StorageReader struct {
	Org    platform.ID
	Bucket platform.ID
	Bounds execute.Bounds
	Close  func()
	query.StorageReader
}

func NewStorageReader(tb testing.TB, setupFn SetupFunc) *StorageReader {
	rootDir, err := ioutil.TempDir("", "storage-flux-test")
	if err != nil {
		tb.Fatal(err)
	}

	var closers []io.Closer
	close := func() {
		for _, c := range closers {
			if err := c.Close(); err != nil {
				tb.Errorf("close error: %s", err)
			}
		}
		_ = os.RemoveAll(rootDir)
	}

	// Create an underlying kv store. We use the inmem version to speed
	// up test runs.
	kvStore := inmem.NewKVStore()

	// Manually create the meta bucket.
	// This seems to be the only bucket used for the read path.
	// If, in the future, there are any "bucket not found" errors due to
	// a change in the storage code, then this section of code will need
	// to be changed to correctly configure the kv store.
	// We do this abbreviated setup instead of a full migration because
	// the full migration is both unnecessary and long.
	if err := kvStore.CreateBucket(context.Background(), meta.BucketName); err != nil {
		close()
		tb.Fatalf("failed to create meta bucket: %s", err)
	}

	// Use this kv store for the meta client. The storage reader
	// uses the meta client for shard information.
	metaClient := meta.NewClient(meta.NewConfig(), kvStore)
	if err := metaClient.Open(); err != nil {
		close()
		tb.Fatalf("failed to open meta client: %s", err)
	}
	closers = append(closers, metaClient)

	// Create the organization and the bucket.
	idgen := mock.NewMockIDGenerator()
	org, bucket := idgen.ID(), idgen.ID()

	// Run the setup function to create the series generator.
	sg, tr := setupFn(org, bucket)

	// Construct a database with a retention policy.
	// This would normally be done by the storage bucket service, but the storage
	// bucket service requires us to already have a storage engine and a fully migrated
	// kv store. Since we don't have either of those, we add the metadata necessary
	// for the storage reader to function.
	// We construct the database with a retention policy that is a year long
	// so that we do not have to generate more than one shard which can get complicated.
	rp := &meta.RetentionPolicySpec{
		Name:               meta.DefaultRetentionPolicyName,
		ShardGroupDuration: 24 * 7 * time.Hour * 52,
	}
	if _, err := metaClient.CreateDatabaseWithRetentionPolicy(bucket.String(), rp); err != nil {
		close()
		tb.Fatalf("failed to create database: %s", err)
	}

	// Create the shard group for the data. There should only be one and
	// it should include the entire time range.
	sgi, err := metaClient.CreateShardGroup(bucket.String(), rp.Name, tr.Start)
	if err != nil {
		close()
		tb.Fatalf("failed to create shard group: %s", err)
	} else if sgi.StartTime.After(tr.Start) || sgi.EndTime.Before(tr.End) {
		close()
		tb.Fatal("shard data range exceeded the shard group range; please use a range for data that is within the same year")
	}

	// Open the series file and prepare the directory for the shard writer.
	enginePath := filepath.Join(rootDir, "engine")
	dbPath := filepath.Join(enginePath, "data", bucket.String())
	if err := os.MkdirAll(dbPath, 0700); err != nil {
		close()
		tb.Fatalf("failed to create data directory: %s", err)
	}

	sfile := tsdb.NewSeriesFile(filepath.Join(dbPath, tsdb.SeriesFileDirectory))
	if err := sfile.Open(); err != nil {
		close()
		tb.Fatalf("failed to open series file: %s", err)
	}
	// Ensure the series file is closed in case of failure.
	defer sfile.Close()
	// Disable compactions to speed up the shard writer.
	sfile.DisableCompactions()

	// Write the shard data.
	shardPath := filepath.Join(dbPath, rp.Name)
	if err := os.MkdirAll(filepath.Join(shardPath, strconv.FormatUint(sgi.Shards[0].ID, 10)), 0700); err != nil {
		close()
		tb.Fatalf("failed to create shard directory: %s", err)
	}
	if err := writeShard(sfile, sg, sgi.Shards[0].ID, shardPath); err != nil {
		close()
		tb.Fatalf("failed to write shard: %s", err)
	}

	// Run the partition compactor on the series file.
	for i, p := range sfile.Partitions() {
		c := tsdb.NewSeriesPartitionCompactor()
		if err := c.Compact(p); err != nil {
			close()
			tb.Fatalf("failed to compact series file %d: %s", i, err)
		}
	}

	// Close the series file as it will be opened by the storage engine.
	if err := sfile.Close(); err != nil {
		close()
		tb.Fatalf("failed to close series file: %s", err)
	}

	// Now load the engine.
	engine := storage.NewEngine(
		enginePath,
		storage.NewConfig(),
		storage.WithMetaClient(metaClient),
	)
	if err := engine.Open(context.Background()); err != nil {
		close()
		tb.Fatalf("failed to open storage engine: %s", err)
	}
	closers = append(closers, engine)

	store := storagev1.NewStore(engine.TSDBStore(), engine.MetaClient())
	reader := storageflux.NewReader(store)
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
	return r.StorageReader.ReadWindowAggregate(ctx, spec, alloc)
}

func TestStorageReader_ReadFilter(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:00:30Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	alloc := &memory.Allocator{
		Allocator: mem,
	}
	ti, err := reader.ReadFilter(context.Background(), query.ReadFilterSpec{
		OrganizationID: reader.Org,
		BucketID:       reader.Bucket,
		Bounds:         reader.Bounds,
	}, alloc)
	if err != nil {
		t.Fatal(err)
	}

	makeTable := func(t0 string) *executetest.Table {
		start, stop := reader.Bounds.Start, reader.Bounds.Stop
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TFloat},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, Time("2019-11-25T00:00:00Z"), 1.0, "f0", "m0", t0},
				{start, stop, Time("2019-11-25T00:00:10Z"), 2.0, "f0", "m0", t0},
				{start, stop, Time("2019-11-25T00:00:20Z"), 3.0, "f0", "m0", t0},
			},
		}
	}

	want := []*executetest.Table{
		makeTable("a-0"),
		makeTable("a-1"),
		makeTable("a-2"),
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

func TestStorageReader_Table(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:00:30Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tc := range []struct {
		name  string
		newFn func(ctx context.Context, alloc *memory.Allocator) flux.TableIterator
	}{
		{
			name: "ReadFilter",
			newFn: func(ctx context.Context, alloc *memory.Allocator) flux.TableIterator {
				ti, err := reader.ReadFilter(context.Background(), query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				}, alloc)
				if err != nil {
					t.Fatal(err)
				}
				return ti
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executetest.RunTableTests(t, executetest.TableTest{
				NewFn: tc.newFn,
				IsDone: func(table flux.Table) bool {
					return table.(interface {
						IsDone() bool
					}).IsDone()
				},
			})
		})
	}
}

func TestStorageReader_ReadWindowAggregate(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:02:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:30Z"),
							static.Ints("_value", 3),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:30Z"),
							static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
							static.Ints("_value", 3),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:01:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:01:30Z"),
							static.Ints("_value", 3),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:01:30Z"),
							static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
							static.Ints("_value", 3),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:30Z"),
							static.Times("_time", "2019-11-25T00:00:00Z"),
							static.Floats("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:30Z"),
							static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
							static.Times("_time", "2019-11-25T00:00:40Z"),
							static.Floats("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:01:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:01:30Z"),
							static.Times("_time", "2019-11-25T00:01:20Z"),
							static.Floats("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:01:30Z"),
							static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
							static.Times("_time", "2019-11-25T00:01:30Z"),
							static.Floats("_value", 2),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:30Z"),
							static.Times("_time", "2019-11-25T00:00:20Z"),
							static.Floats("_value", 3),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:30Z"),
							static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
							static.Times("_time", "2019-11-25T00:00:30Z"),
							static.Floats("_value", 4),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:01:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:01:30Z"),
							static.Times("_time", "2019-11-25T00:01:10Z"),
							static.Floats("_value", 4),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:01:30Z"),
							static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
							static.Times("_time", "2019-11-25T00:01:50Z"),
							static.Floats("_value", 4),
						},
					},
				},
			},
		},
	} {
		t.Run(string(tt.aggregate), func(t *testing.T) {
			mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			alloc := &memory.Allocator{
				Allocator: mem,
			}
			got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				},
				Window: execute.Window{
					Every:  flux.ConvertDuration(30 * time.Second),
					Period: flux.ConvertDuration(30 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{
					tt.aggregate,
				},
			}, alloc)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Fatalf("unexpected output -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregate_ByStopTime(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:02:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:30Z", 30, 60, 90),
							static.Ints("_value", 3, 3, 3, 3),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:30Z", 30, 60, 90),
							static.Floats("_value", 1, 1, 1, 2),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:30Z", 30, 60, 90),
							static.Floats("_value", 3, 4, 4, 4),
						},
					},
				},
			},
		},
	} {
		mem := &memory.Allocator{}
		got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: reader.Org,
				BucketID:       reader.Bucket,
				Bounds:         reader.Bounds,
			},
			TimeColumn: execute.DefaultStopColLabel,
			Window: execute.Window{
				Every:  flux.ConvertDuration(30 * time.Second),
				Period: flux.ConvertDuration(30 * time.Second),
			},
			Aggregates: []plan.ProcedureKind{
				tt.aggregate,
			},
		}, mem)
		if err != nil {
			t.Fatal(err)
		}

		if diff := table.Diff(tt.want, got); diff != "" {
			t.Errorf("unexpected results -want/+got:\n%s", diff)
		}
	}
}

func TestStorageReader_ReadWindowAggregate_ByStartTime(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:02:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z", 30, 60, 90),
							static.Ints("_value", 3, 3, 3, 3),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z", 30, 60, 90),
							static.Floats("_value", 1, 1, 1, 2),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z", 30, 60, 90),
							static.Floats("_value", 3, 4, 4, 4),
						},
					},
				},
			},
		},
	} {
		t.Run(string(tt.aggregate), func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				},
				TimeColumn: execute.DefaultStartColLabel,
				Window: execute.Window{
					Every:  flux.ConvertDuration(30 * time.Second),
					Period: flux.ConvertDuration(30 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{
					tt.aggregate,
				},
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Fatalf("unexpected output -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregate_CreateEmpty(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 15*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:01:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
							static.Ints("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:10Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
							static.Ints("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:20Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:30Z"),
							static.Ints("_value", 0),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:30Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:40Z"),
							static.Ints("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:40Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:50Z"),
							static.Ints("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:50Z"),
							static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
							static.Ints("_value", 0),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
							static.Times("_time", "2019-11-25T00:00:00Z"),
							static.Floats("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:10Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
							static.Times("_time", "2019-11-25T00:00:15Z"),
							static.Floats("_value", 2),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:20Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:30Z"),
							static.Times("_time"),
							static.Floats("_value"),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:30Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:40Z"),
							static.Times("_time", "2019-11-25T00:00:30Z"),
							static.Floats("_value", 3),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:40Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:50Z"),
							static.Times("_time", "2019-11-25T00:00:45Z"),
							static.Floats("_value", 4),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:50Z"),
							static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
							static.Times("_time"),
							static.Floats("_value"),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
							static.Times("_time", "2019-11-25T00:00:00Z"),
							static.Floats("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:10Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
							static.Times("_time", "2019-11-25T00:00:15Z"),
							static.Floats("_value", 2),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:20Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:30Z"),
							static.Times("_time"),
							static.Floats("_value"),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:30Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:40Z"),
							static.Times("_time", "2019-11-25T00:00:30Z"),
							static.Floats("_value", 3),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:40Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:50Z"),
							static.Times("_time", "2019-11-25T00:00:45Z"),
							static.Floats("_value", 4),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:50Z"),
							static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
							static.Times("_time"),
							static.Floats("_value"),
						},
					},
				},
			},
		},
	} {
		t.Run(string(tt.aggregate), func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				},
				Window: execute.Window{
					Every:  flux.ConvertDuration(10 * time.Second),
					Period: flux.ConvertDuration(10 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{
					tt.aggregate,
				},
				CreateEmpty: true,
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Fatalf("unexpected output -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregate_CreateEmptyByStopTime(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 15*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:01:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:10Z", 10, 20, 30, 40, 50),
							static.Ints("_value", 1, 1, 0, 1, 1, 0),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:10Z", 10, 30, 40),
							static.Floats("_value", 1, 2, 3, 4),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:10Z", 10, 30, 40),
							static.Floats("_value", 1, 2, 3, 4),
						},
					},
				},
			},
		},
	} {
		t.Run(string(tt.aggregate), func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				},
				TimeColumn: execute.DefaultStopColLabel,
				Window: execute.Window{
					Every:  flux.ConvertDuration(10 * time.Second),
					Period: flux.ConvertDuration(10 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{
					tt.aggregate,
				},
				CreateEmpty: true,
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Errorf("unexpected results -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregate_CreateEmptyByStartTime(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 15*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:01:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z", 10, 20, 30, 40, 50),
							static.Ints("_value", 1, 1, 0, 1, 1, 0),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z", 10, 30, 40),
							static.Floats("_value", 1, 2, 3, 4),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z", 10, 30, 40),
							static.Floats("_value", 1, 2, 3, 4),
						},
					},
				},
			},
		},
	} {
		t.Run(string(tt.aggregate), func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				},
				TimeColumn: execute.DefaultStartColLabel,
				Window: execute.Window{
					Every:  flux.ConvertDuration(10 * time.Second),
					Period: flux.ConvertDuration(10 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{
					tt.aggregate,
				},
				CreateEmpty: true,
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Errorf("unexpected results -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregate_CreateEmptyAggregateByStopTime(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 15*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:01:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:10Z", 10, 20, 30, 40, 50),
							static.Ints("_value", 1, 1, 0, 1, 1, 0),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:10Z", 10, 20, 30, 40, 50),
							static.Floats("_value", 1, 2, nil, 3, 4, nil),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:10Z", 10, 20, 30, 40, 50),
							static.Floats("_value", 1, 2, nil, 3, 4, nil),
						},
					},
				},
			},
		},
	} {
		t.Run(string(tt.aggregate), func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				},
				TimeColumn: execute.DefaultStopColLabel,
				Window: execute.Window{
					Every:  flux.ConvertDuration(10 * time.Second),
					Period: flux.ConvertDuration(10 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{
					tt.aggregate,
				},
				CreateEmpty:    true,
				ForceAggregate: true,
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Errorf("unexpected results -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregate_CreateEmptyAggregateByStartTime(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 15*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:01:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z", 10, 20, 30, 40, 50),
							static.Ints("_value", 1, 1, 0, 1, 1, 0),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z", 10, 20, 30, 40, 50),
							static.Floats("_value", 1, 2, nil, 3, 4, nil),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z", 10, 20, 30, 40, 50),
							static.Floats("_value", 1, 2, nil, 3, 4, nil),
						},
					},
				},
			},
		},
	} {
		t.Run(string(tt.aggregate), func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				},
				TimeColumn: execute.DefaultStartColLabel,
				Window: execute.Window{
					Every:  flux.ConvertDuration(10 * time.Second),
					Period: flux.ConvertDuration(10 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{
					tt.aggregate,
				},
				CreateEmpty:    true,
				ForceAggregate: true,
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Errorf("unexpected results -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregate_TruncatedBounds(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 5*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:01:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:05Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
							static.Ints("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:10Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
							static.Ints("_value", 2),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:20Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:25Z"),
							static.Ints("_value", 1),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:05Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
							static.Times("_time", "2019-11-25T00:00:05Z"),
							static.Floats("_value", 2),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:10Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
							static.Times("_time", "2019-11-25T00:00:10Z"),
							static.Floats("_value", 3),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:20Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:25Z"),
							static.Times("_time", "2019-11-25T00:00:20Z"),
							static.Floats("_value", 1),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:05Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
							static.Times("_time", "2019-11-25T00:00:05Z"),
							static.Floats("_value", 2),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:10Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
							static.Times("_time", "2019-11-25T00:00:15Z"),
							static.Floats("_value", 4),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:20Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:25Z"),
							static.Times("_time", "2019-11-25T00:00:20Z"),
							static.Floats("_value", 1),
						},
					},
				},
			},
		},
	} {
		t.Run(string(tt.aggregate), func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds: execute.Bounds{
						Start: values.ConvertTime(mustParseTime("2019-11-25T00:00:05Z")),
						Stop:  values.ConvertTime(mustParseTime("2019-11-25T00:00:25Z")),
					},
				},
				Window: execute.Window{
					Every:  flux.ConvertDuration(10 * time.Second),
					Period: flux.ConvertDuration(10 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{
					tt.aggregate,
				},
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Errorf("unexpected results -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregate_TruncatedBoundsCreateEmpty(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 15*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:01:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:05Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
							static.Ints("_value", 0),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:10Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
							static.Ints("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:20Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:25Z"),
							static.Ints("_value", 0),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:05Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
							static.Times("_time"),
							static.Floats("_value"),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:10Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
							static.Times("_time", "2019-11-25T00:00:15Z"),
							static.Floats("_value", 2),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:20Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:25Z"),
							static.Times("_time"),
							static.Floats("_value"),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:05Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
							static.Times("_time"),
							static.Floats("_value"),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:10Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
							static.Times("_time", "2019-11-25T00:00:15Z"),
							static.Floats("_value", 2),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-25T00:00:20Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:25Z"),
							static.Times("_time"),
							static.Floats("_value"),
						},
					},
				},
			},
		},
	} {
		t.Run(string(tt.aggregate), func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds: execute.Bounds{
						Start: values.ConvertTime(mustParseTime("2019-11-25T00:00:05Z")),
						Stop:  values.ConvertTime(mustParseTime("2019-11-25T00:00:25Z")),
					},
				},
				Window: execute.Window{
					Every:  flux.ConvertDuration(10 * time.Second),
					Period: flux.ConvertDuration(10 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{
					tt.aggregate,
				},
				CreateEmpty: true,
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Errorf("unexpected results -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregate_Mean(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 5 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2, 3, 4}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	t.Run("unwindowed mean", func(t *testing.T) {
		mem := &memory.Allocator{}
		ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: reader.Org,
				BucketID:       reader.Bucket,
				Bounds:         reader.Bounds,
			},
			Window: execute.Window{
				Every:  flux.ConvertDuration(math.MaxInt64 * time.Nanosecond),
				Period: flux.ConvertDuration(math.MaxInt64 * time.Nanosecond),
			},
			Aggregates: []plan.ProcedureKind{
				storageflux.MeanKind,
			},
		}, mem)
		if err != nil {
			t.Fatal(err)
		}

		want := static.Table{
			static.StringKey("_measurement", "m0"),
			static.StringKey("_field", "f0"),
			static.StringKey("t0", "a0"),
			static.TimeKey("_start", "2019-11-25T00:00:00Z"),
			static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
			static.Floats("_value", 2.5),
		}
		if diff := table.Diff(want, ti); diff != "" {
			t.Fatalf("table iterators do not match; -want/+got:\n%s", diff)
		}
	})

	t.Run("windowed mean", func(t *testing.T) {
		mem := &memory.Allocator{}
		ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: reader.Org,
				BucketID:       reader.Bucket,
				Bounds:         reader.Bounds,
			},
			Window: execute.Window{
				Every:  flux.ConvertDuration(10 * time.Second),
				Period: flux.ConvertDuration(10 * time.Second),
			},
			Aggregates: []plan.ProcedureKind{
				storageflux.MeanKind,
			},
		}, mem)
		if err != nil {
			t.Fatal(err)
		}

		want := static.TableGroup{
			static.StringKey("_measurement", "m0"),
			static.StringKey("_field", "f0"),
			static.StringKey("t0", "a0"),
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:10Z"),
				static.Floats("_value", 1.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:10Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:20Z"),
				static.Floats("_value", 3.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:20Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:30Z"),
				static.Floats("_value", 1.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:30Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:40Z"),
				static.Floats("_value", 3.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:40Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:50Z"),
				static.Floats("_value", 1.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:50Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.Floats("_value", 3.5),
			},
		}
		if diff := table.Diff(want, ti); diff != "" {
			t.Fatalf("table iterators do not match; -want/+got:\n%s", diff)
		}
	})

	t.Run("windowed mean with offset", func(t *testing.T) {
		mem := &memory.Allocator{}
		ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: reader.Org,
				BucketID:       reader.Bucket,
				Bounds:         reader.Bounds,
			},
			Window: execute.Window{
				Every:  flux.ConvertDuration(10 * time.Second),
				Period: flux.ConvertDuration(10 * time.Second),
				Offset: flux.ConvertDuration(2 * time.Second),
			},
			Aggregates: []plan.ProcedureKind{
				storageflux.MeanKind,
			},
		}, mem)
		if err != nil {
			t.Fatal(err)
		}

		want := static.TableGroup{
			static.StringKey("_measurement", "m0"),
			static.StringKey("_field", "f0"),
			static.StringKey("t0", "a0"),
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:02Z"),
				static.Floats("_value", 1.0),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:02Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:12Z"),
				static.Floats("_value", 2.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:12Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:22Z"),
				static.Floats("_value", 2.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:22Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:32Z"),
				static.Floats("_value", 2.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:32Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:42Z"),
				static.Floats("_value", 2.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:42Z"),
				static.TimeKey("_stop", "2019-11-25T00:00:52Z"),
				static.Floats("_value", 2.5),
			},
			static.Table{
				static.TimeKey("_start", "2019-11-25T00:00:52Z"),
				static.TimeKey("_stop", "2019-11-25T00:01:00Z"),
				static.Floats("_value", 4),
			},
		}
		if diff := table.Diff(want, ti); diff != "" {
			t.Fatalf("table iterators do not match; -want/+got:\n%s", diff)
		}
	})
}

func TestStorageReader_ReadWindowFirst(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 5 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2, 3, 4}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.FirstKind,
		},
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	makeWindowTable := func(start, stop, time values.Time, v int64) *executetest.Table {
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, time, v, "f0", "m0", "a0"},
			},
		}
	}
	want := []*executetest.Table{
		makeWindowTable(Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:00:10Z"), Time("2019-11-25T00:00:00Z"), 1),
		makeWindowTable(Time("2019-11-25T00:00:10Z"), Time("2019-11-25T00:00:20Z"), Time("2019-11-25T00:00:10Z"), 3),
		makeWindowTable(Time("2019-11-25T00:00:20Z"), Time("2019-11-25T00:00:30Z"), Time("2019-11-25T00:00:20Z"), 1),
		makeWindowTable(Time("2019-11-25T00:00:30Z"), Time("2019-11-25T00:00:40Z"), Time("2019-11-25T00:00:30Z"), 3),
		makeWindowTable(Time("2019-11-25T00:00:40Z"), Time("2019-11-25T00:00:50Z"), Time("2019-11-25T00:00:40Z"), 1),
		makeWindowTable(Time("2019-11-25T00:00:50Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:50Z"), 3),
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

func TestStorageReader_WindowFirstOffset(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 5 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2, 3, 4}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
			Offset: flux.ConvertDuration(5 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.FirstKind,
		},
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	makeWindowTable := func(start, stop, time values.Time, v int64) *executetest.Table {
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, time, v, "f0", "m0", "a0"},
			},
		}
	}
	want := []*executetest.Table{
		makeWindowTable(Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:00:05Z"), Time("2019-11-25T00:00:00Z"), 1),
		makeWindowTable(Time("2019-11-25T00:00:05Z"), Time("2019-11-25T00:00:15Z"), Time("2019-11-25T00:00:05Z"), 2),
		makeWindowTable(Time("2019-11-25T00:00:15Z"), Time("2019-11-25T00:00:25Z"), Time("2019-11-25T00:00:15Z"), 4),
		makeWindowTable(Time("2019-11-25T00:00:25Z"), Time("2019-11-25T00:00:35Z"), Time("2019-11-25T00:00:25Z"), 2),
		makeWindowTable(Time("2019-11-25T00:00:35Z"), Time("2019-11-25T00:00:45Z"), Time("2019-11-25T00:00:35Z"), 4),
		makeWindowTable(Time("2019-11-25T00:00:45Z"), Time("2019-11-25T00:00:55Z"), Time("2019-11-25T00:00:45Z"), 2),
		makeWindowTable(Time("2019-11-25T00:00:55Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:55Z"), 4),
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

func TestStorageReader_WindowSumOffset(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 5 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2, 3, 4}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
			Offset: flux.ConvertDuration(2 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.SumKind,
		},
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	makeWindowTable := func(start, stop values.Time, v int64) *executetest.Table {
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, v, "f0", "m0", "a0"},
			},
		}
	}
	want := []*executetest.Table{
		makeWindowTable(Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:00:02Z"), 1),
		makeWindowTable(Time("2019-11-25T00:00:02Z"), Time("2019-11-25T00:00:12Z"), 5),
		makeWindowTable(Time("2019-11-25T00:00:12Z"), Time("2019-11-25T00:00:22Z"), 5),
		makeWindowTable(Time("2019-11-25T00:00:22Z"), Time("2019-11-25T00:00:32Z"), 5),
		makeWindowTable(Time("2019-11-25T00:00:32Z"), Time("2019-11-25T00:00:42Z"), 5),
		makeWindowTable(Time("2019-11-25T00:00:42Z"), Time("2019-11-25T00:00:52Z"), 5),
		makeWindowTable(Time("2019-11-25T00:00:52Z"), Time("2019-11-25T00:01:00Z"), 4),
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

func TestStorageReader_ReadWindowFirstCreateEmpty(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 20 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.FirstKind,
		},
		CreateEmpty: true,
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	makeEmptyTable := func(start, stop values.Time) *executetest.Table {
		return &executetest.Table{
			KeyCols:   []string{"_start", "_stop", "_field", "_measurement", "t0"},
			KeyValues: []interface{}{start, stop, "f0", "m0", "a0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: nil,
		}
	}
	makeWindowTable := func(start, stop, time values.Time, v int64) *executetest.Table {
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, time, v, "f0", "m0", "a0"},
			},
		}
	}
	want := []*executetest.Table{
		makeWindowTable(
			Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:00:10Z"), Time("2019-11-25T00:00:00Z"), 1,
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:10Z"), Time("2019-11-25T00:00:20Z"),
		),
		makeWindowTable(
			Time("2019-11-25T00:00:20Z"), Time("2019-11-25T00:00:30Z"), Time("2019-11-25T00:00:20Z"), 2,
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:30Z"), Time("2019-11-25T00:00:40Z"),
		),
		makeWindowTable(
			Time("2019-11-25T00:00:40Z"), Time("2019-11-25T00:00:50Z"), Time("2019-11-25T00:00:40Z"), 1,
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:50Z"), Time("2019-11-25T00:01:00Z"),
		),
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

func TestStorageReader_WindowFirstOffsetCreateEmpty(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 20 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
			Offset: flux.ConvertDuration(5 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.FirstKind,
		},
		CreateEmpty: true,
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	makeEmptyTable := func(start, stop values.Time) *executetest.Table {
		return &executetest.Table{
			KeyCols:   []string{"_start", "_stop", "_field", "_measurement", "t0"},
			KeyValues: []interface{}{start, stop, "f0", "m0", "a0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: nil,
		}
	}
	makeWindowTable := func(start, stop, time values.Time, v int64) *executetest.Table {
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, time, v, "f0", "m0", "a0"},
			},
		}
	}
	want := []*executetest.Table{
		makeWindowTable(
			Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:00:05Z"), Time("2019-11-25T00:00:00Z"), 1,
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:05Z"), Time("2019-11-25T00:00:15Z"),
		),
		makeWindowTable(
			Time("2019-11-25T00:00:15Z"), Time("2019-11-25T00:00:25Z"), Time("2019-11-25T00:00:20Z"), 2,
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:25Z"), Time("2019-11-25T00:00:35Z"),
		),
		makeWindowTable(
			Time("2019-11-25T00:00:35Z"), Time("2019-11-25T00:00:45Z"), Time("2019-11-25T00:00:40Z"), 1,
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:45Z"), Time("2019-11-25T00:00:55Z"),
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:55Z"), Time("2019-11-25T00:01:00Z"),
		),
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

func TestStorageReader_WindowSumOffsetCreateEmpty(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 20 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
			Offset: flux.ConvertDuration(5 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.SumKind,
		},
		CreateEmpty: true,
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	makeEmptyTable := func(start, stop values.Time) *executetest.Table {
		return &executetest.Table{
			KeyCols:   []string{"_start", "_stop", "_field", "_measurement", "t0"},
			KeyValues: []interface{}{start, stop, "f0", "m0", "a0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, nil, "f0", "m0", "a0"},
			},
		}
	}
	makeWindowTable := func(start, stop values.Time, v int64) *executetest.Table {
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, v, "f0", "m0", "a0"},
			},
		}
	}
	want := []*executetest.Table{
		makeWindowTable(
			Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:00:05Z"), 1,
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:05Z"), Time("2019-11-25T00:00:15Z"),
		),
		makeWindowTable(
			Time("2019-11-25T00:00:15Z"), Time("2019-11-25T00:00:25Z"), 2,
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:25Z"), Time("2019-11-25T00:00:35Z"),
		),
		makeWindowTable(
			Time("2019-11-25T00:00:35Z"), Time("2019-11-25T00:00:45Z"), 1,
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:45Z"), Time("2019-11-25T00:00:55Z"),
		),
		makeEmptyTable(
			Time("2019-11-25T00:00:55Z"), Time("2019-11-25T00:01:00Z"),
		),
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

func TestStorageReader_ReadWindowFirstTimeColumn(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 20 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.FirstKind,
		},
		CreateEmpty: true,
		TimeColumn:  execute.DefaultStopColLabel,
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	want := []*executetest.Table{{
		KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
		ColMeta: []flux.ColMeta{
			{Label: "_start", Type: flux.TTime},
			{Label: "_stop", Type: flux.TTime},
			{Label: "_time", Type: flux.TTime},
			{Label: "_value", Type: flux.TInt},
			{Label: "_field", Type: flux.TString},
			{Label: "_measurement", Type: flux.TString},
			{Label: "t0", Type: flux.TString},
		},
		Data: [][]interface{}{
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:10Z"), int64(1), "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:30Z"), int64(2), "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:50Z"), int64(1), "f0", "m0", "a0"},
		},
	}}

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

func TestStorageReader_WindowFirstOffsetTimeColumn(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 20 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
			Offset: flux.ConvertDuration(18 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.FirstKind,
		},
		CreateEmpty: true,
		TimeColumn:  execute.DefaultStopColLabel,
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	want := []*executetest.Table{{
		KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
		ColMeta: []flux.ColMeta{
			{Label: "_start", Type: flux.TTime},
			{Label: "_stop", Type: flux.TTime},
			{Label: "_time", Type: flux.TTime},
			{Label: "_value", Type: flux.TInt},
			{Label: "_field", Type: flux.TString},
			{Label: "_measurement", Type: flux.TString},
			{Label: "t0", Type: flux.TString},
		},
		Data: [][]interface{}{
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:08Z"), int64(1), "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:28Z"), int64(2), "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:48Z"), int64(1), "f0", "m0", "a0"},
		},
	}}

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

func TestStorageReader_WindowSumOffsetTimeColumn(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 20 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1, 2}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:01:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
			Offset: flux.ConvertDuration(18 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.SumKind,
		},
		CreateEmpty: true,
		TimeColumn:  execute.DefaultStopColLabel,
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	want := []*executetest.Table{{
		KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
		ColMeta: []flux.ColMeta{
			{Label: "_start", Type: flux.TTime},
			{Label: "_stop", Type: flux.TTime},
			{Label: "_time", Type: flux.TTime},
			{Label: "_value", Type: flux.TInt},
			{Label: "_field", Type: flux.TString},
			{Label: "_measurement", Type: flux.TString},
			{Label: "t0", Type: flux.TString},
		},
		Data: [][]interface{}{
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:08Z"), int64(1), "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:18Z"), nil, "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:28Z"), int64(2), "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:38Z"), nil, "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:48Z"), int64(1), "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:00:58Z"), nil, "f0", "m0", "a0"},
			{Time("2019-11-25T00:00:00Z"), Time("2019-11-25T00:01:00Z"), Time("2019-11-25T00:01:00Z"), nil, "f0", "m0", "a0"},
		},
	}}

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

func TestStorageReader_EmptyTableNoEmptyWindows(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a%s", 0, 1)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: 10 * time.Second,
						},
						DataType: models.Integer,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							return datagen.NewTimeIntegerValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewIntegerArrayValuesSequence([]int64{1}),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:10Z"),
			End:   mustParseTime("2019-11-25T00:00:30Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	})
	defer reader.Close()

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
		ReadFilterSpec: query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		},
		Window: execute.Window{
			Every:  flux.ConvertDuration(10 * time.Second),
			Period: flux.ConvertDuration(10 * time.Second),
		},
		Aggregates: []plan.ProcedureKind{
			storageflux.FirstKind,
		},
		CreateEmpty: true,
	}, mem)
	if err != nil {
		t.Fatal(err)
	}

	makeWindowTable := func(start, stop, time values.Time, v int64) *executetest.Table {
		return &executetest.Table{
			KeyCols: []string{"_start", "_stop", "_field", "_measurement", "t0"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TInt},
				{Label: "_field", Type: flux.TString},
				{Label: "_measurement", Type: flux.TString},
				{Label: "t0", Type: flux.TString},
			},
			Data: [][]interface{}{
				{start, stop, time, v, "f0", "m0", "a0"},
			},
		}
	}
	want := []*executetest.Table{
		makeWindowTable(
			Time("2019-11-25T00:00:10Z"), Time("2019-11-25T00:00:20Z"), Time("2019-11-25T00:00:10Z"), 1,
		),
		makeWindowTable(
			Time("2019-11-25T00:00:20Z"), Time("2019-11-25T00:00:30Z"), Time("2019-11-25T00:00:20Z"), 1,
		),
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

func getStorageEqPred(lhsTagKey, rhsTagValue string) *storageproto.Predicate {
	return &storageproto.Predicate{
		Root: &storageproto.Node{
			NodeType: storageproto.NodeTypeComparisonExpression,
			Value: &storageproto.Node_Comparison_{
				Comparison: storageproto.ComparisonEqual,
			},
			Children: []*storageproto.Node{
				{
					NodeType: storageproto.NodeTypeTagRef,
					Value: &storageproto.Node_TagRefValue{
						TagRefValue: lhsTagKey,
					},
				},
				{
					NodeType: storageproto.NodeTypeLiteral,
					Value: &storageproto.Node_StringValue{
						StringValue: rhsTagValue,
					},
				},
			},
		},
	}
}

func TestStorageReader_ReadGroup(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:02:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate string
		filter    *storageproto.Predicate
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Ints("_value", 12),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.SumKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Floats("_value", 30),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.SumKind,
			filter:    getStorageEqPred("t0", "z-9"),
			want:      static.TableGroup{},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z"),
							static.Floats("_value", 1),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:30Z"),
							static.Floats("_value", 4),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			filter:    getStorageEqPred("t0", "z-9"),
			want:      static.TableGroup{},
		},
		{
			aggregate: storageflux.FirstKind,
			want: static.TableGroup {
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:00:00Z"),
							static.Floats("_value", 1),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.LastKind,
			want: static.TableGroup {
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Times("_time", "2019-11-25T00:01:50Z"),
							static.Floats("_value", 4),
						},
					},
				},
			},
		},
	} {
		t.Run(tt.aggregate, func(t *testing.T) {
			mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			alloc := &memory.Allocator{
				Allocator: mem,
			}
			got, err := reader.ReadGroup(context.Background(), query.ReadGroupSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
					Predicate:      tt.filter,
				},
				GroupMode:       query.GroupModeBy,
				GroupKeys:       []string{"_measurement", "_field", "t0"},
				AggregateMethod: tt.aggregate,
			}, alloc)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Errorf("unexpected results -want/+got:\n%s", diff)
			}
		})
	}
}

// TestStorageReader_ReadGroupSelectTags exercises group-selects where the tag
// values vary among the candidate items for select and the read-group
// operation must track and return the correct set of tags.
func TestStorageReader_ReadGroupSelectTags(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
				TagValuesSequence("t1", "b-%s", 0, 1),
			),
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{5.0, 6.0, 7.0, 8.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
				TagValuesSequence("t1", "b-%s", 1, 2),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:02:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	cases := []struct {
		aggregate string
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Strings("t1", "b-0"),
							static.Strings("_measurement", "m0"),
							static.Strings("_field", "f0"),
							static.Times("_time", "2019-11-25T00:00:00Z"),
							static.Floats("_value", 1.0),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Strings("t1", "b-1"),
							static.Strings("_measurement", "m0"),
							static.Strings("_field", "f0"),
							static.Times("_time", "2019-11-25T00:00:30Z"),
							static.Floats("_value", 8.0),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.FirstKind,
			want: static.TableGroup{
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Strings("t1", "b-0"),
							static.Strings("_measurement", "m0"),
							static.Strings("_field", "f0"),
							static.Times("_time", "2019-11-25T00:00:00Z"),
							static.Floats("_value", 1.0),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.LastKind,
			want: static.TableGroup{
				static.TimeKey("_start", "2019-11-25T00:00:00Z"),
				static.TimeKey("_stop", "2019-11-25T00:02:00Z"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.Strings("t1", "b-1"),
							static.Strings("_measurement", "m0"),
							static.Strings("_field", "f0"),
							static.Times("_time", "2019-11-25T00:01:50Z"),
							static.Floats("_value", 8.0),
						},
					},
				},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.aggregate, func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadGroup(context.Background(), query.ReadGroupSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				},
				GroupMode:       query.GroupModeBy,
				GroupKeys:       []string{"t0"},
				AggregateMethod: tt.aggregate,
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Errorf("unexpected results -want/+got:\n%s", diff)
			}
		})
	}
}

// TestStorageReader_ReadGroupNoAgg exercises the path where no aggregate is specified
func TestStorageReader_ReadGroupNoAgg(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t1", "b-%s", 0, 2),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T00:00:40Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	cases := []struct {
		aggregate string
		want      flux.TableIterator
	}{
		{
			want: static.TableGroup{
				static.TableMatrix{
					{
						static.Table{
							static.StringKey("t1", "b-0"),
							static.Strings("_measurement", "m0", "m0", "m0", "m0"),
							static.Strings("_field", "f0", "f0", "f0", "f0"),
							static.TimeKey("_start", "2019-11-25T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:40Z"),
							static.Times("_time", "2019-11-25T00:00:00Z", "2019-11-25T00:00:10Z", "2019-11-25T00:00:20Z", "2019-11-25T00:00:30Z"),
							static.Floats("_value", 1.0, 2.0, 3.0, 4.0),
						},
					},
					{
						static.Table{
							static.StringKey("t1", "b-1"),
							static.Strings("_measurement", "m0", "m0", "m0", "m0"),
							static.Strings("_field", "f0", "f0", "f0", "f0"),
							static.TimeKey("_start", "2019-11-25T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-25T00:00:40Z"),
							static.Times("_time", "2019-11-25T00:00:00Z", "2019-11-25T00:00:10Z", "2019-11-25T00:00:20Z", "2019-11-25T00:00:30Z"),
							static.Floats("_value", 1.0, 2.0, 3.0, 4.0),
						},
					},
				},
			},
		},
	}

	for _, tt := range cases {
		t.Run("", func(t *testing.T) {
			mem := &memory.Allocator{}
			got, err := reader.ReadGroup(context.Background(), query.ReadGroupSpec{
				ReadFilterSpec: query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				},
				GroupMode: query.GroupModeBy,
				GroupKeys: []string{"t1"},
			}, mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := table.Diff(tt.want, got); diff != "" {
				t.Errorf("unexpected results -want/+got:\n%s", diff)
			}
		})
	}
}

func TestStorageReader_ReadWindowAggregateMonths(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 24*time.Hour, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-09-01T00:00:00Z", "2019-12-01T00:00:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		aggregate plan.ProcedureKind
		want      flux.TableIterator
	}{
		{
			aggregate: storageflux.CountKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-09-01T00:00:00Z"),
							static.TimeKey("_stop", "2019-10-01T00:00:00Z"),
							static.Ints("_value", 30),
						},
					},
					{
						static.Table{
							static.TimeKey("_start", "2019-10-01T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-01T00:00:00Z"),
							static.Ints("_value", 31),
						},
					},
					{
						static.Table{
							static.TimeKey("_start", "2019-11-01T00:00:00Z"),
							static.TimeKey("_stop", "2019-12-01T00:00:00Z"),
							static.Ints("_value", 30),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MinKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-09-01T00:00:00Z"),
							static.TimeKey("_stop", "2019-10-01T00:00:00Z"),
							static.Times("_time", "2019-09-01T00:00:00Z"),
							static.Floats("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-10-01T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-01T00:00:00Z"),
							static.Times("_time", "2019-10-03T00:00:00Z"),
							static.Floats("_value", 1),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-01T00:00:00Z"),
							static.TimeKey("_stop", "2019-12-01T00:00:00Z"),
							static.Times("_time", "2019-11-04T00:00:00Z"),
							static.Floats("_value", 1),
						},
					},
				},
			},
		},
		{
			aggregate: storageflux.MaxKind,
			want: static.TableGroup{
				static.StringKey("_measurement", "m0"),
				static.StringKey("_field", "f0"),
				static.TableMatrix{
					static.StringKeys("t0", "a-0", "a-1", "a-2"),
					{
						static.Table{
							static.TimeKey("_start", "2019-09-01T00:00:00Z"),
							static.TimeKey("_stop", "2019-10-01T00:00:00Z"),
							static.Times("_time", "2019-09-04T00:00:00Z"),
							static.Floats("_value", 4),
						},
						static.Table{
							static.TimeKey("_start", "2019-10-01T00:00:00Z"),
							static.TimeKey("_stop", "2019-11-01T00:00:00Z"),
							static.Times("_time", "2019-10-02T00:00:00Z"),
							static.Floats("_value", 4),
						},
						static.Table{
							static.TimeKey("_start", "2019-11-01T00:00:00Z"),
							static.TimeKey("_stop", "2019-12-01T00:00:00Z"),
							static.Times("_time", "2019-11-03T00:00:00Z"),
							static.Floats("_value", 4),
						},
					},
				},
			},
		},
	} {
		mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		alloc := &memory.Allocator{
			Allocator: mem,
		}
		got, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: reader.Org,
				BucketID:       reader.Bucket,
				Bounds:         reader.Bounds,
			},
			Window: execute.Window{
				Every:  values.MakeDuration(0, 1, false),
				Period: values.MakeDuration(0, 1, false),
			},
			Aggregates: []plan.ProcedureKind{
				tt.aggregate,
			},
		}, alloc)

		if err != nil {
			t.Fatal(err)
		}

		if diff := table.Diff(tt.want, got); diff != "" {
			t.Errorf("unexpected results for %v aggregate -want/+got:\n%s", tt.aggregate, diff)
		}
	}
}

// TestStorageReader_Backoff will invoke the read function
// and then send the table to a separate goroutine so it doesn't
// block the table iterator. The table iterator should be blocked
// until it is read by the other goroutine.
func TestStorageReader_Backoff(t *testing.T) {
	t.Skip("memory allocations are not tracked properly")
	reader := NewStorageReader(t, func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 3),
			),
		)
		tr := TimeRange("2019-09-01T00:00:00Z", "2019-09-02T00:00:00Z")
		return datagen.NewSeriesGeneratorFromSpec(spec, tr), tr
	})
	defer reader.Close()

	for _, tt := range []struct {
		name string
		read func(reader *StorageReader, mem *memory.Allocator) (flux.TableIterator, error)
	}{
		{
			name: "ReadFilter",
			read: func(reader *StorageReader, mem *memory.Allocator) (flux.TableIterator, error) {
				return reader.ReadFilter(context.Background(), query.ReadFilterSpec{
					OrganizationID: reader.Org,
					BucketID:       reader.Bucket,
					Bounds:         reader.Bounds,
				}, mem)
			},
		},
		{
			name: "ReadGroup",
			read: func(reader *StorageReader, mem *memory.Allocator) (flux.TableIterator, error) {
				return reader.ReadGroup(context.Background(), query.ReadGroupSpec{
					ReadFilterSpec: query.ReadFilterSpec{
						OrganizationID: reader.Org,
						BucketID:       reader.Bucket,
						Bounds:         reader.Bounds,
					},
					GroupMode: query.GroupModeBy,
					GroupKeys: []string{"_measurement", "_field"},
				}, mem)
			},
		},
		{
			name: "ReadWindowAggregate",
			read: func(reader *StorageReader, mem *memory.Allocator) (flux.TableIterator, error) {
				return reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{
					ReadFilterSpec: query.ReadFilterSpec{
						OrganizationID: reader.Org,
						BucketID:       reader.Bucket,
						Bounds:         reader.Bounds,
					},
					Aggregates: []plan.ProcedureKind{
						storageflux.MeanKind,
					},
					TimeColumn: execute.DefaultStopColLabel,
					Window: execute.Window{
						Every:  values.ConvertDurationNsecs(20 * time.Second),
						Period: values.ConvertDurationNsecs(20 * time.Second),
					},
				}, mem)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Read the table and learn what the maximum allocated
			// value is. We don't want to exceed this.
			mem := &memory.Allocator{}
			tables, err := tt.read(reader, mem)
			if err != nil {
				t.Fatal(err)
			}

			if err := tables.Do(func(t flux.Table) error {
				return t.Do(func(cr flux.ColReader) error {
					return nil
				})
			}); err != nil {
				t.Fatal(err)
			}

			// The total allocated should not be the same
			// as the max allocated. If this is the case, we
			// either had one buffer or did not correctly
			// release memory for each buffer.
			if mem.MaxAllocated() == mem.TotalAllocated() {
				t.Fatal("max allocated is the same as total allocated, they must be different for this test to be meaningful")
			}

			// Recreate the memory allocator and set the limit
			// to the max allocated. This will cause a panic
			// if the next buffer attempts to be allocated
			// before the first.
			limit := mem.MaxAllocated()
			mem = &memory.Allocator{Limit: &limit}
			tables, err = tt.read(reader, mem)
			if err != nil {
				t.Fatal(err)
			}

			var wg sync.WaitGroup
			_ = tables.Do(func(t flux.Table) error {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = t.Do(func(cr flux.ColReader) error {
						return nil
					})
				}()
				return nil
			})
			wg.Wait()
		})
	}
}

func BenchmarkReadFilter(b *testing.B) {
	setupFn := func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a-%s", 0, 5)
					},
				},
				{
					TagKey: "t1",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("b-%s", 0, 1000)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							r := rand.New(rand.NewSource(10))
							return datagen.NewTimeFloatValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewFloatRandomValuesSequence(0, 90, r),
							)
						},
					},
				},
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f1",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							r := rand.New(rand.NewSource(11))
							return datagen.NewTimeFloatValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewFloatRandomValuesSequence(0, 180, r),
							)
						},
					},
				},
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f1",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							r := rand.New(rand.NewSource(12))
							return datagen.NewTimeFloatValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewFloatRandomValuesSequence(10, 10000, r),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-26T00:00:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
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

func BenchmarkReadGroup(b *testing.B) {
	setupFn := func(org, bucket platform.ID) (datagen.SeriesGenerator, datagen.TimeRange) {
		tagsSpec := &datagen.TagsSpec{
			Tags: []*datagen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("a-%s", 0, 5)
					},
				},
				{
					TagKey: "t1",
					Values: func() datagen.CountableSequence {
						return datagen.NewCounterByteSequence("b-%s", 0, 1000)
					},
				},
			},
		}
		spec := datagen.Spec{
			Measurements: []datagen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							r := rand.New(rand.NewSource(10))
							return datagen.NewTimeFloatValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewFloatRandomValuesSequence(0, 90, r),
							)
						},
					},
				},
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f1",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							r := rand.New(rand.NewSource(11))
							return datagen.NewTimeFloatValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewFloatRandomValuesSequence(0, 180, r),
							)
						},
					},
				},
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &datagen.FieldValuesSpec{
						Name: "f1",
						TimeSequenceSpec: datagen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
							r := rand.New(rand.NewSource(12))
							return datagen.NewTimeFloatValuesSequence(
								spec.Count,
								datagen.NewTimestampSequenceFromSpec(spec),
								datagen.NewFloatRandomValuesSequence(10, 100, r),
							)
						},
					},
				},
			},
		}
		tr := datagen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-25T00:10:00Z"),
		}
		return datagen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	}
	benchmarkRead(b, setupFn, func(r *StorageReader) error {
		mem := &memory.Allocator{}
		tables, err := r.ReadGroup(context.Background(), query.ReadGroupSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: r.Org,
				BucketID:       r.Bucket,
				Bounds:         r.Bounds,
			},
			GroupMode:       query.GroupModeBy,
			GroupKeys:       []string{"_start", "_stop", "t0"},
			AggregateMethod: storageflux.MinKind,
		}, mem)
		if err != nil {
			return err
		}

		err = tables.Do(func(table flux.Table) error {
			table.Done()
			return nil
		})

		return err
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

func Time(s string) execute.Time {
	ts := mustParseTime(s)
	return execute.Time(ts.UnixNano())
}

func mustParseTime(s string) time.Time {
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return ts
}

func Spec(org, bucket platform.ID, measurements ...datagen.MeasurementSpec) *datagen.Spec {
	return &datagen.Spec{
		Measurements: measurements,
	}
}

func MeasurementSpec(name string, field *datagen.FieldValuesSpec, tags ...*datagen.TagValuesSpec) datagen.MeasurementSpec {
	return datagen.MeasurementSpec{
		Name:            name,
		TagsSpec:        TagsSpec(tags...),
		FieldValuesSpec: field,
	}
}

func FloatArrayValuesSequence(name string, delta time.Duration, values []float64) *datagen.FieldValuesSpec {
	return &datagen.FieldValuesSpec{
		Name: name,
		TimeSequenceSpec: datagen.TimeSequenceSpec{
			Count: math.MaxInt32,
			Delta: delta,
		},
		DataType: models.Float,
		Values: func(spec datagen.TimeSequenceSpec) datagen.TimeValuesSequence {
			return datagen.NewTimeFloatValuesSequence(
				spec.Count,
				datagen.NewTimestampSequenceFromSpec(spec),
				datagen.NewFloatArrayValuesSequence(values),
			)
		},
	}
}

func TagsSpec(specs ...*datagen.TagValuesSpec) *datagen.TagsSpec {
	return &datagen.TagsSpec{Tags: specs}
}

func TagValuesSequence(key, format string, start, stop int) *datagen.TagValuesSpec {
	return &datagen.TagValuesSpec{
		TagKey: key,
		Values: func() datagen.CountableSequence {
			return datagen.NewCounterByteSequence(format, start, stop)
		},
	}
}

func TimeRange(start, end string) datagen.TimeRange {
	return datagen.TimeRange{
		Start: mustParseTime(start),
		End:   mustParseTime(end),
	}
}

// seriesBatchSize specifies the number of series keys passed to the index.
const seriesBatchSize = 1000

func writeShard(sfile *tsdb.SeriesFile, sg datagen.SeriesGenerator, id uint64, path string) error {
	sw := shard.NewWriter(id, path)
	defer sw.Close()

	var (
		keys  [][]byte
		names [][]byte
		tags  []models.Tags
	)

	for sg.Next() {
		seriesKey := sg.Key()
		keys = append(keys, seriesKey)
		names = append(names, sg.Name())
		tags = append(tags, sg.Tags())

		if len(keys) == seriesBatchSize {
			if _, err := sfile.CreateSeriesListIfNotExists(names, tags); err != nil {
				return err
			}
			keys = keys[:0]
			names = names[:0]
			tags = tags[:0]
		}

		vg := sg.TimeValuesGenerator()

		key := tsm1.SeriesFieldKeyBytes(string(seriesKey), string(sg.Field()))
		for vg.Next() {
			sw.WriteV(key, vg.Values())
		}

		if err := sw.Err(); err != nil {
			return err
		}
	}

	if len(keys) > seriesBatchSize {
		if _, err := sfile.CreateSeriesListIfNotExists(names, tags); err != nil {
			return err
		}
	}
	return nil
}
