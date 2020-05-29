package tsi1_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"github.com/influxdata/influxdb/v2/tsdb/tsi1"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// Bloom filter settings used in tests.
const M, K = 4096, 6

// Ensure index can iterate over all measurement names.
func TestIndex_ForEachMeasurementName(t *testing.T) {
	idx := MustOpenIndex(1, tsi1.NewConfig())
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify measurements are returned.
	idx.Run(t, func(t *testing.T) {
		var names []string
		if err := idx.ForEachMeasurementName(func(name []byte) error {
			names = append(names, string(name))
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(names, []string{"cpu", "mem"}) {
			t.Fatalf("unexpected names: %#v", names)
		}
	})

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk")},
		{Name: []byte("mem")},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify new measurements.
	idx.Run(t, func(t *testing.T) {
		var names []string
		if err := idx.ForEachMeasurementName(func(name []byte) error {
			names = append(names, string(name))
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(names, []string{"cpu", "disk", "mem"}) {
			t.Fatalf("unexpected names: %#v", names)
		}
	})
}

// Ensure index can return whether a measurement exists.
func TestIndex_MeasurementExists(t *testing.T) {
	idx := MustOpenIndex(1, tsi1.NewConfig())
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify measurement exists.
	idx.Run(t, func(t *testing.T) {
		if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
			t.Fatal(err)
		} else if !v {
			t.Fatal("expected measurement to exist")
		}
	})

	name, tags := []byte("cpu"), models.NewTags(map[string]string{"region": "east"})
	sid := idx.Index.SeriesFile().SeriesID(name, tags, nil)
	if sid.IsZero() {
		t.Fatalf("got 0 series id for %s/%v", name, tags)
	}

	// Delete one series.
	if err := idx.DropSeries([]tsi1.DropSeriesItem{{SeriesID: sid, Key: models.MakeKey(name, tags)}}, true); err != nil {
		t.Fatal(err)
	}

	// Verify measurement still exists.
	idx.Run(t, func(t *testing.T) {
		if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
			t.Fatal(err)
		} else if !v {
			t.Fatal("expected measurement to still exist")
		}
	})

	// Delete second series.
	tags.Set([]byte("region"), []byte("west"))
	sid = idx.Index.SeriesFile().SeriesID(name, tags, nil)
	if sid.IsZero() {
		t.Fatalf("got 0 series id for %s/%v", name, tags)
	}
	if err := idx.DropSeries([]tsi1.DropSeriesItem{{SeriesID: sid, Key: models.MakeKey(name, tags)}}, true); err != nil {
		t.Fatal(err)
	}

	// Verify measurement is now deleted.
	idx.Run(t, func(t *testing.T) {
		if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
			t.Fatal(err)
		} else if v {
			t.Fatal("expected measurement to be deleted")
		}
	})
}

// Ensure index can return a list of matching measurements.
func TestIndex_MeasurementNamesByRegex(t *testing.T) {
	idx := MustOpenIndex(1, tsi1.NewConfig())
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu")},
		{Name: []byte("disk")},
		{Name: []byte("mem")},
	}); err != nil {
		t.Fatal(err)
	}

	// Retrieve measurements by regex.
	idx.Run(t, func(t *testing.T) {
		names, err := idx.MeasurementNamesByRegex(regexp.MustCompile(`cpu|mem`))
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(names, [][]byte{[]byte("cpu"), []byte("mem")}) {
			t.Fatalf("unexpected names: %v", names)
		}
	})
}

// Ensure index can delete a measurement and all related keys, values, & series.
func TestIndex_DropMeasurement(t *testing.T) {
	idx := MustOpenIndex(1, tsi1.NewConfig())
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"region": "north"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "west", "country": "us"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Drop measurement.
	if err := idx.DropMeasurement([]byte("cpu")); err != nil {
		t.Fatal(err)
	}

	// Verify data is gone in each stage.
	idx.Run(t, func(t *testing.T) {
		// Verify measurement is gone.
		if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
			t.Fatal(err)
		} else if v {
			t.Fatal("expected no measurement")
		}

		// Obtain file set to perform lower level checks.
		fs, err := idx.PartitionAt(0).FileSet()
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()

		// Verify tags & values are gone.
		if e := fs.TagKeyIterator([]byte("cpu")).Next(); e != nil && !e.Deleted() {
			t.Fatal("expected deleted tag key")
		}
		if itr := fs.TagValueIterator([]byte("cpu"), []byte("region")); itr != nil {
			t.Fatal("expected nil tag value iterator")
		}

	})
}

func TestIndex_Open(t *testing.T) {
	// Opening a fresh index should set the MANIFEST version to current version.
	idx := NewIndex(tsi1.DefaultPartitionN, tsi1.NewConfig())
	defer idx.Close()

	t.Run("open new index", func(t *testing.T) {
		if err := idx.Open(); err != nil {
			t.Fatal(err)
		}

		// Check version set appropriately.
		for i := 0; uint64(i) < tsi1.DefaultPartitionN; i++ {
			partition := idx.PartitionAt(i)
			fs, err := partition.FileSet()
			if err != nil {
				t.Fatal(err)
			}
			if got, exp := partition.Manifest(fs).Version, 1; got != exp {
				t.Fatalf("got index version %d, expected %d", got, exp)
			}
			fs.Release()
		}
	})

	// Reopening an open index should return an error.
	t.Run("reopen open index", func(t *testing.T) {
		err := idx.Open()
		if err == nil {
			idx.Close()
			t.Fatal("didn't get an error on reopen, but expected one")
		}
		idx.Close()
	})

	// Opening an incompatible index should return an error.
	incompatibleVersions := []int{-1, 0, 2}
	for _, v := range incompatibleVersions {
		t.Run(fmt.Sprintf("incompatible index version: %d", v), func(t *testing.T) {
			idx = NewIndex(tsi1.DefaultPartitionN, tsi1.NewConfig())
			// Manually create a MANIFEST file for an incompatible index version.
			// under one of the partitions.
			partitionPath := filepath.Join(idx.Path(), "2")
			os.MkdirAll(partitionPath, 0777)

			mpath := filepath.Join(partitionPath, tsi1.ManifestFileName)
			m := tsi1.NewManifest(mpath)
			m.Levels = nil
			m.Version = v // Set example MANIFEST version.
			if _, err := m.Write(); err != nil {
				t.Fatal(err)
			}

			// Log the MANIFEST file.
			data, err := ioutil.ReadFile(mpath)
			if err != nil {
				panic(err)
			}
			t.Logf("Incompatible MANIFEST: %s", data)

			// Opening this index should return an error because the MANIFEST has an
			// incompatible version.
			err = idx.Open()
			if err != tsi1.ErrIncompatibleVersion {
				idx.Close()
				t.Fatalf("got error %v, expected %v", err, tsi1.ErrIncompatibleVersion)
			}
		})
	}
}

func TestIndex_Manifest(t *testing.T) {
	t.Run("current MANIFEST", func(t *testing.T) {
		idx := MustOpenIndex(tsi1.DefaultPartitionN, tsi1.NewConfig())
		defer idx.Close()

		// Check version set appropriately.
		for i := 0; uint64(i) < tsi1.DefaultPartitionN; i++ {
			partition := idx.PartitionAt(i)
			fs, err := partition.FileSet()
			if err != nil {
				t.Fatal(err)
			}
			if got, exp := partition.Manifest(fs).Version, tsi1.Version; got != exp {
				t.Fatalf("got MANIFEST version %d, expected %d", got, exp)
			}
			fs.Release()
		}
	})
}

func TestIndex_DiskSizeBytes(t *testing.T) {
	idx := MustOpenIndex(tsi1.DefaultPartitionN, tsi1.NewConfig())
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"region": "north"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "west", "country": "us"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify on disk size is the same in each stage.
	// Each series stores flag(1) + series(uvarint(2)) + len(name)(1) + len(key)(1) + len(value)(1) + checksum(4).
	expSize := int64(4 * 9)

	// Each MANIFEST file is 419 bytes and there are tsi1.DefaultPartitionN of them
	expSize += int64(tsi1.DefaultPartitionN * 419)

	idx.Run(t, func(t *testing.T) {
		if got, exp := idx.DiskSizeBytes(), expSize; got != exp {
			t.Fatalf("got %d bytes, expected %d", got, exp)
		}
	})
}

// Ensure index can returns measurement cardinality stats.
func TestIndex_MeasurementCardinalityStats(t *testing.T) {
	t.Parallel()

	t.Run("Empty", func(t *testing.T) {
		idx := MustOpenIndex(1, tsi1.NewConfig())
		defer idx.Close()
		if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{}); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("Simple", func(t *testing.T) {
		idx := MustOpenIndex(1, tsi1.NewConfig())
		defer idx.Close()

		if err := idx.CreateSeriesSliceIfNotExists([]Series{
			{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
			{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
			{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
		}); err != nil {
			t.Fatal(err)
		}

		if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{"cpu": 2, "mem": 1}); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("SimpleWithDelete", func(t *testing.T) {
		idx := MustOpenIndex(1, tsi1.NewConfig())
		defer idx.Close()

		if err := idx.CreateSeriesSliceIfNotExists([]Series{
			{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
			{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
			{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
		}); err != nil {
			t.Fatal(err)
		}

		seriesID := idx.SeriesFile.SeriesID([]byte("cpu"), models.NewTags(map[string]string{"region": "west"}), nil)
		if err := idx.DropSeries([]tsi1.DropSeriesItem{{SeriesID: seriesID, Key: idx.SeriesFile.SeriesKey(seriesID)}}, true); err != nil {
			t.Fatal(err)
		} else if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{"cpu": 1, "mem": 1}); diff != "" {
			t.Fatal(diff)
		}

		seriesID = idx.SeriesFile.SeriesID([]byte("mem"), models.NewTags(map[string]string{"region": "east"}), nil)
		if err := idx.DropSeries([]tsi1.DropSeriesItem{{SeriesID: seriesID, Key: idx.SeriesFile.SeriesKey(seriesID)}}, true); err != nil {
			t.Fatal(err)
		} else if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{"cpu": 1}); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("Large", func(t *testing.T) {
		t.Skip("https://github.com/influxdata/influxdb/issues/15220")
		if testing.Short() {
			t.Skip("short mode, skipping")
		}
		idx := MustOpenIndex(1, tsi1.NewConfig())
		defer idx.Close()

		for i := 0; i < 1000; i++ {
			a := make([]Series, 1000)
			for j := range a {
				a[j] = Series{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": fmt.Sprintf("east%04d", (i*1000)+j)})}
			}
			if err := idx.CreateSeriesSliceIfNotExists(a); err != nil {
				t.Fatal(err)
			}
		}

		if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{"cpu": 1000000}); diff != "" {
			t.Fatal(diff)
		}

		// Reopen and verify count.
		if err := idx.Reopen(); err != nil {
			t.Fatal(err)
		} else if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{"cpu": 1000000}); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("LargeWithDelete", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short mode, skipping")
		}
		config := tsi1.NewConfig()
		config.MaxIndexLogFileSize = 4096
		idx := MustOpenIndex(1, config)
		defer idx.Close()

		a := make([]Series, 1000)
		for i := range a {
			a[i] = Series{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": fmt.Sprintf("east%04d", i)})}
		}
		if err := idx.CreateSeriesSliceIfNotExists(a); err != nil {
			t.Fatal(err)
		}

		// Issue deletion.
		if err := idx.DropMeasurement([]byte("cpu")); err != nil {
			t.Fatal(err)
		} else if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{}); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("Cache", func(t *testing.T) {
		config := tsi1.NewConfig()
		config.StatsTTL = 1 * time.Second
		idx := MustOpenIndex(1, config)
		defer idx.Close()

		// Insert two series & verify series.
		if err := idx.CreateSeriesSliceIfNotExists([]Series{
			{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
			{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		}); err != nil {
			t.Fatal(err)
		} else if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{"cpu": 2}); diff != "" {
			t.Fatal(diff)
		}

		// Insert one more series and immediate check. No change should occur.
		if err := idx.CreateSeriesSliceIfNotExists([]Series{
			{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "north"})},
		}); err != nil {
			t.Fatal(err)
		} else if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{"cpu": 2}); diff != "" {
			t.Fatal(diff)
		}

		// Wait for TTL.
		time.Sleep(config.StatsTTL)

		// Verify again and stats should be updated.
		if stats, err := idx.MeasurementCardinalityStats(); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(stats, tsi1.MeasurementCardinalityStats{"cpu": 3}); diff != "" {
			t.Fatal(diff)
		}
	})
}

// Ensure index keeps the correct set of series even with concurrent compactions.
func TestIndex_CompactionConsistency(t *testing.T) {
	t.Skip("TODO: flaky test: https://github.com/influxdata/influxdb/issues/13755")
	t.Parallel()

	idx := NewIndex(tsi1.DefaultPartitionN, tsi1.NewConfig())
	idx.WithLogger(zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel)))
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	// Set up some framework to track launched goroutines.
	wg, done := new(sync.WaitGroup), make(chan struct{})
	spawn := func(fn func()) {
		wg.Add(1)
		go func() {
			for {
				select {
				case <-done:
					wg.Done()
					return
				default:
					fn()
				}
			}
		}()
	}

	// Spawn a goroutine to constantly ask the index to compact.
	spawn(func() { idx.Compact() })

	// Issue a number of writes and deletes for a while.
	expected, operations := make(map[string]struct{}), []string(nil)
	spawn(func() {
		var err error
		if len(expected) > 0 && rand.Intn(5) == 0 {
			for m := range expected {
				err = idx.DropMeasurement([]byte(m))
				operations = append(operations, "delete: "+m)
				delete(expected, m)
				break
			}
		} else {
			m := []byte(fmt.Sprintf("m%d", rand.Int()))
			s := make([]Series, 100)
			for i := range s {
				s[i] = Series{Name: m, Tags: models.NewTags(map[string]string{fmt.Sprintf("t%d", i): "v"})}
			}
			err = idx.CreateSeriesSliceIfNotExists(s)
			operations = append(operations, "add: "+string(m))
			expected[string(m)] = struct{}{}
		}
		if err != nil {
			t.Error(err)
		}
	})

	// Let them run for a while and then wait.
	time.Sleep(10 * time.Second)
	close(done)
	wg.Wait()

	defer func() {
		if !t.Failed() {
			return
		}
		t.Log("expect", len(expected), "measurements after", len(operations), "operations")
		for _, op := range operations {
			t.Log(op)
		}
	}()

	for m := range expected {
		if v, err := idx.MeasurementExists([]byte(m)); err != nil {
			t.Fatal(err)
		} else if !v {
			t.Fatal("expected", m)
		}
	}

	miter, err := idx.MeasurementIterator()
	if err != nil {
		t.Fatal(err)
	}
	defer miter.Close()

	for {
		m, err := miter.Next()
		if err != nil {
			t.Fatal(err)
		} else if m == nil {
			break
		} else if _, ok := expected[string(m)]; !ok {
			t.Fatal("unexpected", string(m))
		}
	}
}

func BenchmarkIndex_CreateSeriesListIfNotExist(b *testing.B) {
	// Read line-protocol and coerce into tsdb format.
	// 1M series generated with:
	// $inch -b 10000 -c 1 -t 10,10,10,10,10,10 -f 1 -m 5 -p 1
	fd, err := os.Open("../testdata/line-protocol-1M.txt.gz")
	if err != nil {
		b.Fatal(err)
	}

	gzr, err := gzip.NewReader(fd)
	if err != nil {
		fd.Close()
		b.Fatal(err)
	}

	data, err := ioutil.ReadAll(gzr)
	if err != nil {
		b.Fatal(err)
	}

	if err := fd.Close(); err != nil {
		b.Fatal(err)
	}

	setup := func() (idx *tsi1.Index, points []models.Point, cleanup func(), err error) {
		points, err = models.ParsePoints(data, []byte("org_bucket"))
		if err != nil {
			return nil, nil, func() {}, err
		}

		dataRoot, err := ioutil.TempDir("", "BenchmarkIndex_CreateSeriesListIfNotExist")
		if err != nil {
			return nil, nil, func() {}, err
		}
		rmdir := func() { os.RemoveAll(dataRoot) }

		seriesPath, err := ioutil.TempDir(dataRoot, "_series")
		if err != nil {
			return nil, nil, rmdir, err
		}

		sfile := seriesfile.NewSeriesFile(seriesPath)
		if err := sfile.Open(context.Background()); err != nil {
			return nil, nil, rmdir, err
		}

		config := tsi1.NewConfig()
		idx = tsi1.NewIndex(sfile, config, tsi1.WithPath(filepath.Join(dataRoot, "index")))

		if testing.Verbose() {
			idx.WithLogger(logger.New(os.Stdout))
		}

		if err := idx.Open(context.Background()); err != nil {
			return nil, nil, rmdir, err
		}
		return idx, points, func() { idx.Close(); rmdir() }, nil
	}

	b.ReportAllocs()
	b.Run("create_series", func(b *testing.B) {
		idx, points, cleanup, err := setup()
		defer cleanup()
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for i := 0; i < len(points); i += 10000 {
				b.StopTimer()
				collection := tsdb.NewSeriesCollection(points[i : i+10000])
				b.StartTimer()

				if err := idx.CreateSeriesListIfNotExists(collection); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("already_exist_series", func(b *testing.B) {
		idx, points, cleanup, err := setup()
		defer cleanup()
		if err != nil {
			b.Fatal(err)
		}

		// Ensure all points already written.
		for i := 0; i < len(points); i += 10000 {
			collection := tsdb.NewSeriesCollection(points[i : i+10000])
			if err := idx.CreateSeriesListIfNotExists(collection); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for i := 0; i < len(points); i += 10000 {
				b.StopTimer()
				collection := tsdb.NewSeriesCollection(points[i : i+10000])
				b.StartTimer()
				if err := idx.CreateSeriesListIfNotExists(collection); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// Index is a test wrapper for tsi1.Index.
type Index struct {
	*tsi1.Index
	Config     tsi1.Config
	SeriesFile *SeriesFile
}

// NewIndex returns a new instance of Index at a temporary path.
func NewIndex(partitionN uint64, c tsi1.Config) *Index {
	idx := &Index{
		Config:     c,
		SeriesFile: NewSeriesFile(),
	}
	idx.Index = tsi1.NewIndex(idx.SeriesFile.SeriesFile, idx.Config, tsi1.WithPath(MustTempDir()))
	idx.Index.PartitionN = partitionN
	return idx
}

// MustOpenIndex returns a new, open index. Panic on error.
func MustOpenIndex(partitionN uint64, c tsi1.Config) *Index {
	idx := NewIndex(partitionN, c)
	if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
}

// Open opens the underlying tsi1.Index and tsdb.SeriesFile
func (idx Index) Open() error {
	if err := idx.SeriesFile.Open(context.Background()); err != nil {
		return err
	}
	return idx.Index.Open(context.Background())
}

// Close closes and removes the index directory.
func (idx *Index) Close() error {
	defer os.RemoveAll(idx.Path())
	if err := idx.Index.Close(); err != nil {
		return err
	}
	return idx.SeriesFile.Close()
}

// Reopen closes and opens the index.
func (idx *Index) Reopen() error {
	if err := idx.Index.Close(); err != nil {
		return err
	}

	// Reopen the series file correctly, by initialising a new underlying series
	// file using the same disk data.
	if err := idx.SeriesFile.Reopen(); err != nil {
		return err
	}

	partitionN := idx.Index.PartitionN // Remember how many partitions to use.
	idx.Index = tsi1.NewIndex(idx.SeriesFile.SeriesFile, idx.Config, tsi1.WithPath(idx.Index.Path()))
	idx.Index.PartitionN = partitionN
	return idx.Open()
}

// Run executes a subtest for each of several different states:
//
// - Immediately
// - After reopen
// - After compaction
// - After reopen again
//
// The index should always respond in the same fashion regardless of
// how data is stored. This helper allows the index to be easily tested
// in all major states.
func (idx *Index) Run(t *testing.T, fn func(t *testing.T)) {
	// Invoke immediately.
	t.Run("state=initial", fn)

	// Reopen and invoke again.
	if err := idx.Reopen(); err != nil {
		t.Fatalf("reopen error: %s", err)
	}
	t.Run("state=reopen", fn)

	// TODO: Request a compaction.
	// if err := idx.Compact(); err != nil {
	// 	t.Fatalf("compact error: %s", err)
	// }
	// t.Run("state=post-compaction", fn)

	// Reopen and invoke again.
	if err := idx.Reopen(); err != nil {
		t.Fatalf("post-compaction reopen error: %s", err)
	}
	t.Run("state=post-compaction-reopen", fn)
}

// CreateSeriesSliceIfNotExists creates multiple series at a time.
func (idx *Index) CreateSeriesSliceIfNotExists(a []Series) error {
	collection := &tsdb.SeriesCollection{
		Keys:  make([][]byte, 0, len(a)),
		Names: make([][]byte, 0, len(a)),
		Tags:  make([]models.Tags, 0, len(a)),
		Types: make([]models.FieldType, 0, len(a)),
	}

	for _, s := range a {
		collection.Keys = append(collection.Keys, models.MakeKey(s.Name, s.Tags))
		collection.Names = append(collection.Names, s.Name)
		collection.Tags = append(collection.Tags, s.Tags)
		collection.Types = append(collection.Types, s.Type)
	}
	return idx.CreateSeriesListIfNotExists(collection)
}
