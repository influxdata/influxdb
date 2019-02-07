package tsi1_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Bloom filter settings used in tests.
const M, K = 4096, 6

// Ensure index can iterate over all measurement names.
func TestIndex_ForEachMeasurementName(t *testing.T) {
	idx := MustOpenDefaultIndex()
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
	idx := MustOpenDefaultIndex()
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
	if sid == 0 {
		t.Fatalf("got 0 series id for %s/%v", name, tags)
	}

	// Delete one series.
	if err := idx.DropSeries(sid, models.MakeKey(name, tags), true); err != nil {
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
	if sid == 0 {
		t.Fatalf("got 0 series id for %s/%v", name, tags)
	}
	if err := idx.DropSeries(sid, models.MakeKey(name, tags), true); err != nil {
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
	idx := MustOpenDefaultIndex()
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
	idx := MustOpenDefaultIndex()
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
		fs, err := idx.PartitionAt(0).RetainFileSet()
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
	idx := NewDefaultIndex()
	t.Run("open new index", func(t *testing.T) {
		if err := idx.Open(); err != nil {
			t.Fatal(err)
		}

		// Check version set appropriately.
		for i := 0; uint64(i) < tsi1.DefaultPartitionN; i++ {
			partition := idx.PartitionAt(i)
			if got, exp := partition.Manifest().Version, 1; got != exp {
				t.Fatalf("got index version %d, expected %d", got, exp)
			}
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
			idx = NewDefaultIndex()
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
		idx := MustOpenIndex(tsi1.DefaultPartitionN)

		// Check version set appropriately.
		for i := 0; uint64(i) < tsi1.DefaultPartitionN; i++ {
			partition := idx.PartitionAt(i)
			if got, exp := partition.Manifest().Version, tsi1.Version; got != exp {
				t.Fatalf("got MANIFEST version %d, expected %d", got, exp)
			}
		}
	})
}

func TestIndex_DiskSizeBytes(t *testing.T) {
	idx := MustOpenIndex(tsi1.DefaultPartitionN)
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

func TestIndex_TagValueSeriesIDIterator(t *testing.T) {
	idx1 := MustOpenDefaultIndex() // Uses the single series creation method CreateSeriesIfNotExists
	defer idx1.Close()
	idx2 := MustOpenDefaultIndex() // Uses the batch series creation method CreateSeriesListIfNotExists
	defer idx2.Close()

	// Add some series.
	data := []struct {
		Key  string
		Name string
		Tags map[string]string
	}{
		{"cpu,region=west,server=a", "cpu", map[string]string{"region": "west", "server": "a"}},
		{"cpu,region=west,server=b", "cpu", map[string]string{"region": "west", "server": "b"}},
		{"cpu,region=east,server=a", "cpu", map[string]string{"region": "east", "server": "a"}},
		{"cpu,region=north,server=c", "cpu", map[string]string{"region": "north", "server": "c"}},
		{"cpu,region=south,server=s", "cpu", map[string]string{"region": "south", "server": "s"}},
		{"mem,region=west,server=a", "mem", map[string]string{"region": "west", "server": "a"}},
		{"mem,region=west,server=b", "mem", map[string]string{"region": "west", "server": "b"}},
		{"mem,region=west,server=c", "mem", map[string]string{"region": "west", "server": "c"}},
		{"disk,region=east,server=a", "disk", map[string]string{"region": "east", "server": "a"}},
		{"disk,region=east,server=a", "disk", map[string]string{"region": "east", "server": "a"}},
		{"disk,region=north,server=c", "disk", map[string]string{"region": "north", "server": "c"}},
	}

	var batchKeys [][]byte
	var batchNames [][]byte
	var batchTags []models.Tags
	for _, pt := range data {
		if err := idx1.CreateSeriesIfNotExists([]byte(pt.Key), []byte(pt.Name), models.NewTags(pt.Tags)); err != nil {
			t.Fatal(err)
		}

		batchKeys = append(batchKeys, []byte(pt.Key))
		batchNames = append(batchNames, []byte(pt.Name))
		batchTags = append(batchTags, models.NewTags(pt.Tags))
	}

	if err := idx2.CreateSeriesListIfNotExists(batchKeys, batchNames, batchTags); err != nil {
		t.Fatal(err)
	}

	testTagValueSeriesIDIterator := func(t *testing.T, name, key, value string, expKeys []string) {
		for i, idx := range []*Index{idx1, idx2} {
			sitr, err := idx.TagValueSeriesIDIterator([]byte(name), []byte(key), []byte(value))
			if err != nil {
				t.Fatalf("[index %d] %v", i, err)
			} else if sitr == nil {
				t.Fatalf("[index %d] series id iterater nil", i)
			}

			// Convert series ids to series keys.
			itr := tsdb.NewSeriesIteratorAdapter(idx.SeriesFile.SeriesFile, sitr)
			if itr == nil {
				t.Fatalf("[index %d] got nil iterator", i)
			}
			defer itr.Close()

			var keys []string
			for e, err := itr.Next(); err == nil; e, err = itr.Next() {
				if e == nil {
					break
				}
				keys = append(keys, string(models.MakeKey(e.Name(), e.Tags())))
			}

			if err != nil {
				t.Fatal(err)
			}

			// Iterator was in series id order, which may not be series key order.
			sort.Strings(keys)
			if got, exp := keys, expKeys; !reflect.DeepEqual(got, exp) {
				t.Fatalf("[index %d] got %v, expected %v", i, got, exp)
			}
		}
	}

	// Test that correct series are initially returned
	t.Run("initial", func(t *testing.T) {
		testTagValueSeriesIDIterator(t, "mem", "region", "west", []string{
			"mem,region=west,server=a",
			"mem,region=west,server=b",
			"mem,region=west,server=c",
		})
	})

	// The result should now be cached, and the same result should be returned.
	t.Run("cached", func(t *testing.T) {
		testTagValueSeriesIDIterator(t, "mem", "region", "west", []string{
			"mem,region=west,server=a",
			"mem,region=west,server=b",
			"mem,region=west,server=c",
		})
	})

	// Adding a new series that would be referenced by some cached bitsets (in this case
	// the bitsets for mem->region->west and mem->server->c) should cause the cached
	// bitsets to be updated.
	if err := idx1.CreateSeriesIfNotExists(
		[]byte("mem,region=west,root=x,server=c"),
		[]byte("mem"),
		models.NewTags(map[string]string{"region": "west", "root": "x", "server": "c"}),
	); err != nil {
		t.Fatal(err)
	}

	if err := idx2.CreateSeriesListIfNotExists(
		[][]byte{[]byte("mem,region=west,root=x,server=c")},
		[][]byte{[]byte("mem")},
		[]models.Tags{models.NewTags(map[string]string{"region": "west", "root": "x", "server": "c"})},
	); err != nil {
		t.Fatal(err)
	}

	t.Run("insert series", func(t *testing.T) {
		testTagValueSeriesIDIterator(t, "mem", "region", "west", []string{
			"mem,region=west,root=x,server=c",
			"mem,region=west,server=a",
			"mem,region=west,server=b",
			"mem,region=west,server=c",
		})
	})

	if err := idx1.CreateSeriesIfNotExists(
		[]byte("mem,region=west,root=x,server=c"),
		[]byte("mem"),
		models.NewTags(map[string]string{"region": "west", "root": "x", "server": "c"}),
	); err != nil {
		t.Fatal(err)
	}

	if err := idx2.CreateSeriesListIfNotExists(
		[][]byte{[]byte("mem,region=west,root=x,server=c")},
		[][]byte{[]byte("mem")},
		[]models.Tags{models.NewTags(map[string]string{"region": "west", "root": "x", "server": "c"})},
	); err != nil {
		t.Fatal(err)
	}

	t.Run("insert same series", func(t *testing.T) {
		testTagValueSeriesIDIterator(t, "mem", "region", "west", []string{
			"mem,region=west,root=x,server=c",
			"mem,region=west,server=a",
			"mem,region=west,server=b",
			"mem,region=west,server=c",
		})
	})

	t.Run("no matching series", func(t *testing.T) {
		testTagValueSeriesIDIterator(t, "foo", "bar", "zoo", nil)
	})
}

// Index is a test wrapper for tsi1.Index.
type Index struct {
	*tsi1.Index
	SeriesFile *SeriesFile
}

// NewIndex returns a new instance of Index at a temporary path.
func NewIndex(partitionN uint64) *Index {
	idx := &Index{SeriesFile: NewSeriesFile()}
	idx.Index = tsi1.NewIndex(idx.SeriesFile.SeriesFile, "db0", tsi1.WithPath(MustTempDir()))
	idx.Index.PartitionN = partitionN
	return idx
}

// NewIndex returns a new instance of Index with default number of partitions at a temporary path.
func NewDefaultIndex() *Index {
	return NewIndex(tsi1.DefaultPartitionN)
}

// MustOpenIndex returns a new, open index. Panic on error.
func MustOpenIndex(partitionN uint64) *Index {
	idx := NewIndex(partitionN)
	if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
}

// MustOpenIndex returns a new, open index with the default number of partitions.
func MustOpenDefaultIndex() *Index {
	return MustOpenIndex(tsi1.DefaultPartitionN)
}

// Open opens the underlying tsi1.Index and tsdb.SeriesFile
func (idx Index) Open() error {
	if err := idx.SeriesFile.Open(); err != nil {
		return err
	}
	return idx.Index.Open()
}

// Close closes and removes the index directory.
func (idx *Index) Close() error {
	defer os.RemoveAll(idx.Path())
	if err := idx.SeriesFile.Close(); err != nil {
		return err
	}
	return idx.Index.Close()
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
	idx.Index = tsi1.NewIndex(idx.SeriesFile.SeriesFile, "db0", tsi1.WithPath(idx.Index.Path()))
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
	keys := make([][]byte, 0, len(a))
	names := make([][]byte, 0, len(a))
	tags := make([]models.Tags, 0, len(a))
	for _, s := range a {
		keys = append(keys, models.MakeKey(s.Name, s.Tags))
		names = append(names, s.Name)
		tags = append(tags, s.Tags)
	}
	return idx.CreateSeriesListIfNotExists(keys, names, tags)
}

var tsiditr tsdb.SeriesIDIterator

// Calling TagValueSeriesIDIterator on the index involves merging several
// SeriesIDSets together.BenchmarkIndex_TagValueSeriesIDIterator, which can have
// a non trivial cost. In the case of `tsi` files, the mmapd sets are merged
// together. In the case of tsl files the sets need to are cloned and then merged.
//
// Typical results on an i7 laptop
// BenchmarkIndex_IndexFile_TagValueSeriesIDIterator/78888_series_TagValueSeriesIDIterator/cache-8   	 2000000	       643 ns/op	     744 B/op	      13 allocs/op
// BenchmarkIndex_IndexFile_TagValueSeriesIDIterator/78888_series_TagValueSeriesIDIterator/no_cache-8      10000	    130749 ns/op	  124952 B/op	     350 allocs/op
func BenchmarkIndex_IndexFile_TagValueSeriesIDIterator(b *testing.B) {
	runBenchMark := func(b *testing.B, cacheSize int) {
		var err error
		sfile := NewSeriesFile()
		// Load index
		idx := tsi1.NewIndex(sfile.SeriesFile, "foo",
			tsi1.WithPath("testdata/index-file-index"),
			tsi1.DisableCompactions(),
			tsi1.WithSeriesIDCacheSize(cacheSize),
		)
		defer sfile.Close()

		if err = idx.Open(); err != nil {
			b.Fatal(err)
		}
		defer idx.Close()

		for i := 0; i < b.N; i++ {
			tsiditr, err = idx.TagValueSeriesIDIterator([]byte("m4"), []byte("tag0"), []byte("value4"))
			if err != nil {
				b.Fatal(err)
			} else if tsiditr == nil {
				b.Fatal("got nil iterator")
			}
		}
	}

	// This benchmark will merge eight bitsets each containing ~10,000 series IDs.
	b.Run("78888 series TagValueSeriesIDIterator", func(b *testing.B) {
		b.ReportAllocs()
		b.Run("cache", func(b *testing.B) {
			runBenchMark(b, tsdb.DefaultSeriesIDSetCacheSize)
		})

		b.Run("no cache", func(b *testing.B) {
			runBenchMark(b, 0)
		})
	})
}

var errResult error

// Typical results on an i7 laptop
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_1-8         	       1	4004452124 ns/op	2381998144 B/op	21686990 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_2-8         	       1	2625853773 ns/op	2368913968 B/op	21765385 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_4-8         	       1	2127205189 ns/op	2338013584 B/op	21908381 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_8-8         	       1	2331960889 ns/op	2332643248 B/op	22191763 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_1000/partition_16-8        	       1	2398489751 ns/op	2299551824 B/op	22670465 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_1-8        	       1	3404683972 ns/op	2387236504 B/op	21600671 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_2-8        	       1	2173772186 ns/op	2329237224 B/op	21631104 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_4-8        	       1	1729089575 ns/op	2299161840 B/op	21699878 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_8-8        	       1	1644295339 ns/op	2161473200 B/op	21796469 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_10000/partition_16-8       	       1	1683275418 ns/op	2171872432 B/op	21925974 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_1-8       	       1	3330508160 ns/op	2333250904 B/op	21574887 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_2-8       	       1	2278604285 ns/op	2292600808 B/op	21628966 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_4-8       	       1	1760098762 ns/op	2243730672 B/op	21684608 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_8-8       	       1	1693312924 ns/op	2166924112 B/op	21753079 allocs/op
// BenchmarkIndex_CreateSeriesListIfNotExists/batch_size_100000/partition_16-8      	       1	1663610452 ns/op	2131177160 B/op	21806209 allocs/op
func BenchmarkIndex_CreateSeriesListIfNotExists(b *testing.B) {
	// Read line-protocol and coerce into tsdb format.
	keys := make([][]byte, 0, 1e6)
	names := make([][]byte, 0, 1e6)
	tags := make([]models.Tags, 0, 1e6)

	// 1M series generated with:
	// $inch -b 10000 -c 1 -t 10,10,10,10,10,10 -f 1 -m 5 -p 1
	fd, err := os.Open("../../testdata/line-protocol-1M.txt.gz")
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

	points, err := models.ParsePoints(data)
	if err != nil {
		b.Fatal(err)
	}

	for _, pt := range points {
		keys = append(keys, pt.Key())
		names = append(names, pt.Name())
		tags = append(tags, pt.Tags())
	}

	batchSizes := []int{1000, 10000, 100000}
	partitions := []uint64{1, 2, 4, 8, 16}
	for _, sz := range batchSizes {
		b.Run(fmt.Sprintf("batch size %d", sz), func(b *testing.B) {
			for _, partition := range partitions {
				b.Run(fmt.Sprintf("partition %d", partition), func(b *testing.B) {
					idx := MustOpenIndex(partition)
					for j := 0; j < b.N; j++ {
						for i := 0; i < len(keys); i += sz {
							k := keys[i : i+sz]
							n := names[i : i+sz]
							t := tags[i : i+sz]
							if errResult = idx.CreateSeriesListIfNotExists(k, n, t); errResult != nil {
								b.Fatal(err)
							}
						}
						// Reset the index...
						b.StopTimer()
						if err := idx.Close(); err != nil {
							b.Fatal(err)
						}
						idx = MustOpenIndex(partition)
						b.StartTimer()
					}
				})
			}
		})
	}
}

// This benchmark concurrently writes series to the index and fetches cached bitsets.
// The idea is to emphasize the performance difference when bitset caching is on and off.
//
// Typical results for an i7 laptop
// BenchmarkIndex_ConcurrentWriteQuery/partition_1/queries_100000/cache-8   	       	1	3836451407 ns/op	2453296232 B/op		22648482 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_4/queries_100000/cache-8            	1	1836598730 ns/op	2435668224 B/op		22908705 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_8/queries_100000/cache-8            	1	1714771527 ns/op	2341518456 B/op		23450621 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_16/queries_100000/cache-8           	1	1810658403 ns/op	2401239408 B/op		23868079 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_1/queries_100000/no_cache-8           	1	4044478305 ns/op	4414915048 B/op		27292357 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_4/queries_100000/no_cache-8         	1	18663345153 ns/op	23035974472 B/op	54015704 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_8/queries_100000/no_cache-8         	1	22242979152 ns/op	28178915600 B/op	80156305 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/partition_16/queries_100000/no_cache-8        	1	24817283922 ns/op	34613960984 B/op	150356327 allocs/op
func BenchmarkIndex_ConcurrentWriteQuery(b *testing.B) {
	// Read line-protocol and coerce into tsdb format.
	keys := make([][]byte, 0, 1e6)
	names := make([][]byte, 0, 1e6)
	tags := make([]models.Tags, 0, 1e6)

	// 1M series generated with:
	// $inch -b 10000 -c 1 -t 10,10,10,10,10,10 -f 1 -m 5 -p 1
	fd, err := os.Open("testdata/line-protocol-1M.txt.gz")
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

	points, err := models.ParsePoints(data)
	if err != nil {
		b.Fatal(err)
	}

	for _, pt := range points {
		keys = append(keys, pt.Key())
		names = append(names, pt.Name())
		tags = append(tags, pt.Tags())
	}

	runBenchmark := func(b *testing.B, queryN int, partitions uint64, cacheSize int) {
		idx := &Index{SeriesFile: NewSeriesFile()}
		idx.Index = tsi1.NewIndex(idx.SeriesFile.SeriesFile, "db0", tsi1.WithPath(MustTempDir()), tsi1.WithSeriesIDCacheSize(cacheSize))
		idx.Index.PartitionN = partitions

		if err := idx.Open(); err != nil {
			panic(err)
		}

		var wg sync.WaitGroup

		// Run concurrent iterator...
		runIter := func(b *testing.B) {
			keys := [][]string{
				{"m0", "tag2", "value4"},
				{"m1", "tag3", "value5"},
				{"m2", "tag4", "value6"},
				{"m3", "tag0", "value8"},
				{"m4", "tag5", "value0"},
			}

			for i := 0; i < queryN/5; i++ {
				for _, key := range keys {
					itr, err := idx.TagValueSeriesIDIterator([]byte(key[0]), []byte(key[1]), []byte(key[2]))
					if err != nil {
						b.Fatal(err)
					} else if itr == nil {
						b.Fatal("got nil iterator")
					}
					if err := itr.Close(); err != nil {
						b.Fatal(err)
					}
				}
			}
		}

		wg.Add(1)
		go func() { defer wg.Done(); runIter(b) }()
		batchSize := 10000

		for j := 0; j < 1; j++ {
			for i := 0; i < len(keys); i += batchSize {
				k := keys[i : i+batchSize]
				n := names[i : i+batchSize]
				t := tags[i : i+batchSize]
				if errResult = idx.CreateSeriesListIfNotExists(k, n, t); errResult != nil {
					b.Fatal(err)
				}
			}

			// Wait for queries to finish
			wg.Wait()

			// Reset the index...
			b.StopTimer()
			if err := idx.Close(); err != nil {
				b.Fatal(err)
			}

			// Re-open everything
			idx := &Index{SeriesFile: NewSeriesFile()}
			idx.Index = tsi1.NewIndex(idx.SeriesFile.SeriesFile, "db0", tsi1.WithPath(MustTempDir()), tsi1.WithSeriesIDCacheSize(cacheSize))
			idx.Index.PartitionN = partitions

			if err := idx.Open(); err != nil {
				b.Fatal(err)
			}

			wg.Add(1)
			go func() { defer wg.Done(); runIter(b) }()
			b.StartTimer()
		}
	}

	partitions := []uint64{1, 4, 8, 16}
	queries := []int{1e5}
	for _, partition := range partitions {
		b.Run(fmt.Sprintf("partition %d", partition), func(b *testing.B) {
			for _, queryN := range queries {
				b.Run(fmt.Sprintf("queries %d", queryN), func(b *testing.B) {
					b.Run("cache", func(b *testing.B) {
						runBenchmark(b, queryN, partition, tsdb.DefaultSeriesIDSetCacheSize)
					})

					b.Run("no cache", func(b *testing.B) {
						runBenchmark(b, queryN, partition, 0)
					})
				})
			}
		})
	}
}
