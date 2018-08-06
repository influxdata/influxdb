package tsi1_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Bloom filter settings used in tests.
const M, K = 4096, 6

// Ensure index can iterate over all measurement names.
func TestIndex_ForEachMeasurementName(t *testing.T) {
	idx := MustOpenIndex(1)
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
	idx := MustOpenIndex(1)
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
	idx := MustOpenIndex(1)
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
	idx := MustOpenIndex(1)
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
	idx := NewIndex(tsi1.DefaultPartitionN)
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
			idx = NewIndex(tsi1.DefaultPartitionN)
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

// MustOpenIndex returns a new, open index. Panic on error.
func MustOpenIndex(partitionN uint64) *Index {
	idx := NewIndex(partitionN)
	if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
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
func BenchmarkIndex_IndexFile_TagValueSeriesIDIterator(b *testing.B) {
	var err error
	sfile := NewSeriesFile()
	// Load index
	idx := tsi1.NewIndex(sfile.SeriesFile, "foo",
		tsi1.WithPath("testdata/index-file-index"),
		tsi1.DisableCompactions(),
	)
	defer sfile.Close()

	if err = idx.Open(); err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	// This benchmark will merge eight bitsets each containing ~10,000 series IDs.
	b.Run("78888 series TagValueSeriesIDIterator", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tsiditr, err = idx.TagValueSeriesIDIterator([]byte("m4"), []byte("tag0"), []byte("value4"))
			if err != nil {
				b.Fatal(err)
			} else if tsiditr == nil {
				b.Fatal("got nil iterator")
			}
		}
	})
}

var errResult error

func BenchmarkIndex_CreateSeriesListIfNotExists(b *testing.B) {
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
