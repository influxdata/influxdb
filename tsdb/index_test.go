package tsdb_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/logger"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/tsdb"
	"github.com/influxdata/platform/tsdb/tsi1"
)

func toSeriesIDs(ids []uint64) []tsdb.SeriesID {
	sids := make([]tsdb.SeriesID, 0, len(ids))
	for _, id := range ids {
		sids = append(sids, tsdb.NewSeriesID(id))
	}
	return sids
}

// Ensure iterator can merge multiple iterators together.
func TestMergeSeriesIDIterators(t *testing.T) {
	itr := tsdb.MergeSeriesIDIterators(
		tsdb.NewSeriesIDSliceIterator(toSeriesIDs([]uint64{1, 2, 3})),
		tsdb.NewSeriesIDSliceIterator(nil),
		tsdb.NewSeriesIDSliceIterator(toSeriesIDs([]uint64{1, 2, 3, 4})),
	)

	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, tsdb.SeriesIDElem{SeriesID: tsdb.NewSeriesID(1)}) {
		t.Fatalf("unexpected elem(0): %#v", e)
	}
	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, tsdb.SeriesIDElem{SeriesID: tsdb.NewSeriesID(2)}) {
		t.Fatalf("unexpected elem(1): %#v", e)
	}
	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, tsdb.SeriesIDElem{SeriesID: tsdb.NewSeriesID(3)}) {
		t.Fatalf("unexpected elem(2): %#v", e)
	}
	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, tsdb.SeriesIDElem{SeriesID: tsdb.NewSeriesID(4)}) {
		t.Fatalf("unexpected elem(3): %#v", e)
	}
	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if !e.SeriesID.IsZero() {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Index wraps a series file and index.
type Index struct {
	rootPath string

	config tsi1.Config
	*tsi1.Index
	sfile *tsdb.SeriesFile
}

// MustNewIndex will initialize a new index using the provide type. It creates
// everything under the same root directory so it can be cleanly removed on Close.
//
// The index will not be opened.
func MustNewIndex(c tsi1.Config) *Index {
	rootPath, err := ioutil.TempDir("", "influxdb-tsdb")
	if err != nil {
		panic(err)
	}

	seriesPath, err := ioutil.TempDir(rootPath, tsdb.DefaultSeriesFileDirectory)
	if err != nil {
		panic(err)
	}

	sfile := tsdb.NewSeriesFile(seriesPath)
	if err := sfile.Open(); err != nil {
		panic(err)
	}

	i := tsi1.NewIndex(sfile, "remove-me", c, tsi1.WithPath(filepath.Join(rootPath, "index")))

	if testing.Verbose() {
		i.WithLogger(logger.New(os.Stderr))
	}

	idx := &Index{
		config:   c,
		Index:    i,
		rootPath: rootPath,
		sfile:    sfile,
	}
	return idx
}

// MustOpenNewIndex will initialize a new index using the provide type and opens
// it.
func MustOpenNewIndex(c tsi1.Config) *Index {
	idx := MustNewIndex(c)
	idx.MustOpen()
	return idx
}

// MustOpen opens the underlying index or panics.
func (i *Index) MustOpen() {
	if err := i.Index.Open(); err != nil {
		panic(err)
	}
}

func (idx *Index) AddSeries(name string, tags map[string]string, typ models.FieldType) error {
	t := models.NewTags(tags)
	key := fmt.Sprintf("%s,%s", name, t.HashKey())
	return idx.CreateSeriesIfNotExists([]byte(key), []byte(name), t, typ)
}

// Reopen closes and re-opens the underlying index, without removing any data.
func (i *Index) Reopen() error {
	if err := i.Index.Close(); err != nil {
		return err
	}

	if err := i.sfile.Close(); err != nil {
		return err
	}

	i.sfile = tsdb.NewSeriesFile(i.sfile.Path())
	if err := i.sfile.Open(); err != nil {
		return err
	}

	i.Index = tsi1.NewIndex(i.SeriesFile(), "remove-me", i.config, tsi1.WithPath(filepath.Join(i.rootPath, "index")))
	return i.Index.Open()
}

// Close closes the index cleanly and removes all on-disk data.
func (i *Index) Close() error {
	if err := i.Index.Close(); err != nil {
		return err
	}

	if err := i.sfile.Close(); err != nil {
		return err
	}
	//return os.RemoveAll(i.rootPath)
	return nil
}

// This benchmark compares the TagSets implementation across index types.
//
// In the case of the TSI index, TagSets has to merge results across all several
// index partitions.
//
// Typical results on an i7 laptop.
//
// BenchmarkIndex_TagSets/1M_series/tsi1-8    	     100	  18995530 ns/op	 5221180 B/op	   20379 allocs/op
func BenchmarkIndex_TagSets(b *testing.B) {
	// Read line-protocol and coerce into tsdb format.
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

	// setup writes all of the above points to the index.
	setup := func(idx *Index) {
		batchSize := 10000
		for j := 0; j < 1; j++ {
			for i := 0; i < len(points); i += batchSize {
				collection := tsdb.NewSeriesCollection(points[i : i+batchSize])
				if err := idx.CreateSeriesListIfNotExists(collection); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	var errResult error

	// This benchmark will merge eight bitsets each containing ~10,000 series IDs.
	b.Run("1M series", func(b *testing.B) {
		b.ReportAllocs()
		for _, indexType := range tsdb.RegisteredIndexes() {
			idx := MustOpenNewIndex(tsi1.NewConfig())
			setup(idx)

			name := []byte("m4")
			opt := query.IteratorOptions{Condition: influxql.MustParseExpr(`"tag5"::tag = 'value0'`)}

			ts := func() ([]*query.TagSet, error) {
				return idx.Index.TagSets(name, opt)
			}

			b.Run(indexType, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					// Will call TagSets on the appropriate implementation.
					_, errResult = ts()
					if errResult != nil {
						b.Fatal(err)
					}
				}
			})

			if err := idx.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// This benchmark concurrently writes series to the index and fetches cached bitsets.
// The idea is to emphasize the performance difference when bitset caching is on and off.
//
// Typical results for an i7 laptop
//
// BenchmarkIndex_ConcurrentWriteQuery/inmem/queries_100000/cache-8   	  1	5963346204 ns/op	2499655768 B/op	 23964183 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/inmem/queries_100000/no_cache-8    1	5314841090 ns/op	2499495280 B/op	 23963322 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/tsi1/queries_100000/cache-8        1	1645048376 ns/op	2215402840 B/op	 23048978 allocs/op
// BenchmarkIndex_ConcurrentWriteQuery/tsi1/queries_100000/no_cache-8     1	22242155616 ns/op	28277544136 B/op 79620463 allocs/op
func BenchmarkIndex_ConcurrentWriteQuery(b *testing.B) {
	// Read line-protocol and coerce into tsdb format.
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

	runBenchmark := func(b *testing.B, index string, queryN int) {
		idx := MustOpenNewIndex(tsi1.NewConfig())
		var wg sync.WaitGroup
		begin := make(chan struct{})

		// Run concurrent iterator...
		runIter := func() {
			keys := [][]string{
				{"m0", "tag2", "value4"},
				{"m1", "tag3", "value5"},
				{"m2", "tag4", "value6"},
				{"m3", "tag0", "value8"},
				{"m4", "tag5", "value0"},
			}

			<-begin // Wait for writes to land
			for i := 0; i < queryN/5; i++ {
				for _, key := range keys {
					itr, err := idx.TagValueSeriesIDIterator([]byte(key[0]), []byte(key[1]), []byte(key[2]))
					if err != nil {
						b.Fatal(err)
					}

					if itr == nil {
						panic("should not happen")
					}

					if err := itr.Close(); err != nil {
						b.Fatal(err)
					}
				}
			}
		}

		batchSize := 10000
		wg.Add(1)
		go func() { defer wg.Done(); runIter() }()
		var once sync.Once
		for j := 0; j < b.N; j++ {
			for i := 0; i < len(points); i += batchSize {
				collection := tsdb.NewSeriesCollection(points[i : i+batchSize])
				if err := idx.CreateSeriesListIfNotExists(collection); err != nil {
					b.Fatal(err)
				}
				once.Do(func() { close(begin) })
			}

			// Wait for queries to finish
			wg.Wait()

			// Reset the index...
			b.StopTimer()
			if err := idx.Close(); err != nil {
				b.Fatal(err)
			}

			// Re-open everything
			idx = MustOpenNewIndex(tsi1.NewConfig())
			wg.Add(1)
			begin = make(chan struct{})
			once = sync.Once{}
			go func() { defer wg.Done(); runIter() }()
			b.StartTimer()
		}
	}

	queries := []int{1e5}
	for _, indexType := range tsdb.RegisteredIndexes() {
		b.Run(indexType, func(b *testing.B) {
			for _, queryN := range queries {
				b.Run(fmt.Sprintf("queries %d", queryN), func(b *testing.B) {
					b.Run("cache", func(b *testing.B) {
						tsi1.EnableBitsetCache = true
						runBenchmark(b, indexType, queryN)
					})

					b.Run("no cache", func(b *testing.B) {
						tsi1.EnableBitsetCache = false
						runBenchmark(b, indexType, queryN)
					})
				})
			}
		})
	}
}
