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

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/slices"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"github.com/influxdata/influxql"
)

// Ensure iterator can merge multiple iterators together.
func TestMergeSeriesIDIterators(t *testing.T) {
	itr := tsdb.MergeSeriesIDIterators(
		tsdb.NewSeriesIDSliceIterator([]uint64{1, 2, 3}),
		tsdb.NewSeriesIDSliceIterator(nil),
		nil,
		tsdb.NewSeriesIDSliceIterator([]uint64{1, 2, 3, 4}),
	)

	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, tsdb.SeriesIDElem{SeriesID: 1}) {
		t.Fatalf("unexpected elem(0): %#v", e)
	}
	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, tsdb.SeriesIDElem{SeriesID: 2}) {
		t.Fatalf("unexpected elem(1): %#v", e)
	}
	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, tsdb.SeriesIDElem{SeriesID: 3}) {
		t.Fatalf("unexpected elem(2): %#v", e)
	}
	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, tsdb.SeriesIDElem{SeriesID: 4}) {
		t.Fatalf("unexpected elem(3): %#v", e)
	}
	if e, err := itr.Next(); err != nil {
		t.Fatal(err)
	} else if e.SeriesID != 0 {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

func TestIndexSet_MeasurementNamesByExpr(t *testing.T) {
	// Setup indexes
	indexes := map[string]*Index{}
	for _, name := range tsdb.RegisteredIndexes() {
		idx := MustOpenNewIndex(name)
		idx.AddSeries("cpu", map[string]string{"region": "east"})
		idx.AddSeries("cpu", map[string]string{"region": "west", "secret": "foo"})
		idx.AddSeries("disk", map[string]string{"secret": "foo"})
		idx.AddSeries("mem", map[string]string{"region": "west"})
		idx.AddSeries("gpu", map[string]string{"region": "east"})
		idx.AddSeries("pci", map[string]string{"region": "east", "secret": "foo"})
		indexes[name] = idx
		defer idx.Close()
	}

	authorizer := &internal.AuthorizerMock{
		AuthorizeSeriesReadFn: func(database string, measurement []byte, tags models.Tags) bool {
			if tags.GetString("secret") != "" {
				t.Logf("Rejecting series db=%s, m=%s, tags=%v", database, measurement, tags)
				return false
			}
			return true
		},
	}

	type example struct {
		name     string
		expr     influxql.Expr
		expected [][]byte
	}

	// These examples should be run without any auth.
	examples := []example{
		{name: "all", expected: slices.StringsToBytes("cpu", "disk", "gpu", "mem", "pci")},
		{name: "EQ", expr: influxql.MustParseExpr(`region = 'west'`), expected: slices.StringsToBytes("cpu", "mem")},
		{name: "NEQ", expr: influxql.MustParseExpr(`region != 'west'`), expected: slices.StringsToBytes("gpu", "pci")},
		{name: "EQREGEX", expr: influxql.MustParseExpr(`region =~ /.*st/`), expected: slices.StringsToBytes("cpu", "gpu", "mem", "pci")},
		{name: "NEQREGEX", expr: influxql.MustParseExpr(`region !~ /.*est/`), expected: slices.StringsToBytes("gpu", "pci")},
	}

	// These examples should be run with the authorizer.
	authExamples := []example{
		{name: "all", expected: slices.StringsToBytes("cpu", "gpu", "mem")},
		{name: "EQ", expr: influxql.MustParseExpr(`region = 'west'`), expected: slices.StringsToBytes("mem")},
		{name: "NEQ", expr: influxql.MustParseExpr(`region != 'west'`), expected: slices.StringsToBytes("gpu")},
		{name: "EQREGEX", expr: influxql.MustParseExpr(`region =~ /.*st/`), expected: slices.StringsToBytes("cpu", "gpu", "mem")},
		{name: "NEQREGEX", expr: influxql.MustParseExpr(`region !~ /.*est/`), expected: slices.StringsToBytes("gpu")},
	}

	for _, idx := range tsdb.RegisteredIndexes() {
		t.Run(idx, func(t *testing.T) {
			t.Run("no authorization", func(t *testing.T) {
				for _, example := range examples {
					t.Run(example.name, func(t *testing.T) {
						names, err := indexes[idx].IndexSet().MeasurementNamesByExpr(nil, example.expr)
						if err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(names, example.expected) {
							t.Fatalf("got names: %v, expected %v", slices.BytesToStrings(names), slices.BytesToStrings(example.expected))
						}
					})
				}
			})

			t.Run("with authorization", func(t *testing.T) {
				for _, example := range authExamples {
					t.Run(example.name, func(t *testing.T) {
						names, err := indexes[idx].IndexSet().MeasurementNamesByExpr(authorizer, example.expr)
						if err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(names, example.expected) {
							t.Fatalf("got names: %v, expected %v", slices.BytesToStrings(names), slices.BytesToStrings(example.expected))
						}
					})
				}
			})
		})
	}
}

func TestIndexSet_DedupeInmemIndexes(t *testing.T) {
	testCases := []struct {
		tsiN    int // Quantity of TSI indexes
		inmem1N int // Quantity of ShardIndexes proxying the first inmem Index
		inmem2N int // Quantity of ShardIndexes proxying the second inmem Index
		uniqueN int // Quantity of total, deduplicated indexes
	}{
		{tsiN: 1, inmem1N: 0, uniqueN: 1},
		{tsiN: 2, inmem1N: 0, uniqueN: 2},
		{tsiN: 0, inmem1N: 1, uniqueN: 1},
		{tsiN: 0, inmem1N: 2, uniqueN: 1},
		{tsiN: 0, inmem1N: 1, inmem2N: 1, uniqueN: 2},
		{tsiN: 0, inmem1N: 2, inmem2N: 2, uniqueN: 2},
		{tsiN: 2, inmem1N: 2, inmem2N: 2, uniqueN: 4},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("%d/%d/%d -> %d", testCase.tsiN, testCase.inmem1N, testCase.inmem2N, testCase.uniqueN)
		t.Run(name, func(t *testing.T) {

			var indexes []tsdb.Index
			for i := 0; i < testCase.tsiN; i++ {
				indexes = append(indexes, MustOpenNewIndex(tsi1.IndexName))
			}
			if testCase.inmem1N > 0 {
				sfile := MustOpenSeriesFile()
				opts := tsdb.NewEngineOptions()
				opts.IndexVersion = inmem.IndexName
				opts.InmemIndex = inmem.NewIndex("db", sfile.SeriesFile)

				for i := 0; i < testCase.inmem1N; i++ {
					indexes = append(indexes, inmem.NewShardIndex(uint64(i), tsdb.NewSeriesIDSet(), opts))
				}
			}
			if testCase.inmem2N > 0 {
				sfile := MustOpenSeriesFile()
				opts := tsdb.NewEngineOptions()
				opts.IndexVersion = inmem.IndexName
				opts.InmemIndex = inmem.NewIndex("db", sfile.SeriesFile)

				for i := 0; i < testCase.inmem2N; i++ {
					indexes = append(indexes, inmem.NewShardIndex(uint64(i), tsdb.NewSeriesIDSet(), opts))
				}
			}

			is := tsdb.IndexSet{Indexes: indexes}.DedupeInmemIndexes()
			if len(is.Indexes) != testCase.uniqueN {
				t.Errorf("expected %d indexes, got %d", testCase.uniqueN, len(is.Indexes))
			}
		})
	}
}

func TestIndex_Sketches(t *testing.T) {
	checkCardinalities := func(t *testing.T, index *Index, state string, series, tseries, measurements, tmeasurements int) {
		t.Helper()

		// Get sketches and check cardinality...
		sketch, tsketch, err := index.SeriesSketches()
		if err != nil {
			t.Fatal(err)
		}

		// delta calculates a rough 10% delta. If i is small then a minimum value
		// of 2 is used.
		delta := func(i int) int {
			v := i / 10
			if v == 0 {
				v = 2
			}
			return v
		}

		// series cardinality should be well within 10%.
		if got, exp := int(sketch.Count()), series; got-exp < -delta(series) || got-exp > delta(series) {
			t.Errorf("[%s] got series cardinality %d, expected ~%d", state, got, exp)
		}

		// check series tombstones
		if got, exp := int(tsketch.Count()), tseries; got-exp < -delta(tseries) || got-exp > delta(tseries) {
			t.Errorf("[%s] got series tombstone cardinality %d, expected ~%d", state, got, exp)
		}

		// Check measurement cardinality.
		if sketch, tsketch, err = index.MeasurementsSketches(); err != nil {
			t.Fatal(err)
		}

		if got, exp := int(sketch.Count()), measurements; got != exp { //got-exp < -delta(measurements) || got-exp > delta(measurements) {
			t.Errorf("[%s] got measurement cardinality %d, expected ~%d", state, got, exp)
		}

		if got, exp := int(tsketch.Count()), tmeasurements; got != exp { //got-exp < -delta(tmeasurements) || got-exp > delta(tmeasurements) {
			t.Errorf("[%s] got measurement tombstone cardinality %d, expected ~%d", state, got, exp)
		}
	}

	test := func(t *testing.T, index string) error {
		idx := MustNewIndex(index)
		if index, ok := idx.Index.(*tsi1.Index); ok {
			// Override the log file max size to force a log file compaction sooner.
			// This way, we will test the sketches are correct when they have been
			// compacted into IndexFiles, and also when they're loaded from
			// IndexFiles after a re-open.
			tsi1.WithMaximumLogFileSize(1 << 10)(index)
		}

		// Open the index
		idx.MustOpen()
		defer idx.Close()

		series := genTestSeries(10, 5, 3)
		// Add series to index.
		for _, serie := range series {
			if err := idx.AddSeries(serie.Measurement, serie.Tags.Map()); err != nil {
				t.Fatal(err)
			}
		}

		// Check cardinalities after adding series.
		checkCardinalities(t, idx, "initial", 2430, 0, 10, 0)

		// Re-open step only applies to the TSI index.
		if _, ok := idx.Index.(*tsi1.Index); ok {
			// Re-open the index.
			if err := idx.Reopen(); err != nil {
				panic(err)
			}

			// Check cardinalities after the reopen
			checkCardinalities(t, idx, "initial|reopen", 2430, 0, 10, 0)
		}

		// Drop some series
		if err := idx.DropMeasurement([]byte("measurement2")); err != nil {
			return err
		} else if err := idx.DropMeasurement([]byte("measurement5")); err != nil {
			return err
		}

		// Check cardinalities after the delete
		checkCardinalities(t, idx, "initial|reopen|delete", 2430, 486, 10, 2)

		// Re-open step only applies to the TSI index.
		if _, ok := idx.Index.(*tsi1.Index); ok {
			// Re-open the index.
			if err := idx.Reopen(); err != nil {
				panic(err)
			}

			// Check cardinalities after the reopen
			checkCardinalities(t, idx, "initial|reopen|delete|reopen", 2430, 486, 10, 2)
		}
		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(t, index); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Index wraps a series file and index.
type Index struct {
	tsdb.Index
	rootPath  string
	indexType string
	sfile     *tsdb.SeriesFile
}

type EngineOption func(opts *tsdb.EngineOptions)

// DisableTSICache allows the caller to disable the TSI bitset cache during a test.
var DisableTSICache = func() EngineOption {
	return func(opts *tsdb.EngineOptions) {
		opts.Config.SeriesIDSetCacheSize = 0
	}
}

// MustNewIndex will initialize a new index using the provide type. It creates
// everything under the same root directory so it can be cleanly removed on Close.
//
// The index will not be opened.
func MustNewIndex(index string, eopts ...EngineOption) *Index {
	opts := tsdb.NewEngineOptions()
	opts.IndexVersion = index

	for _, opt := range eopts {
		opt(&opts)
	}

	rootPath, err := ioutil.TempDir("", "influxdb-tsdb")
	if err != nil {
		panic(err)
	}

	seriesPath, err := ioutil.TempDir(rootPath, tsdb.SeriesFileDirectory)
	if err != nil {
		panic(err)
	}

	sfile := tsdb.NewSeriesFile(seriesPath)
	if err := sfile.Open(); err != nil {
		panic(err)
	}

	if index == inmem.IndexName {
		opts.InmemIndex = inmem.NewIndex("db0", sfile)
	}

	i, err := tsdb.NewIndex(0, "db0", filepath.Join(rootPath, "index"), tsdb.NewSeriesIDSet(), sfile, opts)
	if err != nil {
		panic(err)
	}

	if testing.Verbose() {
		i.WithLogger(logger.New(os.Stderr))
	}

	idx := &Index{
		Index:     i,
		indexType: index,
		rootPath:  rootPath,
		sfile:     sfile,
	}
	return idx
}

// MustOpenNewIndex will initialize a new index using the provide type and opens
// it.
func MustOpenNewIndex(index string, opts ...EngineOption) *Index {
	idx := MustNewIndex(index, opts...)
	idx.MustOpen()
	return idx
}

// MustOpen opens the underlying index or panics.
func (i *Index) MustOpen() {
	if err := i.Index.Open(); err != nil {
		panic(err)
	}
}

func (idx *Index) IndexSet() *tsdb.IndexSet {
	return &tsdb.IndexSet{Indexes: []tsdb.Index{idx.Index}, SeriesFile: idx.sfile}
}

func (idx *Index) AddSeries(name string, tags map[string]string) error {
	t := models.NewTags(tags)
	key := fmt.Sprintf("%s,%s", name, t.HashKey())
	return idx.CreateSeriesIfNotExists([]byte(key), []byte(name), t)
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

	opts := tsdb.NewEngineOptions()
	opts.IndexVersion = i.indexType
	if i.indexType == inmem.IndexName {
		opts.InmemIndex = inmem.NewIndex("db0", i.sfile)
	}

	idx, err := tsdb.NewIndex(0, "db0", filepath.Join(i.rootPath, "index"), tsdb.NewSeriesIDSet(), i.sfile, opts)
	if err != nil {
		return err
	}
	i.Index = idx
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
// BenchmarkIndexSet_TagSets/1M_series/inmem-8   	     100	  10430732 ns/op	 3556728 B/op	      51 allocs/op
// BenchmarkIndexSet_TagSets/1M_series/tsi1-8    	     100	  18995530 ns/op	 5221180 B/op	   20379 allocs/op
func BenchmarkIndexSet_TagSets(b *testing.B) {
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

	// setup writes all of the above points to the index.
	setup := func(idx *Index) {
		batchSize := 10000
		for j := 0; j < 1; j++ {
			for i := 0; i < len(keys); i += batchSize {
				k := keys[i : i+batchSize]
				n := names[i : i+batchSize]
				t := tags[i : i+batchSize]
				if err := idx.CreateSeriesListIfNotExists(k, n, t); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	// TODO(edd): refactor how we call into tag sets in the tsdb package.
	type indexTagSets interface {
		TagSets(name []byte, options query.IteratorOptions) ([]*query.TagSet, error)
	}

	var errResult error

	// This benchmark will merge eight bitsets each containing ~10,000 series IDs.
	b.Run("1M series", func(b *testing.B) {
		b.ReportAllocs()
		for _, indexType := range tsdb.RegisteredIndexes() {
			idx := MustOpenNewIndex(indexType)
			setup(idx)

			name := []byte("m4")
			opt := query.IteratorOptions{Condition: influxql.MustParseExpr(`"tag5"::tag = 'value0'`)}
			indexSet := tsdb.IndexSet{
				SeriesFile: idx.sfile,
				Indexes:    []tsdb.Index{idx.Index},
			} // For TSI implementation

			var ts func() ([]*query.TagSet, error)
			// TODO(edd): this is somewhat awkward. We should unify this difference somewhere higher
			// up than the engine. I don't want to open an engine do a benchmark on
			// different index implementations.
			if indexType == tsdb.InmemIndexName {
				ts = func() ([]*query.TagSet, error) {
					return idx.Index.(indexTagSets).TagSets(name, opt)
				}
			} else {
				ts = func() ([]*query.TagSet, error) {
					return indexSet.TagSets(idx.sfile, name, opt)
				}
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

	runBenchmark := func(b *testing.B, index string, queryN int, useTSICache bool) {
		var idx *Index
		if !useTSICache {
			idx = MustOpenNewIndex(index, DisableTSICache())
		} else {
			idx = MustOpenNewIndex(index)
		}

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
			for i := 0; i < len(keys); i += batchSize {
				k := keys[i : i+batchSize]
				n := names[i : i+batchSize]
				t := tags[i : i+batchSize]
				if err := idx.CreateSeriesListIfNotExists(k, n, t); err != nil {
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
			idx = MustOpenNewIndex(index)
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
						runBenchmark(b, indexType, queryN, true)
					})

					b.Run("no cache", func(b *testing.B) {
						runBenchmark(b, indexType, queryN, false)
					})
				})
			}
		})
	}
}
