package tsdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/slices"
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

func TestIndex_Sketches(t *testing.T) {
	checkCardinalities := func(t *testing.T, index *Index, state string, series, tseries, measurements, tmeasurements int) {
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

// MustNewIndex will initialize a new index using the provide type. It creates
// everything under the same root directory so it can be cleanly removed on Close.
//
// The index will not be opened.
func MustNewIndex(index string) *Index {
	opts := tsdb.NewEngineOptions()
	opts.IndexVersion = index

	rootPath, err := ioutil.TempDir("", "influxdb-tsdb")
	fmt.Println(rootPath)
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
func MustOpenNewIndex(index string) *Index {
	idx := MustNewIndex(index)
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
