package tsdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/slices"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
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
		idx := MustNewIndex(name)
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

type Index struct {
	tsdb.Index
	rootPath string
	sfile    *tsdb.SeriesFile
}

func MustNewIndex(index string) *Index {
	opts := tsdb.NewEngineOptions()
	opts.IndexVersion = index

	rootPath, err := ioutil.TempDir("", "influxdb-tsdb")
	if err != nil {
		panic(err)
	}

	file, err := ioutil.TempFile(rootPath, "series")
	if err != nil {
		panic(err)
	}
	file.Close()

	sfile := tsdb.NewSeriesFile(file.Name())
	// If we're running on a 32-bit system then reduce the SeriesFile size, so we
	// can address is in memory.
	if runtime.GOARCH == "386" {
		sfile.MaxSize = 100000000 // 100M bytes
	}

	if err := sfile.Open(); err != nil {
		panic(err)
	}

	if index == inmem.IndexName {
		opts.InmemIndex = inmem.NewIndex("db0", sfile)
	}

	idx := &Index{
		Index:    tsdb.MustOpenIndex(0, "db0", filepath.Join(rootPath, "index"), sfile, opts),
		rootPath: rootPath,
		sfile:    sfile,
	}
	return idx
}

func (idx *Index) IndexSet() *tsdb.IndexSet {
	return &tsdb.IndexSet{Indexes: []tsdb.Index{idx.Index}, SeriesFile: idx.sfile}
}

func (idx *Index) AddSeries(name string, tags map[string]string) error {
	t := models.NewTags(tags)
	key := fmt.Sprintf("%s,%s", name, t.HashKey())
	return idx.CreateSeriesIfNotExists([]byte(key), []byte(name), t)
}

func (i *Index) Close() error {
	if err := i.Index.Close(); err != nil {
		return err
	}

	if err := i.sfile.Close(); err != nil {
		return err
	}
	return os.RemoveAll(i.rootPath)
}
