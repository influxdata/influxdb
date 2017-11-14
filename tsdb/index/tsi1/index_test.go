package tsi1_test

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"github.com/influxdata/influxql"
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

	// Delete one series.
	if err := idx.DropSeries(models.MakeKey([]byte("cpu"), models.NewTags(map[string]string{"region": "east"})), 0); err != nil {
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
	if err := idx.DropSeries(models.MakeKey([]byte("cpu"), models.NewTags(map[string]string{"region": "west"})), 0); err != nil {
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
func TestIndex_MeasurementNamesByExpr(t *testing.T) {
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

	// Retrieve measurements by expression
	idx.Run(t, func(t *testing.T) {
		t.Run("EQ", func(t *testing.T) {
			names, err := idx.MeasurementNamesByExpr(influxql.MustParseExpr(`region = 'west'`))
			if err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(names, [][]byte{[]byte("cpu"), []byte("mem")}) {
				t.Fatalf("unexpected names: %v", names)
			}
		})

		t.Run("NEQ", func(t *testing.T) {
			names, err := idx.MeasurementNamesByExpr(influxql.MustParseExpr(`region != 'east'`))
			if err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(names, [][]byte{[]byte("disk"), []byte("mem")}) {
				t.Fatalf("unexpected names: %v", names)
			}
		})

		t.Run("EQREGEX", func(t *testing.T) {
			names, err := idx.MeasurementNamesByExpr(influxql.MustParseExpr(`region =~ /east|west/`))
			if err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(names, [][]byte{[]byte("cpu"), []byte("mem")}) {
				t.Fatalf("unexpected names: %v", names)
			}
		})

		t.Run("NEQREGEX", func(t *testing.T) {
			names, err := idx.MeasurementNamesByExpr(influxql.MustParseExpr(`country !~ /^u/`))
			if err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(names, [][]byte{[]byte("cpu"), []byte("disk")}) {
				t.Fatalf("unexpected names: %v", names)
			}
		})
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
		fs := idx.PartitionAt(0).RetainFileSet()
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

// Index is a test wrapper for tsi1.Index.
type Index struct {
	*tsi1.Index
}

// NewIndex returns a new instance of Index at a temporary path.
func NewIndex() *Index {
	return &Index{Index: tsi1.NewIndex(tsi1.WithPath(MustTempDir()))}
}

// MustOpenIndex returns a new, open index. Panic on error.
func MustOpenIndex(partitionN uint64) *Index {
	idx := NewIndex()
	idx.PartitionN = partitionN
	if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
}

// Close closes and removes the index directory.
func (idx *Index) Close() error {
	defer os.RemoveAll(idx.Path())
	return idx.Index.Close()
}

// Reopen closes and opens the index.
func (idx *Index) Reopen() error {
	if err := idx.Index.Close(); err != nil {
		return err
	}

	path, partitionN := idx.Path(), idx.PartitionN

	idx.Index = tsi1.NewIndex(tsi1.WithPath(path))
	idx.PartitionN = partitionN
	if err := idx.Open(); err != nil {
		return err
	}
	return nil
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
	for i, s := range a {
		if err := idx.CreateSeriesListIfNotExists(nil, [][]byte{s.Name}, []models.Tags{s.Tags}); err != nil {
			return fmt.Errorf("i=%d, name=%s, tags=%v, err=%s", i, s.Name, s.Tags, err)
		}
	}
	return nil
}
