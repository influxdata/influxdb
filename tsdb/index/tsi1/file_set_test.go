package tsi1_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

// Ensure fileset can return an iterator over all series in the index.
func TestFileSet_SeriesIDIterator(t *testing.T) {
	idx := MustOpenIndex(1)
	defer idx.Close()

	// Create initial set of series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify initial set of series.
	idx.Run(t, func(t *testing.T) {
		fs, err := idx.PartitionAt(0).RetainFileSet()
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()

		itr := fs.SeriesFile().SeriesIDIterator()
		if itr == nil {
			t.Fatal("expected iterator")
		}
		if result := MustReadAllSeriesIDIteratorString(fs.SeriesFile(), itr); !reflect.DeepEqual(result, []string{
			"cpu,[{region east}]",
			"cpu,[{region west}]",
			"mem,[{region east}]",
		}) {
			t.Fatalf("unexpected keys: %s", result)
		}
	})

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk")},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "north"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify additional series.
	idx.Run(t, func(t *testing.T) {
		fs, err := idx.PartitionAt(0).RetainFileSet()
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()

		itr := fs.SeriesFile().SeriesIDIterator()
		if itr == nil {
			t.Fatal("expected iterator")
		}

		if result := MustReadAllSeriesIDIteratorString(fs.SeriesFile(), itr); !reflect.DeepEqual(result, []string{
			"cpu,[{region east}]",
			"cpu,[{region north}]",
			"cpu,[{region west}]",
			"disk,[]",
			"mem,[{region east}]",
		}) {
			t.Fatalf("unexpected keys: %s", result)
		}
	})
}

// Ensure fileset can return an iterator over all series for one measurement.
func TestFileSet_MeasurementSeriesIDIterator(t *testing.T) {
	idx := MustOpenIndex(1)
	defer idx.Close()

	// Create initial set of series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify initial set of series.
	idx.Run(t, func(t *testing.T) {
		fs, err := idx.PartitionAt(0).RetainFileSet()
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()

		itr := fs.MeasurementSeriesIDIterator([]byte("cpu"))
		if itr == nil {
			t.Fatal("expected iterator")
		}

		if result := MustReadAllSeriesIDIteratorString(fs.SeriesFile(), itr); !reflect.DeepEqual(result, []string{
			"cpu,[{region east}]",
			"cpu,[{region west}]",
		}) {
			t.Fatalf("unexpected keys: %s", result)
		}
	})

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk")},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "north"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify additional series.
	idx.Run(t, func(t *testing.T) {
		fs, err := idx.PartitionAt(0).RetainFileSet()
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()

		itr := fs.MeasurementSeriesIDIterator([]byte("cpu"))
		if itr == nil {
			t.Fatalf("expected iterator")
		}

		if result := MustReadAllSeriesIDIteratorString(fs.SeriesFile(), itr); !reflect.DeepEqual(result, []string{
			"cpu,[{region east}]",
			"cpu,[{region north}]",
			"cpu,[{region west}]",
		}) {
			t.Fatalf("unexpected keys: %s", result)
		}
	})
}

// Ensure fileset can return an iterator over all measurements for the index.
func TestFileSet_MeasurementIterator(t *testing.T) {
	idx := MustOpenIndex(1)
	defer idx.Close()

	// Create initial set of series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu")},
		{Name: []byte("mem")},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify initial set of series.
	idx.Run(t, func(t *testing.T) {
		fs, err := idx.PartitionAt(0).RetainFileSet()
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()

		itr := fs.MeasurementIterator()
		if itr == nil {
			t.Fatal("expected iterator")
		}

		expectedNames := []string{"cpu", "mem", ""} // Empty string implies end
		for _, name := range expectedNames {
			e := itr.Next()
			if name == "" && e != nil {
				t.Errorf("got measurement %s, expected nil measurement", e.Name())
			} else if e == nil && name != "" {
				t.Errorf("got nil measurement, expected %s", name)
			} else if e != nil && string(e.Name()) != name {
				t.Errorf("got measurement %s, expected %s", e.Name(), name)
			}
		}
	})

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"foo": "bar"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "north", "x": "y"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify additional series.
	idx.Run(t, func(t *testing.T) {
		fs, err := idx.PartitionAt(0).RetainFileSet()
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()

		itr := fs.MeasurementIterator()
		if itr == nil {
			t.Fatal("expected iterator")
		}

		expectedNames := []string{"cpu", "disk", "mem", ""} // Empty string implies end
		for _, name := range expectedNames {
			e := itr.Next()
			if name == "" && e != nil {
				t.Errorf("got measurement %s, expected nil measurement", e.Name())
			} else if e == nil && name != "" {
				t.Errorf("got nil measurement, expected %s", name)
			} else if e != nil && string(e.Name()) != name {
				t.Errorf("got measurement %s, expected %s", e.Name(), name)
			}
		}
	})
}

// Ensure fileset can return an iterator over all keys for one measurement.
func TestFileSet_TagKeyIterator(t *testing.T) {
	idx := MustOpenIndex(1)
	defer idx.Close()

	// Create initial set of series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west", "type": "gpu"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east", "misc": "other"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify initial set of series.
	idx.Run(t, func(t *testing.T) {
		fs, err := idx.PartitionAt(0).RetainFileSet()
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()

		itr := fs.TagKeyIterator([]byte("cpu"))
		if itr == nil {
			t.Fatalf("expected iterator")
		}

		if e := itr.Next(); string(e.Key()) != `region` {
			t.Fatalf("unexpected key: %s", e.Key())
		} else if e := itr.Next(); string(e.Key()) != `type` {
			t.Fatalf("unexpected key: %s", e.Key())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil key: %s", e.Key())
		}
	})

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"foo": "bar"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "north", "x": "y"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify additional series.
	idx.Run(t, func(t *testing.T) {
		fs, err := idx.PartitionAt(0).RetainFileSet()
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()

		itr := fs.TagKeyIterator([]byte("cpu"))
		if itr == nil {
			t.Fatal("expected iterator")
		}

		if e := itr.Next(); string(e.Key()) != `region` {
			t.Fatalf("unexpected key: %s", e.Key())
		} else if e := itr.Next(); string(e.Key()) != `type` {
			t.Fatalf("unexpected key: %s", e.Key())
		} else if e := itr.Next(); string(e.Key()) != `x` {
			t.Fatalf("unexpected key: %s", e.Key())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil key: %s", e.Key())
		}
	})
}

func MustReadAllSeriesIDIteratorString(sfile *tsdb.SeriesFile, itr tsdb.SeriesIDIterator) []string {
	// Read all ids.
	ids, err := tsdb.ReadAllSeriesIDIterator(itr)
	if err != nil {
		panic(err)
	}

	// Convert to keys and sort.
	keys := sfile.SeriesKeys(ids)
	sort.Slice(keys, func(i, j int) bool { return tsdb.CompareSeriesKeys(keys[i], keys[j]) == -1 })

	// Convert to strings.
	a := make([]string, len(keys))
	for i := range a {
		name, tags := tsdb.ParseSeriesKey(keys[i])
		a[i] = fmt.Sprintf("%s,%s", name, tags.String())
	}
	return a
}
