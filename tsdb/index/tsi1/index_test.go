package tsi1_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Ensure index can return an iterator over all series in the index.
func TestIndex_SeriesIterator(t *testing.T) {
	idx := MustOpenIndex()
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
	if err := idx.MultiInvoke(func(state string) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.SeriesIterator()
		if itr == nil {
			t.Fatalf("expected iterator(%s)", state)
		}

		if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region east}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region west}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); string(e.Name()) != `mem` || e.Tags().String() != `[{region east}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil series(%s): %s/%s", state, e.Name(), e.Tags().String())
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk")},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "north"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify additional series.
	if err := idx.MultiInvoke(func(state string) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.SeriesIterator()
		if itr == nil {
			t.Fatalf("expected iterator(%s)", state)
		}

		if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region east}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region north}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region west}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); string(e.Name()) != `disk` || len(e.Tags()) != 0 {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); string(e.Name()) != `mem` || e.Tags().String() != `[{region east}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil series(%s): %s/%s", state, e.Name(), e.Tags().String())
		}
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure index can iterate over all measurement names.
func TestIndex_ForEachMeasurementName(t *testing.T) {
	idx := MustOpenIndex()
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
	if err := idx.MultiInvoke(func(state string) {
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
	}); err != nil {
		t.Fatal(err)
	}

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk")},
		{Name: []byte("mem")},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify new measurements.
	if err := idx.MultiInvoke(func(state string) {
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
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure index can return an iterator over all series for one measurement.
func TestIndex_MeasurementSeriesIterator(t *testing.T) {
	idx := MustOpenIndex()
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
	if err := idx.MultiInvoke(func(state string) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.MeasurementSeriesIterator([]byte("cpu"))
		if itr == nil {
			t.Fatalf("expected iterator(%s)", state)
		}

		if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region east}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region west}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil series(%s): %s/%s", state, e.Name(), e.Tags().String())
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk")},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "north"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify additional series.
	if err := idx.MultiInvoke(func(state string) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.MeasurementSeriesIterator([]byte("cpu"))
		if itr == nil {
			t.Fatalf("expected iterator(%s)", state)
		}

		if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region east}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region north}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); string(e.Name()) != `cpu` || e.Tags().String() != `[{region west}]` {
			t.Fatalf("unexpected series(%s): %s/%s", state, e.Name(), e.Tags().String())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil series(%s): %s/%s", state, e.Name(), e.Tags().String())
		}
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure index can return an iterator over all measurements for the index.
func TestIndex_MeasurementIterator(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Create initial set of series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu")},
		{Name: []byte("mem")},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify initial set of series.
	if err := idx.MultiInvoke(func(state string) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.MeasurementIterator()
		if itr == nil {
			t.Fatalf("expected iterator(%s)", state)
		}

		if e := itr.Next(); string(e.Name()) != `cpu` {
			t.Fatalf("unexpected measurement(%s): %s", state, e.Name())
		} else if e := itr.Next(); string(e.Name()) != `mem` {
			t.Fatalf("unexpected measurement(%s): %s", state, e.Name())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil measurement(%s): %s", state, e.Name())
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"foo": "bar"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "north", "x": "y"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify additional series.
	if err := idx.MultiInvoke(func(state string) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.MeasurementIterator()
		if itr == nil {
			t.Fatalf("expected iterator(%s)", state)
		}

		if e := itr.Next(); string(e.Name()) != `cpu` {
			t.Fatalf("unexpected measurement(%s): %s", state, e.Name())
		} else if e := itr.Next(); string(e.Name()) != `disk` {
			t.Fatalf("unexpected measurement(%s): %s", state, e.Name())
		} else if e := itr.Next(); string(e.Name()) != `mem` {
			t.Fatalf("unexpected measurement(%s): %s", state, e.Name())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil measurement(%s): %s", state, e.Name())
		}
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure index can return an iterator over all keys for one measurement.
func TestIndex_TagKeyIterator(t *testing.T) {
	idx := MustOpenIndex()
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
	if err := idx.MultiInvoke(func(state string) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.TagKeyIterator([]byte("cpu"))
		if itr == nil {
			t.Fatalf("expected iterator(%s)", state)
		}

		if e := itr.Next(); string(e.Key()) != `region` {
			t.Fatalf("unexpected key(%s): %s", state, e.Key())
		} else if e := itr.Next(); string(e.Key()) != `type` {
			t.Fatalf("unexpected key(%s): %s", state, e.Key())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil key(%s): %s", state, e.Key())
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"foo": "bar"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "north", "x": "y"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify additional series.
	if err := idx.MultiInvoke(func(state string) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.TagKeyIterator([]byte("cpu"))
		if itr == nil {
			t.Fatalf("expected iterator(%s)", state)
		}

		if e := itr.Next(); string(e.Key()) != `region` {
			t.Fatalf("unexpected key(%s): %s", state, e.Key())
		} else if e := itr.Next(); string(e.Key()) != `type` {
			t.Fatalf("unexpected key(%s): %s", state, e.Key())
		} else if e := itr.Next(); string(e.Key()) != `x` {
			t.Fatalf("unexpected key(%s): %s", state, e.Key())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil key(%s): %s", state, e.Key())
		}
	}); err != nil {
		t.Fatal(err)
	}
}

// Index is a test wrapper for tsi1.Index.
type Index struct {
	*tsi1.Index
}

// NewIndex returns a new instance of Index at a temporary path.
func NewIndex() *Index {
	idx := &Index{Index: tsi1.NewIndex()}
	idx.Path = MustTempDir()
	return idx
}

// MustOpenIndex returns a new, open index. Panic on error.
func MustOpenIndex() *Index {
	idx := NewIndex()
	if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
}

// Close closes and removes the index directory.
func (idx *Index) Close() error {
	defer os.RemoveAll(idx.Path)
	return idx.Index.Close()
}

// Reopen closes and opens the index.
func (idx *Index) Reopen() error {
	if err := idx.Index.Close(); err != nil {
		return err
	}

	path := idx.Path
	idx.Index = tsi1.NewIndex()
	idx.Path = path
	if err := idx.Open(); err != nil {
		return err
	}
	return nil
}

// MultiInvoke executes fn in several different states:
//
// - Immediately
// - After reopen
// - After compaction
// - After reopen again
//
// The index should always respond in the same fashion regardless of
// how data is stored. This helper allows the index to be easily tested
// in all major states.
func (idx *Index) MultiInvoke(fn func(state string)) error {
	// Invoke immediately.
	fn("initial")

	if testing.Verbose() {
		println("[index] reopening")
	}

	// Reopen and invoke again.
	if err := idx.Reopen(); err != nil {
		return fmt.Errorf("reopen error: %s", err)
	}
	fn("reopen")

	if testing.Verbose() {
		println("[index] forcing compaction")
	}

	// Force a compaction
	if err := idx.Compact(true); err != nil {
		return err
	}
	fn("post-compaction")

	if testing.Verbose() {
		println("[index] reopening after compaction")
	}

	// Reopen and invoke again.
	if err := idx.Reopen(); err != nil {
		return fmt.Errorf("post-compaction reopen error: %s", err)
	}
	fn("post-compaction-reopen")

	return nil
}

// CreateSeriesSliceIfNotExists creates multiple series at a time.
func (idx *Index) CreateSeriesSliceIfNotExists(a []Series) error {
	for i, s := range a {
		if err := idx.CreateSeriesIfNotExists(nil, s.Name, s.Tags); err != nil {
			return fmt.Errorf("i=%d, name=%s, tags=%s, err=%s", i, s.Name, s.Tags, err)
		}
	}
	return nil
}
