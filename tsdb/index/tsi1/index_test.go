package tsi1_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

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

// Ensure index can return whether a measurement exists.
func TestIndex_MeasurementExists(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify measurement exists.
	if err := idx.MultiInvoke(func(state string) {
		if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
			t.Fatalf("%s: %s", state, err)
		} else if !v {
			t.Fatalf("%s: expected measurement to exist", state)
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Delete one series.
	if err := idx.DropSeries([][]byte{models.MakeKey([]byte("cpu"), models.NewTags(map[string]string{"region": "east"}))}); err != nil {
		t.Fatal(err)
	}

	// Verify measurement still exists.
	if err := idx.MultiInvoke(func(state string) {
		if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
			t.Fatalf("%s: %s", state, err)
		} else if !v {
			t.Fatalf("%s: expected measurement to still exist", state)
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Delete second series.
	if err := idx.DropSeries([][]byte{models.MakeKey([]byte("cpu"), models.NewTags(map[string]string{"region": "west"}))}); err != nil {
		t.Fatal(err)
	}

	// Verify measurement is now deleted.
	if err := idx.MultiInvoke(func(state string) {
		if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
			t.Fatalf("%s: %s", state, err)
		} else if v {
			t.Fatalf("%s: expected measurement to be deleted", state)
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
