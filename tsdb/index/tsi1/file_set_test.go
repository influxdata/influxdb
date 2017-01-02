package tsi1_test

import (
	"testing"

	"github.com/influxdata/influxdb/models"
)

// Ensure fileset can return an iterator over all series in the index.
func TestFileSet_SeriesIterator(t *testing.T) {
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

// Ensure fileset can return an iterator over all series for one measurement.
func TestFileSet_MeasurementSeriesIterator(t *testing.T) {
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

// Ensure fileset can return an iterator over all measurements for the index.
func TestFileSet_MeasurementIterator(t *testing.T) {
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

// Ensure fileset can return an iterator over all keys for one measurement.
func TestFileSet_TagKeyIterator(t *testing.T) {
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
