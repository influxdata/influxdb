package tsi1_test

import (
	"testing"

	"github.com/influxdata/influxdb/models"
)

// Ensure fileset can return an iterator over all series in the index.
func TestFileSet_SeriesIDIterator(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	idx := MustOpenIndex(sfile.SeriesFile, 1)
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

		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := fs.SeriesFile().Series(elem.SeriesID); string(name) != `cpu` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := fs.SeriesFile().Series(elem.SeriesID); string(name) != `cpu` || tags.String() != `[{region west}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := fs.SeriesFile().Series(elem.SeriesID); string(name) != `mem` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if elem.SeriesID != 0 {
			t.Fatalf("expected eof, got: %d", elem.SeriesID)
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

		allexpected := []struct {
			name   string
			tagset string
		}{
			{`cpu`, `[{region east}]`},
			{`cpu`, `[{region west}]`},
			{`mem`, `[{region east}]`},
			{`disk`, `[]`},
			{`cpu`, `[{region north}]`},
		}

		for _, expected := range allexpected {
			e, err := itr.Next()
			if err != nil {
				t.Fatal(err)
			}

			if name, tags := fs.SeriesFile().Series(e.SeriesID); string(name) != expected.name || tags.String() != expected.tagset {
				t.Fatalf("unexpected series: %s/%s", name, tags.String())
			}
		}

		// Check for end of iterator...
		e, err := itr.Next()
		if err != nil {
			t.Fatal(err)
		}

		if e.SeriesID != 0 {
			name, tags := fs.SeriesFile().Series(e.SeriesID)
			t.Fatalf("got: %s/%s, but expected EOF", name, tags.String())
		}
	})
}

// Ensure fileset can return an iterator over all series for one measurement.
func TestFileSet_MeasurementSeriesIDIterator(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	idx := MustOpenIndex(sfile.SeriesFile, 1)
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

		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := fs.SeriesFile().Series(elem.SeriesID); string(name) != `cpu` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := fs.SeriesFile().Series(elem.SeriesID); string(name) != `cpu` || tags.String() != `[{region west}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if elem.SeriesID != 0 {
			t.Fatalf("expected eof, got: %d", elem.SeriesID)
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

		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := fs.SeriesFile().Series(elem.SeriesID); string(name) != `cpu` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := fs.SeriesFile().Series(elem.SeriesID); string(name) != `cpu` || tags.String() != `[{region west}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := fs.SeriesFile().Series(elem.SeriesID); string(name) != `cpu` || tags.String() != `[{region north}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if elem.SeriesID != 0 {
			t.Fatalf("expected eof, got: %d", elem.SeriesID)
		}
	})
}

// Ensure fileset can return an iterator over all measurements for the index.
func TestFileSet_MeasurementIterator(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	idx := MustOpenIndex(sfile.SeriesFile, 1)
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

		if e := itr.Next(); string(e.Name()) != `cpu` {
			t.Fatalf("unexpected measurement: %s", e.Name())
		} else if e := itr.Next(); string(e.Name()) != `mem` {
			t.Fatalf("unexpected measurement: %s", e.Name())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil measurement: %s", e.Name())
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

		if e := itr.Next(); string(e.Name()) != `cpu` {
			t.Fatalf("unexpected measurement: %s", e.Name())
		} else if e := itr.Next(); string(e.Name()) != `disk` {
			t.Fatalf("unexpected measurement: %s", e.Name())
		} else if e := itr.Next(); string(e.Name()) != `mem` {
			t.Fatalf("unexpected measurement: %s", e.Name())
		} else if e := itr.Next(); e != nil {
			t.Fatalf("expected nil measurement: %s", e.Name())
		}
	})
}

// Ensure fileset can return an iterator over all keys for one measurement.
func TestFileSet_TagKeyIterator(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	idx := MustOpenIndex(sfile.SeriesFile, 1)
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
