package tsi1_test

import (
	"testing"

	"github.com/influxdata/influxdb/models"
)

// Ensure fileset can return an iterator over all series in the index.
func TestFileSet_SeriesIDIterator(t *testing.T) {
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
	idx.Run(t, func(t *testing.T) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.SeriesIDIterator()
		if itr == nil {
			t.Fatal("expected iterator")
		}

		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region west}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `mem` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if e := itr.Next(); e.SeriesID != 0 {
			t.Fatalf("expected eof, got: %d", e.SeriesID)
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
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.SeriesIDIterator()
		if itr == nil {
			t.Fatal("expected iterator")
		}

		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region west}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `mem` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `disk` || tags.String() != `[]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region north}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if e := itr.Next(); e.SeriesID != 0 {
			name, tags := fs.SeriesFile().Series(e.SeriesID)
			t.Fatalf("expected eof, got: %s/%s", name, tags.String())
		}
	})
}

// Ensure fileset can return an iterator over all series for one measurement.
func TestFileSet_MeasurementSeriesIDIterator(t *testing.T) {
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
	idx.Run(t, func(t *testing.T) {
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.MeasurementSeriesIDIterator([]byte("cpu"))
		if itr == nil {
			t.Fatal("expected iterator")
		}

		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region west}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if e := itr.Next(); e.SeriesID != 0 {
			t.Fatalf("expected eof, got: %d", e.SeriesID)
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
		fs := idx.RetainFileSet()
		defer fs.Release()

		itr := fs.MeasurementSeriesIDIterator([]byte("cpu"))
		if itr == nil {
			t.Fatalf("expected iterator")
		}

		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region east}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region north}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if name, tags := fs.SeriesFile().Series(itr.Next().SeriesID); string(name) != `cpu` || tags.String() != `[{region west}]` {
			t.Fatalf("unexpected series: %s/%s", name, tags.String())
		}
		if e := itr.Next(); e.SeriesID != 0 {
			t.Fatalf("expected eof, got: %d", e.SeriesID)
		}
	})
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
	idx.Run(t, func(t *testing.T) {
		fs := idx.RetainFileSet()
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
		fs := idx.RetainFileSet()
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
	idx.Run(t, func(t *testing.T) {
		fs := idx.RetainFileSet()
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
		fs := idx.RetainFileSet()
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

/*
var (
	byteSliceResult [][]byte
	tagsSliceResult []models.Tags
)

func BenchmarkFileset_FilterNamesTags(b *testing.B) {
	idx := MustOpenIndex()
	defer idx.Close()

	allNames := make([][]byte, 0, 2000*1000)
	allTags := make([]models.Tags, 0, 2000*1000)

	for i := 0; i < 2000; i++ {
		for j := 0; j < 1000; j++ {
			name := []byte(fmt.Sprintf("measurement-%d", i))
			tags := models.NewTags(map[string]string{"host": fmt.Sprintf("server-%d", j)})
			allNames = append(allNames, name)
			allTags = append(allTags, tags)
		}
	}

	if err := idx.CreateSeriesListIfNotExists(nil, allNames, allTags); err != nil {
		b.Fatal(err)
	}
	// idx.CheckFastCompaction()

	fs := idx.RetainFileSet()
	defer fs.Release()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		names := [][]byte{
			[]byte("foo"),
			[]byte("measurement-222"), // filtered
			[]byte("measurement-222"), // kept (tags won't match)
			[]byte("measurements-1"),
			[]byte("measurement-900"), // filtered
			[]byte("measurement-44444"),
			[]byte("bar"),
		}

		tags := []models.Tags{
			nil,
			models.NewTags(map[string]string{"host": "server-297"}), // filtered
			models.NewTags(map[string]string{"host": "wrong"}),
			nil,
			models.NewTags(map[string]string{"host": "server-1026"}), // filtered
			models.NewTags(map[string]string{"host": "server-23"}),   // kept (measurement won't match)
			models.NewTags(map[string]string{"host": "zoo"}),
		}
		b.StartTimer()
		byteSliceResult, tagsSliceResult = fs.FilterNamesTags(names, tags)
	}
}
*/
