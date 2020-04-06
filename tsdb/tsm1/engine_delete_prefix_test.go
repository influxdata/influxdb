package tsm1_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
)

func TestEngine_DeletePrefix(t *testing.T) {
	// Create a few points.
	p1 := MustParsePointString("cpu,host=0 value=1.1 6", "mm0")
	p2 := MustParsePointString("cpu,host=A value=1.2 2", "mm0")
	p3 := MustParsePointString("cpu,host=A value=1.3 3", "mm0")
	p4 := MustParsePointString("cpu,host=B value=1.3 4", "mm0")
	p5 := MustParsePointString("cpu,host=B value=1.3 5", "mm0")
	p6 := MustParsePointString("cpu,host=C value=1.3 1", "mm0")
	p7 := MustParsePointString("mem,host=C value=1.3 1", "mm1")
	p8 := MustParsePointString("disk,host=C value=1.3 1", "mm2")

	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if err := e.writePoints(p1, p2, p3, p4, p5, p6, p7, p8); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.WriteSnapshot(context.Background(), tsm1.CacheStatusColdNoWrites); err != nil {
		t.Fatalf("failed to snapshot: %s", err.Error())
	}

	keys := e.FileStore.Keys()
	if exp, got := 6, len(keys); exp != got {
		t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
	}

	if err := e.DeletePrefixRange(context.Background(), []byte("mm0"), 0, 3, nil); err != nil {
		t.Fatalf("failed to delete series: %v", err)
	}

	keys = e.FileStore.Keys()
	if exp, got := 4, len(keys); exp != got {
		t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
	}

	exp := map[string]byte{
		"mm0,\x00=cpu,host=0,\xff=value#!~#value":  0,
		"mm0,\x00=cpu,host=B,\xff=value#!~#value":  0,
		"mm1,\x00=mem,host=C,\xff=value#!~#value":  0,
		"mm2,\x00=disk,host=C,\xff=value#!~#value": 0,
	}
	if !reflect.DeepEqual(keys, exp) {
		t.Fatalf("unexpected series in file store: %v != %v", keys, exp)
	}

	// Check that the series still exists in the index
	iter, err := e.index.MeasurementSeriesIDIterator([]byte("mm0"))
	if err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	defer iter.Close()

	elem, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	if elem.SeriesID.IsZero() {
		t.Fatalf("series index mismatch: EOF, exp 2 series")
	}

	// Lookup series.
	name, tags := e.sfile.Series(elem.SeriesID)
	if got, exp := name, []byte("mm0"); !bytes.Equal(got, exp) {
		t.Fatalf("series mismatch: got %s, exp %s", got, exp)
	}

	if !tags.Equal(models.NewTags(map[string]string{models.FieldKeyTagKey: "value", models.MeasurementTagKey: "cpu", "host": "0"})) && !tags.Equal(models.NewTags(map[string]string{models.FieldKeyTagKey: "value", models.MeasurementTagKey: "cpu", "host": "B"})) {
		t.Fatalf(`series mismatch: got %s, exp either "host=0" or "host=B"`, tags)
	}
	iter.Close()

	// Deleting remaining series should remove them from the series.
	if err := e.DeletePrefixRange(context.Background(), []byte("mm0"), 0, 9, nil); err != nil {
		t.Fatalf("failed to delete series: %v", err)
	}

	keys = e.FileStore.Keys()
	if exp, got := 2, len(keys); exp != got {
		t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
	}

	exp = map[string]byte{
		"mm1,\x00=mem,host=C,\xff=value#!~#value":  0,
		"mm2,\x00=disk,host=C,\xff=value#!~#value": 0,
	}
	if !reflect.DeepEqual(keys, exp) {
		t.Fatalf("unexpected series in file store: %v != %v", keys, exp)
	}

	if iter, err = e.index.MeasurementSeriesIDIterator([]byte("mm0")); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if iter != nil {
		defer iter.Close()
		if elem, err = iter.Next(); err != nil {
			t.Fatal(err)
		}
		if !elem.SeriesID.IsZero() {
			t.Fatalf("got an undeleted series id, but series should be dropped from index")
		}
	}
}

func BenchmarkEngine_DeletePrefixRange(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e, err := NewEngine(tsm1.NewConfig(), b)
		if err != nil {
			b.Fatal(err)
		} else if err := e.Open(context.Background()); err != nil {
			b.Fatal(err)
		}
		defer e.Close()

		const n = 100000
		var points []models.Point
		for i := 0; i < n; i++ {
			points = append(points, MustParsePointString(fmt.Sprintf("cpu,host=A%d value=1", i), "mm0"))
			points = append(points, MustParsePointString(fmt.Sprintf("cpu,host=B%d value=1", i), "mm1"))
		}
		if err := e.writePoints(points...); err != nil {
			b.Fatal(err)
		}

		if err := e.WriteSnapshot(context.Background(), tsm1.CacheStatusColdNoWrites); err != nil {
			b.Fatal(err)
		} else if got, want := len(e.FileStore.Keys()), n*2; got != want {
			b.Fatalf("len(Keys())=%d, want %d", got, want)
		}
		b.StartTimer()

		if err := e.DeletePrefixRange(context.Background(), []byte("mm0"), 0, 3, nil); err != nil {
			b.Fatal(err)
		} else if err := e.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
