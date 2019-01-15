package tsm1_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/models"
)

func TestEngine_DeletePrefix(t *testing.T) {
	// Create a few points.
	p1 := MustParsePointString("cpu,host=0 value=1.1 6")
	p2 := MustParsePointString("cpu,host=A value=1.2 2")
	p3 := MustParsePointString("cpu,host=A value=1.3 3")
	p4 := MustParsePointString("cpu,host=B value=1.3 4")
	p5 := MustParsePointString("cpu,host=B value=1.3 5")
	p6 := MustParsePointString("cpu,host=C value=1.3 1")
	p7 := MustParsePointString("mem,host=C value=1.3 1")
	p8 := MustParsePointString("disk,host=C value=1.3 1")

	e, err := NewEngine()
	if err != nil {
		t.Fatal(err)
	}

	// mock the planner so compactions don't run during the test
	e.CompactionPlan = &mockPlanner{}
	if err := e.Open(); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if err := e.writePoints(p1, p2, p3, p4, p5, p6, p7, p8); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.WriteSnapshot(); err != nil {
		t.Fatalf("failed to snapshot: %s", err.Error())
	}

	keys := e.FileStore.Keys()
	if exp, got := 6, len(keys); exp != got {
		t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
	}

	if err := e.DeleteBucket([]byte("cpu"), 0, 3); err != nil {
		t.Fatalf("failed to delete series: %v", err)
	}

	keys = e.FileStore.Keys()
	if exp, got := 4, len(keys); exp != got {
		t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
	}

	exp := map[string]byte{
		"cpu,host=0#!~#value":  0,
		"cpu,host=B#!~#value":  0,
		"disk,host=C#!~#value": 0,
		"mem,host=C#!~#value":  0,
	}
	if !reflect.DeepEqual(keys, exp) {
		t.Fatalf("unexpected series in file store: %v != %v", keys, exp)
	}

	// Check that the series still exists in the index
	iter, err := e.index.MeasurementSeriesIDIterator([]byte("cpu"))
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
	if got, exp := name, []byte("cpu"); !bytes.Equal(got, exp) {
		t.Fatalf("series mismatch: got %s, exp %s", got, exp)
	}

	if !tags.Equal(models.NewTags(map[string]string{"host": "0"})) && !tags.Equal(models.NewTags(map[string]string{"host": "B"})) {
		t.Fatalf(`series mismatch: got %s, exp either "host=0" or "host=B"`, tags)
	}
	iter.Close()

	// Deleting remaining series should remove them from the series.
	if err := e.DeleteBucket([]byte("cpu"), 0, 9); err != nil {
		t.Fatalf("failed to delete series: %v", err)
	}

	keys = e.FileStore.Keys()
	if exp, got := 2, len(keys); exp != got {
		t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
	}

	exp = map[string]byte{
		"disk,host=C#!~#value": 0,
		"mem,host=C#!~#value":  0,
	}
	if !reflect.DeepEqual(keys, exp) {
		t.Fatalf("unexpected series in file store: %v != %v", keys, exp)
	}

	if iter, err = e.index.MeasurementSeriesIDIterator([]byte("cpu")); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if iter == nil {
		return
	}

	defer iter.Close()
	if elem, err = iter.Next(); err != nil {
		t.Fatal(err)
	}
	if !elem.SeriesID.IsZero() {
		t.Fatalf("got an undeleted series id, but series should be dropped from index")
	}
}
