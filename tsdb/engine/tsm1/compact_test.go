package tsm1_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

// Tests that a single WAL segment can be read and iterated over
func TestKeyIterator_WALSegment_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	points := []models.Point{p1}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points,
		},
	}
	r := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), len(points); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for i, v := range values {
			readValues = true
			assertEqual(t, v, points[i], "value")
		}
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that a multiple WAL segment can be read and iterated over
func TestKeyIterator_WALSegment_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=A value=1 1000000000")
	points1 := []models.Point{p1}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	p2 := parsePoint("cpu,host=A value=2 2000000000")
	points2 := []models.Point{p2}

	entries = []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points2,
		},
	}
	r2 := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 2; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}
		readValues = true

		assertEqual(t, values[0], p1, "value")
		assertEqual(t, values[1], p2, "value")
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that a multiple WAL segments with out of order points are
// sorted while iterating
func TestKeyIterator_WALSegment_MultiplePointsSorted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=A value=2 2000000000")
	points1 := []models.Point{p1}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	p2 := parsePoint("cpu,host=A value=1 1000000000")
	points2 := []models.Point{p2}

	entries = []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points2,
		},
	}
	r2 := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 2; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}
		readValues = true

		assertEqual(t, values[0], p2, "value")
		assertEqual(t, values[1], p1, "value")
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that multipel keys are iterated over in sorted order
func TestKeyIterator_WALSegment_MultipleKeysSorted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=B value=1 1000000000")
	points1 := []models.Point{p1}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	p2 := parsePoint("cpu,host=A value=1 1000000000")
	points2 := []models.Point{p2}

	entries = []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points2,
		},
	}
	r2 := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	var data = []struct {
		key   string
		point models.Point
	}{
		{"cpu,host=A#!~#value", p2},
		{"cpu,host=B#!~#value", p1},
	}

	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, data[0].key; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}
		readValues = true

		assertEqual(t, values[0], data[0].point, "value")
		data = data[1:]
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that deletes after writes removes the previous written values
func TestKeyIterator_WALSegment_MultipleKeysDeleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=A value=1 1000000000")
	points1 := []models.Point{p1}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	p2 := parsePoint("cpu,host=A count=1 1000000000")
	p3 := parsePoint("cpu,host=B value=1 1000000000")
	points2 := []models.Point{p2, p3}

	entries = []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points2,
		},
		&tsm1.DeleteWALEntry{
			Keys: []string{
				"cpu,host=A",
			},
		},
	}
	r2 := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	var data = []struct {
		key   string
		point models.Point
	}{
		{"cpu,host=B#!~#value", p3},
	}

	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, data[0].key; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}
		readValues = true

		assertEqual(t, values[0], data[0].point, "value")
		data = data[1:]
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that writes, deletes followed by more writes returns the the
// correct values.
func TestKeyIterator_WALSegment_WriteAfterDelete(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=A value=1 1000000000")
	points1 := []models.Point{p1}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	p2 := parsePoint("cpu,host=A count=1 1000000000")
	p3 := parsePoint("cpu,host=B value=1 1000000000")
	points2 := []models.Point{p2, p3}

	entries = []tsm1.WALEntry{
		// Delete would remove p1
		&tsm1.DeleteWALEntry{
			Keys: []string{
				"cpu,host=A",
			},
		},
		&tsm1.WriteWALEntry{
			Points: points2,
		},
	}
	r2 := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	var data = []struct {
		key   string
		point models.Point
		field string
	}{
		{"cpu,host=A#!~#count", p2, "count"},
		{"cpu,host=B#!~#value", p3, "value"},
	}

	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, data[0].key; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}
		readValues = true

		assertEqual(t, values[0], data[0].point, data[0].field)
		data = data[1:]
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

func assertEqual(t *testing.T, a tsm1.Value, b models.Point, field string) {
	if got, exp := a.Time(), b.Time(); !got.Equal(exp) {
		t.Fatalf("time mismatch: got %v, exp %v", got, exp)
	}
	if got, exp := a.Value(), b.Fields()[field]; got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func MustWALSegment(dir string, entries []tsm1.WALEntry) *tsm1.WALSegmentReader {
	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	for _, e := range entries {
		if err := w.Write(e); err != nil {
			panic(fmt.Sprintf("write WAL entry: %v", err))
		}
	}

	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		panic(fmt.Sprintf("seek WAL: %v", err))
	}

	return tsm1.NewWALSegmentReader(f)
}
