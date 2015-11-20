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

// Tests that duplicate point values are merged
func TestKeyIterator_WALSegment_Duplicate(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=A value=1 1000000000")
	p2 := parsePoint("cpu,host=A value=2 1000000000")

	points := []models.Point{p1, p2}

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

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		readValues = true
		assertEqual(t, values[0], p2, "value")
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

// Tests that merge iterator over a wal returns points order correctly.
func TestMergeIteragor_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=A value=1 1000000000")
	p2 := parsePoint("cpu,host=A value=2 2000000000")

	points := []models.Point{p1, p2}

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

	m := tsm1.NewMergeIterator(iter, 1)

	for _, p := range points {
		if !m.Next() {
			t.Fatalf("expected next, got false")
		}

		key, values, err := m.Read()
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		assertEqual(t, values[0], p, "value")
	}
}

// Tests that merge iterator over a wal returns points order by key and time.
func TestMergeIteragor_MultipleKeys(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=A value=1 1000000000")
	p2 := parsePoint("cpu,host=B value=1 1000000000")
	p3 := parsePoint("cpu,host=A value=2 2000000000")
	p4 := parsePoint("cpu,host=B value=2 2000000000")
	p5 := parsePoint("cpu,host=A value=3 1000000000") // overwrites p1

	points := []models.Point{p1, p2, p3, p4, p5}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points[:2],
		},
		&tsm1.WriteWALEntry{
			Points: points[2:],
		},
	}
	r := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	m := tsm1.NewMergeIterator(iter, 2)

	var data = []struct {
		key    string
		points []models.Point
		field  string
	}{
		{"cpu,host=A#!~#value", []models.Point{p5, p3}, "value"},
		{"cpu,host=B#!~#value", []models.Point{p2, p4}, "value"},
	}

	for _, p := range data {
		if !m.Next() {
			t.Fatalf("expected next, got false")
		}

		key, values, err := m.Read()
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := key, p.key; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for i, point := range p.points {
			assertEqual(t, values[i], point, p.field)
		}
	}
}

// Tests that the merge iterator does not pull in deleted WAL entries.
func TestMergeIteragor_DeletedKeys(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	p1 := parsePoint("cpu,host=A value=1 1000000000")
	p2 := parsePoint("cpu,host=B value=1 1000000000")
	p3 := parsePoint("cpu,host=A value=2 2000000000")
	p4 := parsePoint("cpu,host=B value=2 2000000000")
	p5 := parsePoint("cpu,host=A value=3 1000000000") // overwrites p1

	points := []models.Point{p1, p2, p3, p4, p5}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Points: points[:2],
		},
		&tsm1.WriteWALEntry{
			Points: points[2:],
		},
		&tsm1.DeleteWALEntry{
			Keys: []string{"cpu,host=A"},
		},
	}
	r := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	m := tsm1.NewMergeIterator(iter, 2)

	var data = []struct {
		key    string
		points []models.Point
		field  string
	}{
		{"cpu,host=B#!~#value", []models.Point{p2, p4}, "value"},
	}

	for _, p := range data {
		if !m.Next() {
			t.Fatalf("expected next, got false")
		}

		key, values, err := m.Read()
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := key, p.key; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for i, point := range p.points {
			assertEqual(t, values[i], point, p.field)
		}
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
