package tsm1_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

// Tests that a single WAL segment can be read and iterated over
func TestKeyIterator_WALSegment_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: writes,
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

		if got, exp := len(values), len(writes); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for _, v := range values {
			readValues = true
			assertValueEqual(t, v, v1)
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

	v1 := tsm1.NewValue(time.Unix(1, 0), int64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), int64(2))
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v2},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: writes,
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
		assertValueEqual(t, values[0], v2)
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that a multiple WAL segment can be read and iterated over
func TestKeyIterator_WALSegment_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), int64(1))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
	}

	r1 := MustWALSegment(dir, entries)

	v2 := tsm1.NewValue(time.Unix(2, 0), int64(2))
	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v2},
	}

	entries = []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points2,
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

		assertValueEqual(t, values[0], v1)
		assertValueEqual(t, values[1], v2)
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

	v1 := tsm1.NewValue(time.Unix(2, 0), int64(2))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	v2 := tsm1.NewValue(time.Unix(1, 0), int64(1))
	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v2},
	}

	entries = []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points2,
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

		assertValueEqual(t, values[0], v2)
		assertValueEqual(t, values[1], v1)
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that multiple keys are iterated over in sorted order
func TestKeyIterator_WALSegment_MultipleKeysSorted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	points1 := map[string][]tsm1.Value{
		"cpu,host=B#!~#value": []tsm1.Value{v1},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v2},
	}

	entries = []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points2,
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
		value tsm1.Value
	}{
		{"cpu,host=A#!~#value", v2},
		{"cpu,host=B#!~#value", v1},
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

		assertValueEqual(t, values[0], data[0].value)
		data = data[1:]
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// // Tests that deletes after writes removes the previous written values
func TestKeyIterator_WALSegment_MultipleKeysDeleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
	}

	r1 := MustWALSegment(dir, entries)

	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(1, 0), float64(1))

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#count": []tsm1.Value{v2},
		"cpu,host=B#!~#value": []tsm1.Value{v3},
	}

	entries = []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points2,
		},
		&tsm1.DeleteWALEntry{
			Keys: []string{
				"cpu,host=A#!~#count",
				"cpu,host=A#!~#value",
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
		value tsm1.Value
	}{
		{"cpu,host=B#!~#value", v3},
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

		assertValueEqual(t, values[0], data[0].value)
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

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
	}

	r1 := MustWALSegment(dir, entries)

	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(1, 0), float64(1))

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#count": []tsm1.Value{v2},
		"cpu,host=B#!~#value": []tsm1.Value{v3},
	}

	entries = []tsm1.WALEntry{
		&tsm1.DeleteWALEntry{
			Keys: []string{
				"cpu,host=A#!~#count",
				"cpu,host=A#!~#value",
			},
		},
		&tsm1.WriteWALEntry{
			Values: points2,
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
		value tsm1.Value
	}{
		{"cpu,host=A#!~#count", v2},
		{"cpu,host=B#!~#value", v3},
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

		assertValueEqual(t, values[0], data[0].value)
		data = data[1:]
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that merge iterator over a wal returns points order correctly.
func TestMergeIterator_WAL_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v2 := tsm1.NewValue(time.Unix(2, 0), float64(2))

	points := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v2},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points,
		},
	}
	r := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	// Read should return a chunk of 1 value
	m := tsm1.NewMergeIterator(nil, iter, 1, tsm1.MinTime, tsm1.MaxTime)
	var readValues bool
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
		readValues = true

		assertValueEqual(t, values[0], p[0])
	}
	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that merge iterator over a wal returns points order correctly.
func TestMergeIterator_TSM_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1.1))
	v2 := tsm1.NewValue(time.Unix(2, 0), float64(2.2))

	points := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v2},
	}

	r := MustTSMReader(dir, points)

	iter, err := tsm1.NewTSMKeyIterator(r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	// Read should return a chunk of 1 value
	m := tsm1.NewMergeIterator(iter, nil, 1, tsm1.MinTime, tsm1.MaxTime)
	var readValues bool
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
		readValues = true

		assertValueEqual(t, values[0], p[0])
	}
	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that merge iterator over a wal and tsm returns points order correctly.
func TestMergeIterator_WALTSM_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	v2 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v2},
	}
	r2 := MustTSMReader(dir, points2)

	wal, err := tsm1.NewWALKeyIterator(r1)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	tsm, err := tsm1.NewTSMKeyIterator(r2)
	if err != nil {
		t.Fatalf("unexpected error creating TSMKeyIterator: %v", err)
	}

	expected := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v2},
	}

	// Read should return a chunk of 1 value
	m := tsm1.NewMergeIterator(tsm, wal, 1, tsm1.MinTime, tsm1.MaxTime)
	var readValues bool
	for _, p := range expected {
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
		readValues = true

		assertValueEqual(t, values[0], p[0])
	}
	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that merge iterator over a wal and tsm returns points order correctly
// and filtered between [minTime, maxTime)
func TestMergeIterator_WALTSM_TimeFilter(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v2 := tsm1.NewValue(time.Unix(2, 0), float64(2))

	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v2},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	v3 := tsm1.NewValue(time.Unix(3, 0), float64(2))
	v4 := tsm1.NewValue(time.Unix(4, 0), float64(2))

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v3, v4},
	}
	r2 := MustTSMReader(dir, points2)

	wal, err := tsm1.NewWALKeyIterator(r1)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	tsm, err := tsm1.NewTSMKeyIterator(r2)
	if err != nil {
		t.Fatalf("unexpected error creating TSMKeyIterator: %v", err)
	}

	expected := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v2, v4},
	}

	// Read should return a chunk of 1 value
	m := tsm1.NewMergeIterator(tsm, wal, 2, time.Unix(2, 0), time.Unix(3, 1))
	var readValues bool
	for _, p := range expected {
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

		spew.Dump(values)
		if got, exp := len(values), len(expected["cpu,host=A#!~#value"]); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}
		readValues = true

		assertValueEqual(t, values[0], p[0])
	}
	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that merge iterator over a wal and tsm returns wal points
// with matching timestamps in tsm overwritten
func TestMergeIterator_WALTSM_Overwrite(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(2))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v2},
	}
	r2 := MustTSMReader(dir, points2)

	wal, err := tsm1.NewWALKeyIterator(r1)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	tsm, err := tsm1.NewTSMKeyIterator(r2)
	if err != nil {
		t.Fatalf("unexpected error creating TSMKeyIterator: %v", err)
	}

	expected := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	// Read should return a chunk of 1 value
	m := tsm1.NewMergeIterator(tsm, wal, 1, tsm1.MinTime, tsm1.MaxTime)
	var readValues bool
	for _, p := range expected {
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
		readValues = true

		assertValueEqual(t, values[0], p[0])
	}
	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that merge iterator over a wal and tsm returns wal points
// sorted by key and time with tsm points
func TestMergeIterator_WALTSM_MultipleKeys(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(0, 0), float64(2))
	v2 := tsm1.NewValue(time.Unix(0, 0), float64(2))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
		"cpu,host=B#!~#value": []tsm1.Value{v2},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
	}
	r1 := MustWALSegment(dir, entries)

	v3 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v3},
	}
	r2 := MustTSMReader(dir, points2)

	wal, err := tsm1.NewWALKeyIterator(r1)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	tsm, err := tsm1.NewTSMKeyIterator(r2)
	if err != nil {
		t.Fatalf("unexpected error creating TSMKeyIterator: %v", err)
	}

	expected := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v3},
		"cpu,host=B#!~#value": []tsm1.Value{v2},
	}

	// Read should return a chunk of 1 value
	m := tsm1.NewMergeIterator(tsm, wal, 1, tsm1.MinTime, tsm1.MaxTime)
	var readValues bool
	for _, p := range expected {
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
		readValues = true

		assertValueEqual(t, values[0], p[0])
	}
	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that merge iterator over a wal returns points order by key and time.
func TestMergeIterator_WAL_MultipleKeys(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	v4 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	v5 := tsm1.NewValue(time.Unix(1, 0), float64(3)) // overwrites p1

	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v3},
		"cpu,host=B#!~#value": []tsm1.Value{v2},
	}

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v5},
		"cpu,host=B#!~#value": []tsm1.Value{v4},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
		&tsm1.WriteWALEntry{
			Values: points2,
		},
	}
	r := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	m := tsm1.NewMergeIterator(nil, iter, 2, tsm1.MinTime, tsm1.MaxTime)

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{v5, v3}},
		{"cpu,host=B#!~#value", []tsm1.Value{v2, v4}},
	}

	for i, p := range data {
		if !m.Next() {
			t.Fatalf("expected next(%d), got false", i)
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
			assertValueEqual(t, values[i], point)
		}
	}
}

// Tests that merge iterator over a wal returns points order by key and time.
func TestMergeIterator_TSM_MultipleKeys(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	v4 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	v5 := tsm1.NewValue(time.Unix(1, 0), float64(3)) // overwrites p1

	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v3},
		"cpu,host=B#!~#value": []tsm1.Value{v2},
	}

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v5},
		"cpu,host=B#!~#value": []tsm1.Value{v4},
	}

	r1 := MustTSMReader(dir, points1)
	r2 := MustTSMReader(dir, points2)

	iter, err := tsm1.NewTSMKeyIterator(r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	m := tsm1.NewMergeIterator(iter, nil, 2, tsm1.MinTime, tsm1.MaxTime)

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{v5, v3}},
		{"cpu,host=B#!~#value", []tsm1.Value{v2, v4}},
	}

	for i, p := range data {
		if !m.Next() {
			t.Fatalf("expected next(%d), got false", i)
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
			assertValueEqual(t, values[i], point)
		}
	}
}

// Tests that the merge iterator does not pull in deleted WAL entries.
func TestMergeIterator_WAL_DeletedKeys(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	v4 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	v5 := tsm1.NewValue(time.Unix(1, 0), float64(3)) // overwrites p1

	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v3},
		"cpu,host=B#!~#value": []tsm1.Value{v2},
	}

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v5},
		"cpu,host=B#!~#value": []tsm1.Value{v4},
	}

	entries := []tsm1.WALEntry{
		&tsm1.WriteWALEntry{
			Values: points1,
		},
		&tsm1.WriteWALEntry{
			Values: points2,
		},
		&tsm1.DeleteWALEntry{
			Keys: []string{"cpu,host=A#!~#value"},
		},
	}

	r := MustWALSegment(dir, entries)

	iter, err := tsm1.NewWALKeyIterator(r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	m := tsm1.NewMergeIterator(nil, iter, 2, tsm1.MinTime, tsm1.MaxTime)

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=B#!~#value", []tsm1.Value{v2, v4}},
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
			assertValueEqual(t, values[i], point)
		}
	}
}

// Tests that the merge iterator does not pull in deleted WAL entries.
func TestMergeIterator_TSM_DeletedKeys(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	v4 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	v5 := tsm1.NewValue(time.Unix(1, 0), float64(3)) // overwrites p1

	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v3},
		"cpu,host=B#!~#value": []tsm1.Value{v2},
	}

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v5},
		"cpu,host=B#!~#value": []tsm1.Value{v4},
	}

	r1 := MustTSMReader(dir, points1)
	r2 := MustTSMReader(dir, points2)
	r1.Delete("cpu,host=A#!~#value")
	r2.Delete("cpu,host=A#!~#value")

	iter, err := tsm1.NewTSMKeyIterator(r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating TSMKeyIterator: %v", err)
	}

	m := tsm1.NewMergeIterator(iter, nil, 2, tsm1.MinTime, tsm1.MaxTime)

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=B#!~#value", []tsm1.Value{v2, v4}},
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
			assertValueEqual(t, values[i], point)
		}
	}
}

//  Tests compacting a single wal segment into one tsm file
func TestCompactor_SingleWALSegment(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(2, 0), float64(2))

	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
		"cpu,host=B#!~#value": []tsm1.Value{v2, v3},
	}

	c := tsm1.NewCache(0)
	for k, v := range points1 {
		if err := c.Write(k, v, 1); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	compactor := &tsm1.Compactor{
		Dir:       dir,
		FileStore: &fakeFileStore{},
		Cache:     c,
	}

	files, err := compactor.Compact(nil, []string{"_00001.wal"})
	if err != nil {
		t.Fatalf("unexpected error compacting: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	f, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("unexpected error openting tsm: %v", err)
	}
	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		t.Fatalf("unexpected error creating tsm reader: %v", err)
	}

	keys := r.Keys()
	if got, exp := len(keys), 2; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{v1}},
		{"cpu,host=B#!~#value", []tsm1.Value{v2, v3}},
	}

	for _, p := range data {
		values, err := r.ReadAll(p.key)
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}
}

// // Tests compacting a multiple wal segment into one tsm file
func TestCompactor_MultipleWALSegment(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// First WAL segment
	v1 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(2, 0), float64(2))

	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v3},
		"cpu,host=B#!~#value": []tsm1.Value{v2},
	}

	c := tsm1.NewCache(0)
	for k, v := range points1 {
		if err := c.Write(k, v, 1); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	// Second WAL segment
	v4 := tsm1.NewValue(time.Unix(2, 0), float64(2))
	v5 := tsm1.NewValue(time.Unix(3, 0), float64(1))
	v6 := tsm1.NewValue(time.Unix(4, 0), float64(1))

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v5, v6},
		"cpu,host=B#!~#value": []tsm1.Value{v4},
	}

	for k, v := range points2 {
		if err := c.Write(k, v, 2); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	compactor := &tsm1.Compactor{
		Dir:       dir,
		FileStore: &fakeFileStore{},
		Cache:     c,
	}

	files, err := compactor.Compact(nil, []string{"_00001.wal", "_00002.wal"})
	if err != nil {
		t.Fatalf("unexpected error compacting: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	f, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("unexpected error openting tsm: %v", err)
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		t.Fatalf("unexpected error creating tsm reader: %v", err)
	}
	defer r.Close()

	keys := r.Keys()
	if got, exp := len(keys), 2; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{v1, v3, v5, v6}},
		{"cpu,host=B#!~#value", []tsm1.Value{v2, v4}},
	}

	for _, p := range data {
		values, err := r.ReadAll(p.key)
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}
}

// Tests that a single TSM file can be read and iterated over
func TestKeyIterator_TSM_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	r := MustTSMReader(dir, writes)

	iter, err := tsm1.NewTSMKeyIterator(r)
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

		if got, exp := len(values), len(writes); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for _, v := range values {
			readValues = true
			assertValueEqual(t, v, v1)
		}
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that duplicate point values are merged.  There is only one case
// where this could happen and that is when a compaction completed and we replace
// the old TSM file with a new one and we crash just before deleting the old file.
// No data is lost but the same point time/value would exist in two files until
// compaction corrects it.
func TestKeyIterator_TSM_Duplicate(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), int64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), int64(1))

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	r := MustTSMReader(dir, writes)

	iter, err := tsm1.NewTSMKeyIterator(r)
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
		assertValueEqual(t, values[0], v2)
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that a multiple WAL TSM can be read and iterated over and that
// points are sorted across them.
func TestKeyIterator_TSM_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(2, 0), int64(1))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	r1 := MustTSMReader(dir, points1)

	v2 := tsm1.NewValue(time.Unix(1, 0), int64(2))
	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v2},
	}

	r2 := MustTSMReader(dir, points2)

	iter, err := tsm1.NewTSMKeyIterator(r1, r2)
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

		assertValueEqual(t, values[0], v2)
		assertValueEqual(t, values[1], v1)
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that deleted keys are not seen during iteration with
// TSM files.
func TestKeyIterator_TSM_MultipleKeysDeleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(2, 0), int64(1))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1},
	}

	r1 := MustTSMReader(dir, points1)
	r1.Delete("cpu,host=A#!~#value")

	v2 := tsm1.NewValue(time.Unix(1, 0), float64(1))
	v3 := tsm1.NewValue(time.Unix(1, 0), float64(1))

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#count": []tsm1.Value{v2},
		"cpu,host=B#!~#value": []tsm1.Value{v3},
	}

	r2 := MustTSMReader(dir, points2)
	r2.Delete("cpu,host=A#!~#count")

	iter, err := tsm1.NewTSMKeyIterator(r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	var data = []struct {
		key   string
		value tsm1.Value
	}{
		{"cpu,host=B#!~#value", v3},
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

		assertValueEqual(t, values[0], data[0].value)
		data = data[1:]
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

func TestKeyIterator_Cache_Single(t *testing.T) {
	v0 := tsm1.NewValue(time.Unix(1, 0).UTC(), 1.0)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v0},
	}

	c := tsm1.NewCache(0)

	for k, v := range writes {
		if err := c.Write(k, v, 1); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	iter := tsm1.NewCacheKeyIterator(c, 0, 10)
	var readValues bool
	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), len(writes); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for _, v := range values {
			readValues = true
			assertValueEqual(t, v, v0)
		}
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

func TestKeyIterator_Cache_CheckpoinMiss(t *testing.T) {
	v0 := tsm1.NewValue(time.Unix(1, 0).UTC(), 1.0)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v0},
	}

	c := tsm1.NewCache(0)

	for k, v := range writes {
		if err := c.Write(k, v, 1); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	iter := tsm1.NewCacheKeyIterator(c, 2, 10)
	for iter.Next() {
		key, values, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		if got, exp := key, ""; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 0; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}
	}
}

// Tests that duplicate point values are merged
func TestKeyIterator_Cache_Duplicate(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(time.Unix(1, 0), int64(1))
	v2 := tsm1.NewValue(time.Unix(1, 0), int64(2))
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{v1, v2},
	}

	c := tsm1.NewCache(0)

	for k, v := range writes {
		if err := c.Write(k, v, 1); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	iter := tsm1.NewCacheKeyIterator(c, 0, 10)

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
		assertValueEqual(t, values[0], v2)
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

func TestDefaultCompactionPlanner_OnlyWAL(t *testing.T) {
	c := tsm1.NewCache(0)

	cp := &tsm1.DefaultPlanner{
		WAL: &fakeWAL{
			ClosedSegmentsFn: func() ([]tsm1.SegmentStat, error) {
				return []tsm1.SegmentStat{
					tsm1.SegmentStat{Path: "000001.wal"},
				}, nil
			},
		},
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return nil
			},
		},
		Cache: c,
	}

	tsm, wal, err := cp.Plan()
	if err != nil {
		t.Fatalf("unexpected error running plan: %v", err)
	}

	if exp, got := 0, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	if exp, got := 1, len(wal); got != exp {
		t.Fatalf("wal file length mismatch: got %v, exp %v", got, exp)
	}
}

func TestDefaultCompactionPlanner_OnlyTSM_MaxSize(t *testing.T) {
	c := tsm1.NewCache(0)

	cp := &tsm1.DefaultPlanner{
		WAL: &fakeWAL{
			ClosedSegmentsFn: func() ([]tsm1.SegmentStat, error) {
				return nil, nil
			},
		},
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					tsm1.FileStat{
						Path: "1.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Path: "2.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Path: "3.tsm",
						Size: 251 * 1024 * 1024,
					},
				}
			},
		},
		Cache: c,
	}

	tsm, wal, err := cp.Plan()
	if err != nil {
		t.Fatalf("unexpected error running plan: %v", err)
	}

	if exp, got := 2, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	if exp, got := 0, len(wal); got != exp {
		t.Fatalf("wal file length mismatch: got %v, exp %v", got, exp)
	}
}

func TestDefaultCompactionPlanner_TSM_Rewrite(t *testing.T) {
	c := tsm1.NewCache(0)

	cp := &tsm1.DefaultPlanner{
		WAL: &fakeWAL{
			ClosedSegmentsFn: func() ([]tsm1.SegmentStat, error) {
				return nil, nil
			},
		},
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					tsm1.FileStat{
						Path: "0001.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Path: "0002.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Size: 251 * 1024 * 1024,
					},
				}
			},
		},
		Cache: c,
	}

	tsm, wal, err := cp.Plan()
	if err != nil {
		t.Fatalf("unexpected error running plan: %v", err)
	}

	if exp, got := 2, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	if exp, got := 0, len(wal); got != exp {
		t.Fatalf("wal file length mismatch: got %v, exp %v", got, exp)
	}
}

func TestDefaultCompactionPlanner_NoRewrite_MaxWAL(t *testing.T) {
	c := tsm1.NewCache(0)

	cp := &tsm1.DefaultPlanner{
		WAL: &fakeWAL{
			ClosedSegmentsFn: func() ([]tsm1.SegmentStat, error) {
				return []tsm1.SegmentStat{
					tsm1.SegmentStat{Path: "00001.tsm1"},
					tsm1.SegmentStat{Path: "00002.tsm1"},
					tsm1.SegmentStat{Path: "00003.tsm1"},
					tsm1.SegmentStat{Path: "00004.tsm1"},
					tsm1.SegmentStat{Path: "00005.tsm1"},
					tsm1.SegmentStat{Path: "00006.tsm1"},
					tsm1.SegmentStat{Path: "00007.tsm1"},
					tsm1.SegmentStat{Path: "00008.tsm1"},
					tsm1.SegmentStat{Path: "00009.tsm1"},
					tsm1.SegmentStat{Path: "00010.tsm1"},
					tsm1.SegmentStat{Path: "00011.tsm1"},
				}, nil
			},
		},
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					tsm1.FileStat{
						Path: "0001.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Path: "0002.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Size: 251 * 1024 * 1024,
					},
				}
			},
		},
		Cache: c,
	}

	tsm, wal, err := cp.Plan()
	if err != nil {
		t.Fatalf("unexpected error running plan: %v", err)
	}

	if exp, got := 0, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	if exp, got := 10, len(wal); got != exp {
		t.Fatalf("wal file length mismatch: got %v, exp %v", got, exp)
	}
}

func TestDefaultCompactionPlanner_Rewrite_MixWAL(t *testing.T) {
	c := tsm1.NewCache(0)

	cp := &tsm1.DefaultPlanner{
		WAL: &fakeWAL{
			ClosedSegmentsFn: func() ([]tsm1.SegmentStat, error) {
				return []tsm1.SegmentStat{
					tsm1.SegmentStat{Path: "00001.wal"},
					tsm1.SegmentStat{Path: "00002.wal"},
					tsm1.SegmentStat{Path: "00003.wal"},
					tsm1.SegmentStat{Path: "00004.wal"},
					tsm1.SegmentStat{Path: "00005.wal"},
				}, nil
			},
		},
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					tsm1.FileStat{
						Path: "0001.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Path: "0002.tsm1",
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Size: 251 * 1024 * 1024,
					},
				}
			},
		},
		Cache: c,
	}

	tsm, wal, err := cp.Plan()
	if err != nil {
		t.Fatalf("unexpected error running plan: %v", err)
	}

	if exp, got := 2, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	if exp, got := 5, len(wal); got != exp {
		t.Fatalf("wal file length mismatch: got %v, exp %v", got, exp)
	}
}

func TestDefaultCompactionPlanner_Rewrite_WALOverlap(t *testing.T) {
	c := tsm1.NewCache(0)

	cp := &tsm1.DefaultPlanner{
		WAL: &fakeWAL{
			ClosedSegmentsFn: func() ([]tsm1.SegmentStat, error) {
				return []tsm1.SegmentStat{
					tsm1.SegmentStat{Path: "00001.tsm1",
						MinTime: time.Unix(1, 0),
						MaxTime: time.Unix(10, 0),
						MinKey:  "cpu",
						MaxKey:  "cpu"},
					tsm1.SegmentStat{Path: "00002.tsm1"},
					tsm1.SegmentStat{Path: "00003.tsm1"},
					tsm1.SegmentStat{Path: "00004.tsm1"},
					tsm1.SegmentStat{Path: "00005.tsm1"},
				}, nil
			},
		},
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					tsm1.FileStat{
						MinKey:  "cpu",
						MaxKey:  "mem",
						MinTime: time.Unix(0, 0),
						MaxTime: time.Unix(5, 0),
						Size:    1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Size: 1 * 1024 * 1024,
					},
					tsm1.FileStat{
						Size: 51 * 1024 * 1024,
					},
				}
			},
		},
		Cache: c,
	}

	tsm, wal, err := cp.Plan()
	if err != nil {
		t.Fatalf("unexpected error running plan: %v", err)
	}

	if exp, got := 1, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	if exp, got := 5, len(wal); got != exp {
		t.Fatalf("wal file length mismatch: got %v, exp %v", got, exp)
	}
}

func TestDefaultCompactionPlanner_Rewrite_Deletes(t *testing.T) {
	c := tsm1.NewCache(0)

	cp := &tsm1.DefaultPlanner{
		WAL: &fakeWAL{
			ClosedSegmentsFn: func() ([]tsm1.SegmentStat, error) {
				return []tsm1.SegmentStat{
					tsm1.SegmentStat{Path: "00001.wal"},
					tsm1.SegmentStat{Path: "00002.wal"},
					tsm1.SegmentStat{Path: "00003.wal"},
					tsm1.SegmentStat{Path: "00004.wal"},
					tsm1.SegmentStat{Path: "00005.wal"},
				}, nil
			},
		},
		FileStore: &fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					tsm1.FileStat{
						Path:         "000007.tsm1",
						HasTombstone: true,
					},
					tsm1.FileStat{
						Size: 251 * 1024 * 1024,
					},
				}
			},
		},
		Cache: c,
	}

	tsm, wal, err := cp.Plan()
	if err != nil {
		t.Fatalf("unexpected error running plan: %v", err)
	}

	if exp, got := 1, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	if exp, got := 5, len(wal); got != exp {
		t.Fatalf("wal file length mismatch: got %v, exp %v", got, exp)
	}
}
func assertValueEqual(t *testing.T, a, b tsm1.Value) {
	if got, exp := a.Time(), b.Time(); !got.Equal(exp) {
		t.Fatalf("time mismatch: got %v, exp %v", got, exp)
	}
	if got, exp := a.Value(), b.Value(); got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
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

func MustTSMReader(dir string, values map[string][]tsm1.Value) *tsm1.TSMReader {
	f := MustTempFile(dir)
	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		panic(fmt.Sprintf("create TSM writer: %v", err))
	}

	for k, v := range values {
		if err := w.Write(k, v); err != nil {
			panic(fmt.Sprintf("write TSM value: %v", err))
		}
	}

	if err := w.WriteIndex(); err != nil {
		panic(fmt.Sprintf("write TSM index: %v", err))
	}

	if err := w.Close(); err != nil {
		panic(fmt.Sprintf("write TSM close: %v", err))
	}

	f, err = os.Open(f.Name())
	if err != nil {
		panic(fmt.Sprintf("open file: %v", err))
	}

	r, err := tsm1.NewTSMReaderWithOptions(
		tsm1.TSMReaderOptions{
			MMAPFile: f,
		})
	if err != nil {
		panic(fmt.Sprintf("new reader: %v", err))
	}
	return r
}

type fakeWAL struct {
	ClosedSegmentsFn func() ([]tsm1.SegmentStat, error)
}

func (w *fakeWAL) ClosedSegments() ([]tsm1.SegmentStat, error) {
	return w.ClosedSegmentsFn()
}

type fakeFileStore struct {
	PathsFn func() []tsm1.FileStat
}

func (w *fakeFileStore) Stats() []tsm1.FileStat {
	return w.PathsFn()
}

func (w *fakeFileStore) NextID() int {
	return 1
}
