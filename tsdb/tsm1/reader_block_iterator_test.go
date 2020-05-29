package tsm1

import (
	"os"
	"sort"
	"testing"
)

func TestBlockIterator_Single(t *testing.T) {
	dir := mustTempDir()
	defer os.RemoveAll(dir)
	f := mustTempFile(dir)

	w, err := NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	values := []Value{NewValue(0, int64(1))}
	if err := w.Write([]byte("cpu"), values); err != nil {
		t.Fatalf("unexpected error writing: %v", err)

	}
	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error opening: %v", err)
	}

	r, err := NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	var count int
	iter := r.BlockIterator()
	for iter.Next() {
		key, minTime, maxTime, typ, _, buf, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error creating iterator: %v", err)
		}

		if got, exp := string(key), "cpu"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := minTime, int64(0); got != exp {
			t.Fatalf("min time mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := maxTime, int64(0); got != exp {
			t.Fatalf("max time mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := typ, BlockInteger; got != exp {
			t.Fatalf("block type mismatch: got %v, exp %v", got, exp)
		}

		if len(buf) == 0 {
			t.Fatalf("buf length = 0")
		}

		count++
	}

	if got, exp := count, len(values); got != exp {
		t.Fatalf("value count mismatch: got %v, exp %v", got, exp)
	}
}

func TestBlockIterator_Tombstone(t *testing.T) {
	dir := mustTempDir()
	defer os.RemoveAll(dir)
	f := mustTempFile(dir)

	w, err := NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	values := []Value{NewValue(0, int64(1))}
	if err := w.Write([]byte("cpu"), values); err != nil {
		t.Fatalf("unexpected error writing: %v", err)
	}

	if err := w.Write([]byte("mem"), values); err != nil {
		t.Fatalf("unexpected error writing: %v", err)
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error opening: %v", err)
	}

	r, err := NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	iter := r.BlockIterator()
	for iter.Next() {
		// Trigger a delete during iteration.  This should cause an error condition for
		// the BlockIterator
		r.Delete([][]byte{[]byte("cpu")})
	}

	if iter.Err() == nil {
		t.Fatalf("expected error: got nil")
	}
}

func TestBlockIterator_MultipleBlocks(t *testing.T) {
	dir := mustTempDir()
	defer os.RemoveAll(dir)
	f := mustTempFile(dir)

	w, err := NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	values1 := []Value{NewValue(0, int64(1))}
	if err := w.Write([]byte("cpu"), values1); err != nil {
		t.Fatalf("unexpected error writing: %v", err)
	}

	values2 := []Value{NewValue(1, int64(2))}
	if err := w.Write([]byte("cpu"), values2); err != nil {
		t.Fatalf("unexpected error writing: %v", err)
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error opening: %v", err)
	}

	r, err := NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	var count int
	expData := []Values{values1, values2}
	iter := r.BlockIterator()
	var i int
	for iter.Next() {
		key, minTime, maxTime, typ, _, buf, err := iter.Read()

		if err != nil {
			t.Fatalf("unexpected error creating iterator: %v", err)
		}

		if got, exp := string(key), "cpu"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := minTime, expData[i][0].UnixNano(); got != exp {
			t.Fatalf("min time mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := maxTime, expData[i][0].UnixNano(); got != exp {
			t.Fatalf("max time mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := typ, BlockInteger; got != exp {
			t.Fatalf("block type mismatch: got %v, exp %v", got, exp)
		}

		if len(buf) == 0 {
			t.Fatalf("buf length = 0")
		}

		count++
		i++
	}

	if got, exp := count, 2; got != exp {
		t.Fatalf("value count mismatch: got %v, exp %v", got, exp)
	}
}

func TestBlockIterator_Sorted(t *testing.T) {
	dir := mustTempDir()
	defer os.RemoveAll(dir)
	f := mustTempFile(dir)

	w, err := NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	values := map[string][]Value{
		"mem":    []Value{NewValue(0, int64(1))},
		"cycles": []Value{NewValue(0, ^uint64(0))},
		"cpu":    []Value{NewValue(1, float64(2))},
		"disk":   []Value{NewValue(1, true)},
		"load":   []Value{NewValue(1, "string")},
	}

	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if err := w.Write([]byte(k), values[k]); err != nil {
			t.Fatalf("unexpected error writing: %v", err)

		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error opening: %v", err)
	}

	r, err := NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	var count int
	iter := r.BlockIterator()
	var lastKey string
	for iter.Next() {
		key, _, _, _, _, buf, err := iter.Read()

		if string(key) < lastKey {
			t.Fatalf("keys not sorted: got %v, last %v", key, lastKey)
		}

		lastKey = string(key)

		if err != nil {
			t.Fatalf("unexpected error creating iterator: %v", err)
		}

		if len(buf) == 0 {
			t.Fatalf("buf length = 0")
		}

		count++
	}

	if got, exp := count, len(values); got != exp {
		t.Fatalf("value count mismatch: got %v, exp %v", got, exp)
	}
}
