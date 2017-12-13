package tsm1_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestDigest_None(t *testing.T) {
	dir := MustTempDir()
	dataDir := filepath.Join(dir, "data")
	if err := os.Mkdir(dataDir, 0755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	df := MustTempFile(dir)

	if err := tsm1.Digest(dir, df); err != nil {
		t.Fatalf("digest error: %v", err)
	}

	df, err := os.Open(df.Name())
	if err != nil {
		t.Fatalf("open error: %v", err)
	}

	r, err := tsm1.NewDigestReader(df)
	if err != nil {
		t.Fatalf("NewDigestReader error: %v", err)
	}
	defer r.Close()

	var count int
	for {
		_, _, err := r.ReadTimeSpan()
		if err == io.EOF {
			break
		}

		count++
	}

	if got, exp := count, 0; got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
}

func TestDigest_One(t *testing.T) {
	dir := MustTempDir()
	dataDir := filepath.Join(dir, "data")
	if err := os.Mkdir(dataDir, 0755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	a1 := tsm1.NewValue(1, 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{a1},
	}
	MustWriteTSM(dir, 1, writes)

	df := MustTempFile(dir)

	if err := tsm1.Digest(dir, df); err != nil {
		t.Fatalf("digest error: %v", err)
	}

	df, err := os.Open(df.Name())
	if err != nil {
		t.Fatalf("open error: %v", err)
	}

	r, err := tsm1.NewDigestReader(df)
	if err != nil {
		t.Fatalf("NewDigestReader error: %v", err)
	}
	defer r.Close()

	var count int
	for {
		key, _, err := r.ReadTimeSpan()
		if err == io.EOF {
			break
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		count++
	}

	if got, exp := count, 1; got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
}

func TestDigest_TimeFilter(t *testing.T) {
	dir := MustTempDir()
	dataDir := filepath.Join(dir, "data")
	if err := os.Mkdir(dataDir, 0755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	a1 := tsm1.NewValue(1, 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{a1},
	}
	MustWriteTSM(dir, 1, writes)

	a2 := tsm1.NewValue(2, 2.1)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{a2},
	}
	MustWriteTSM(dir, 2, writes)

	a3 := tsm1.NewValue(3, 3.1)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{a3},
	}
	MustWriteTSM(dir, 3, writes)

	df := MustTempFile(dir)

	if err := tsm1.DigestWithOptions(dir, tsm1.DigestOptions{MinTime: 2, MaxTime: 2}, df); err != nil {
		t.Fatalf("digest error: %v", err)
	}

	df, err := os.Open(df.Name())
	if err != nil {
		t.Fatalf("open error: %v", err)
	}

	r, err := tsm1.NewDigestReader(df)
	if err != nil {
		t.Fatalf("NewDigestReader error: %v", err)
	}
	defer r.Close()

	var count int
	for {
		key, ts, err := r.ReadTimeSpan()
		if err == io.EOF {
			break
		}

		if got, exp := key, "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		for _, tr := range ts.Ranges {
			if got, exp := tr.Max, int64(2); got != exp {
				t.Fatalf("min time not filtered: got %v, exp %v", got, exp)
			}
		}

		count++
	}

	if got, exp := count, 1; got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
}

func TestDigest_KeyFilter(t *testing.T) {
	dir := MustTempDir()
	dataDir := filepath.Join(dir, "data")
	if err := os.Mkdir(dataDir, 0755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	a1 := tsm1.NewValue(1, 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{a1},
	}
	MustWriteTSM(dir, 1, writes)

	a2 := tsm1.NewValue(2, 2.1)
	writes = map[string][]tsm1.Value{
		"cpu,host=B#!~#value": []tsm1.Value{a2},
	}
	MustWriteTSM(dir, 2, writes)

	a3 := tsm1.NewValue(3, 3.1)
	writes = map[string][]tsm1.Value{
		"cpu,host=C#!~#value": []tsm1.Value{a3},
	}
	MustWriteTSM(dir, 3, writes)

	df := MustTempFile(dir)

	if err := tsm1.DigestWithOptions(dir, tsm1.DigestOptions{
		MinKey: []byte("cpu,host=B#!~#value"),
		MaxKey: []byte("cpu,host=B#!~#value")}, df); err != nil {
		t.Fatalf("digest error: %v", err)
	}

	df, err := os.Open(df.Name())
	if err != nil {
		t.Fatalf("open error: %v", err)
	}

	r, err := tsm1.NewDigestReader(df)
	if err != nil {
		t.Fatalf("NewDigestReader error: %v", err)
	}
	defer r.Close()

	var count int
	for {
		key, _, err := r.ReadTimeSpan()
		if err == io.EOF {
			break
		}

		if got, exp := key, "cpu,host=B#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		count++
	}

	if got, exp := count, 1; got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
}
