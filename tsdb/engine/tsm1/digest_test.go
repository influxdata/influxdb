package tsm1_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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

	files := []string{}
	if err := tsm1.Digest(dir, files, df); err != nil {
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

	mfest, err := r.ReadManifest()
	if err != nil {
		t.Fatal(err)
	}

	if len(mfest.Entries) != 0 {
		t.Fatalf("exp: 0, got: %d", len(mfest.Entries))
	}

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

	files, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		t.Fatal(err)
	}

	df := MustTempFile(dir)

	if err := tsm1.Digest(dir, files, df); err != nil {
		t.Fatalf("digest error: %v", err)
	}

	df, err = os.Open(df.Name())
	if err != nil {
		t.Fatalf("open error: %v", err)
	}

	r, err := tsm1.NewDigestReader(df)
	if err != nil {
		t.Fatalf("NewDigestReader error: %v", err)
	}
	defer r.Close()

	mfest, err := r.ReadManifest()
	if err != nil {
		t.Fatal(err)
	}

	if len(mfest.Entries) != 1 {
		t.Fatalf("exp: 1, got: %d", len(mfest.Entries))
	}

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

	files, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		t.Fatal(err)
	}

	df := MustTempFile(dir)

	if err := tsm1.DigestWithOptions(dir, files, tsm1.DigestOptions{MinTime: 2, MaxTime: 2}, df); err != nil {
		t.Fatalf("digest error: %v", err)
	}

	df, err = os.Open(df.Name())
	if err != nil {
		t.Fatalf("open error: %v", err)
	}

	r, err := tsm1.NewDigestReader(df)
	if err != nil {
		t.Fatalf("NewDigestReader error: %v", err)
	}
	defer r.Close()

	mfest, err := r.ReadManifest()
	if err != nil {
		t.Fatal(err)
	}

	if len(mfest.Entries) != 3 {
		t.Fatalf("exp: 3, got: %d", len(mfest.Entries))
	}

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

	files, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		t.Fatal(err)
	}

	df := MustTempFile(dir)

	if err := tsm1.DigestWithOptions(dir, files, tsm1.DigestOptions{
		MinKey: []byte("cpu,host=B#!~#value"),
		MaxKey: []byte("cpu,host=B#!~#value")}, df); err != nil {
		t.Fatalf("digest error: %v", err)
	}

	df, err = os.Open(df.Name())
	if err != nil {
		t.Fatalf("open error: %v", err)
	}

	r, err := tsm1.NewDigestReader(df)
	if err != nil {
		t.Fatalf("NewDigestReader error: %v", err)
	}
	defer r.Close()

	mfest, err := r.ReadManifest()
	if err != nil {
		t.Fatal(err)
	}

	if len(mfest.Entries) != 3 {
		t.Fatalf("exp: 3, got: %d", len(mfest.Entries))
	}

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

func TestDigest_Manifest(t *testing.T) {
	// Create temp directory to hold test files.
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	digestFile := filepath.Join(dir, tsm1.DigestFilename)

	// Create a point to write to the tsm files.
	a1 := tsm1.NewValue(1, 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": []tsm1.Value{a1},
	}

	// Write a few tsm files.
	var files []string
	gen := 1
	for ; gen < 4; gen++ {
		name := MustWriteTSM(dir, gen, writes)
		files = append(files, name)
	}

	// Generate a manifest.
	mfest, err := tsm1.NewDigestManifest(dir, files)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure manifest contains only the expected files.
	var got []string
	for _, e := range mfest.Entries {
		got = append(got, e.Filename)
	}

	sort.StringSlice(files).Sort()
	sort.StringSlice(got).Sort()

	if !reflect.DeepEqual(files, got) {
		t.Fatalf("exp: %v, got: %v", files, got)
	}

	// Write a digest of the files.
	df := MustCreate(digestFile)
	if err := tsm1.Digest(dir, files, df); err != nil {
		t.Fatalf("digest error: %v", err)
	}

	// Helper func to read manifest from a digest.
	readManifest := func(name string) *tsm1.DigestManifest {
		t.Helper()

		df, err = os.Open(df.Name())
		if err != nil {
			t.Fatal(err)
		}

		r, err := tsm1.NewDigestReader(df)
		if err != nil {
			t.Fatal(err)
		}

		mfest, err := r.ReadManifest()
		if err != nil {
			t.Fatal(err)
		}

		if err := r.Close(); err != nil {
			t.Fatal(err)
		}

		return mfest
	}

	// Read the manifest from the digest.
	mfest2 := readManifest(df.Name())

	// Make sure the manifest read from the digest on disk is correct.
	if !reflect.DeepEqual(mfest, mfest2) {
		t.Fatalf("invalid manifest:\nexp: %v\ngot: %v", mfest, mfest2)
	}

	// Write an extra tsm file that shouldn't be included in the manifest.
	extra := MustWriteTSM(dir, gen, writes)

	// Re-generate manifest.
	mfest, err = tsm1.NewDigestManifest(dir, files)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure manifest contains only the expected files.
	got = got[:0]
	for _, e := range mfest.Entries {
		if e.Filename == extra {
			t.Fatal("extra file in shard directory should not be in digest manifest")
		}
		got = append(got, e.Filename)
	}

	sort.StringSlice(got).Sort()

	if !reflect.DeepEqual(files, got) {
		t.Fatalf("exp: %v, got: %v", files, got)
	}

	// Re-generate digest and make sure it does not include the extra tsm file.
	df = MustCreate(digestFile)
	if err := tsm1.Digest(dir, files, df); err != nil {
		t.Fatalf("digest error: %v", err)
	}

	// Read the manifest from the new digest.
	mfest2 = readManifest(df.Name())

	// Make sure the manifest read from the digest on disk is correct.
	if !reflect.DeepEqual(mfest, mfest2) {
		t.Fatalf("invalid manifest:\nexp: %v\ngot: %v", mfest, mfest2)
	}

	// Make sure the digest is fresh.
	digest, err := os.Stat(df.Name())
	if err != nil {
		t.Fatal(err)
	}

	fresh, reason := tsm1.DigestFresh(dir, files, digest.ModTime())
	if !fresh {
		t.Fatalf("digest is stale: reason=%s", reason)
	}

	// Test that digest is stale if shard time is newer than digest time.
	fresh, _ = tsm1.DigestFresh(dir, files, digest.ModTime().Add(1))
	if fresh {
		t.Fatalf("digest is fresh")
	}

	// Test that digest is stale if a new tsm file has been written by the engine.
	allfiles := append(files, extra)
	fresh, _ = tsm1.DigestFresh(dir, allfiles, digest.ModTime())
	if fresh {
		t.Fatalf("digest is fresh")
	}

	// Open one of the tsm files and write data to it.
	f, err := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.WriteString("some data"); err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Test that digest is stale if a tsm file is changed.
	fresh, _ = tsm1.DigestFresh(dir, files, digest.ModTime())
	if fresh {
		t.Fatalf("digest is fresh")
	}

	// Delete a tsm file.
	if err := os.Remove(files[0]); err != nil {
		t.Fatal(err)
	}

	// Test that digest is stale if a tsm file is missing on disk.
	fresh, _ = tsm1.DigestFresh(dir, files, digest.ModTime())
	if fresh {
		t.Fatalf("digest is fresh")
	}

	// Delete the entire shard directory
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}

	// Test that digest is stale if the entire shard directory is missing.
	fresh, _ = tsm1.DigestFresh(dir, files, digest.ModTime())
	if fresh {
		t.Fatalf("digest is fresh")
	}
}

func MustCreate(path string) *os.File {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	return f
}
