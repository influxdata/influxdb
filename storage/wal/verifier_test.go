package wal

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/v1/tsdb/engine/tsm1"
)

type Test struct {
	dir          string
	corruptFiles []string
}

func TestVerifyWALL_CleanFile(t *testing.T) {
	numTestEntries := 100
	test := CreateTest(t, func() (string, []string, error) {
		dir := MustTempDir()

		w := NewWAL(dir)
		if err := w.Open(context.Background()); err != nil {
			return "", nil, errors.Wrap(err, "error opening wal")
		}

		for i := 0; i < numTestEntries; i++ {
			writeRandomEntry(w, t)
		}

		if err := w.Close(); err != nil {
			return "", nil, errors.Wrap(err, "error closing wal")
		}

		return dir, []string{}, nil
	})
	defer test.Close()

	verifier := &Verifier{Dir: test.dir}
	summary, err := verifier.Run(false)
	if err != nil {
		t.Fatalf("Unexpected error: %v\n", err)
	}

	expectedEntries := numTestEntries
	if summary.EntryCount != expectedEntries {
		t.Fatalf("Error: expected %d entries, checked %d entries", expectedEntries, summary.EntryCount)
	}

	if summary.CorruptFiles != nil {
		t.Fatalf("Error: expected no corrupt files")
	}
}

func CreateTest(t *testing.T, createFiles func() (string, []string, error)) *Test {
	t.Helper()

	dir, corruptFiles, err := createFiles()

	if err != nil {
		t.Fatal(err)
	}

	return &Test{
		dir:          dir,
		corruptFiles: corruptFiles,
	}
}

func TestVerifyWALL_CorruptFile(t *testing.T) {
	test := CreateTest(t, func() (string, []string, error) {
		dir := MustTempDir()
		f := mustTempWalFile(t, dir)
		writeCorruptEntries(f, t, 1)

		path := f.Name()
		return dir, []string{path}, nil
	})

	defer test.Close()

	verifier := &Verifier{Dir: test.dir}
	expectedEntries := 2 // 1 valid entry + 1 corrupt entry

	summary, err := verifier.Run(false)
	if err != nil {
		t.Fatalf("Unexpected error when running wal verification: %v", err)
	}

	if summary.EntryCount != expectedEntries {
		t.Fatalf("Error: expected %d entries, found %d entries", expectedEntries, summary.EntryCount)
	}

	want := test.corruptFiles
	got := summary.CorruptFiles
	lessFunc := func(a, b string) bool { return a < b }

	if !cmp.Equal(summary.CorruptFiles, want, cmpopts.SortSlices(lessFunc)) {
		t.Fatalf("Error: unexpected list of corrupt files %v", cmp.Diff(got, want))
	}
}

func writeRandomEntry(w *WAL, t *testing.T) {
	if _, err := w.WriteMulti(context.Background(), map[string][]value.Value{
		"cpu,host=A#!~#value": {
			value.NewValue(rand.Int63(), rand.Float64()),
		},
	}); err != nil {
		t.Fatalf("error writing entry: %v", err)
	}
}

func writeCorruptEntries(file *os.File, t *testing.T, n int) {
	w := NewWALSegmentWriter(file)

	// random byte sequence
	corruption := []byte{1, 4, 0, 0, 0}

	p1 := value.NewValue(1, 1.1)
	values := map[string][]value.Value{
		"cpu,host=A#!~#float": {p1},
	}

	for i := 0; i < n; i++ {
		entry := &WriteWALEntry{
			Values: values,
		}

		if err := w.Write(mustMarshalEntry(entry)); err != nil {
			fatal(t, "write points", err)
		}

		if err := w.Flush(); err != nil {
			fatal(t, "flush", err)
		}
	}

	// Write some random bytes to the file to simulate corruption.
	if _, err := file.Write(corruption); err != nil {
		fatal(t, "corrupt WAL segment", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Error: failed to close file: %v\n", err)
	}
}

func (t *Test) Close() {
	err := os.RemoveAll(t.dir)
	if err != nil {
		panic(err)
	}
}

func mustTempWalFile(t *testing.T, dir string) *os.File {
	file, err := ioutil.TempFile(dir, "corrupt*.wal")
	if err != nil {
		t.Fatal(err)
	}

	return file
}
