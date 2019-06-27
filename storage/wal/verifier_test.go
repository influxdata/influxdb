package wal

import (
	"context"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/tsdb/value"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

type Test struct {
	dir string
	files []string
}

func TestVerifyWALL_CleanFile(t *testing.T) {
	numTestEntries := 100
	test := CreateTest(t, func() (string, []string, error) {
		dir := mustCreateTempDir(t)

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
	summary, err := verifier.Run(true)
	if err != nil {
		t.Fatalf("Unexpected error: %v\n", err)
	}

	expectedEntries :=  numTestEntries
	if summary.EntryCount != expectedEntries {
		t.Fatalf("Error: expected %d entries, checked %d entries", expectedEntries, summary.EntryCount)
	}

	if summary.CorruptFiles != nil {
		t.Fatalf("Error: expected no corrupt files")
	}
}

func CreateTest(t *testing.T, createFiles func() (string, []string, error)) *Test {
	t.Helper()

	dir, files, err := createFiles()

	if err != nil {
		t.Fatal(err)
	}

	return &Test{
		dir: dir,
		files: files,
	}
}

func TestVerifyWALL_CorruptFile(t *testing.T) {
	test := CreateTest(t, func() (string, []string, error) {
		dir := mustCreateTempDir(t)
		f := mustCreateTempFile(t, dir)
		writeCorruptEntries(f, t, 1)

		path := f.Name()
		return dir, []string{path}, nil
	})

	defer test.Close()

	verifier := &Verifier{Dir: test.dir}
	expectedEntries := 1
	expectedErrors := 1

	summary, err := verifier.Run(true)
	if err != nil {
		t.Fatalf("Unexpected error when running wal verification: %v", err)
	}

	if summary.EntryCount != expectedEntries {
		t.Fatalf("Error: expected %d entries, found %d entries", expectedEntries, summary.EntryCount)
	}

	if len(summary.CorruptFiles) != expectedErrors {
		t.Fatalf("Error: expected %d corrupt entries, found %d corrupt entries", expectedErrors, len(summary.CorruptFiles))
	}

	want := test.files
	got := summary.CorruptFiles
	t.Log("got: ", summary.CorruptFiles)
	t.Log("want: ", want)
	t.Log(cmp.Diff(got, want))

	if !cmp.Equal(summary.CorruptFiles, want) {
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

func writeRandomEntryRaw(w *WALSegmentWriter, t *testing.T) {
	values := map[string][]value.Value{
		"cpu,host=A#!~#value":    {value.NewValue(rand.Int63(), rand.Float64())},
	}

	entry := &WriteWALEntry{
		Values: values,
	}


	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		t.Fatalf("error writing entry: %v", err)
	}
}

func writeCorruptEntries(file *os.File, t *testing.T, n int) {
	// random byte sequence
	corrupt := []byte{1, 255, 0, 3, 45, 26, 110}

	for i := 0; i < n; i++ {
		wrote, err := file.Write(corrupt)
		if err != nil {
			t.Fatal(err)
		} else if wrote != len(corrupt) {
			t.Fatal("Error writing corrupt data to file")
		}
	}


	if err := file.Close(); err != nil {
		t.Fatalf("Error: filed to close file: %v\n", err)
	}
}

func (t *Test) Close() {
	err := os.RemoveAll(t.dir)
	if err != nil {
		panic(err)
	}
}

func mustCreateTempDir(t *testing.T) string {
	name, err := ioutil.TempDir(".", "wal-test")
	if err != nil {
		t.Fatal(err)
	}

	return name
}

func mustCreateTempFile(t *testing.T, dir string) *os.File {
	file, err := ioutil.TempFile(dir, "corrupt*.wal")
	if err != nil {
		t.Fatal(err)
	}

	return file
}
