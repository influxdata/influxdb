package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/v1/tsdb"
	"github.com/influxdata/influxdb/v2/v1/tsdb/engine/tsm1"
)

func TestWalDump_RunWriteEntries(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	file := mustTempWalFile(t, dir)

	w := NewWALSegmentWriter(file)

	p1 := value.NewValue(1, 1.1)
	p2 := value.NewValue(1, int64(1))
	p3 := value.NewValue(1, true)
	p4 := value.NewValue(1, "string")
	p5 := value.NewValue(1, ^uint64(0))

	org := influxdb.ID(1)
	orgBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(orgBytes, uint64(org))
	bucket := influxdb.ID(2)
	bucketBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bucketBytes, uint64(bucket))
	prefix := string(orgBytes) + string(bucketBytes)

	values := map[string][]value.Value{
		prefix + ",cpu,host=A#!~#float":    {p1},
		prefix + ",cpu,host=A#!~#int":      {p2},
		prefix + ",cpu,host=A#!~#bool":     {p3},
		prefix + ",cpu,host=A#!~#string":   {p4},
		prefix + ",cpu,host=A#!~#unsigned": {p5},
	}

	entry := &WriteWALEntry{
		Values: values,
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	file.Close()

	var testOut bytes.Buffer
	dump := &Dump{
		Stderr:    &testOut,
		Stdout:    &testOut,
		FileGlobs: []string{file.Name()},
	}

	wantOut := fmt.Sprintf(`File: %s
[write] sz=291
00000000000000010000000000000002,cpu,host=A#!~#bool true 1
00000000000000010000000000000002,cpu,host=A#!~#float 1.1 1
00000000000000010000000000000002,cpu,host=A#!~#int 1i 1
00000000000000010000000000000002,cpu,host=A#!~#string "string" 1
00000000000000010000000000000002,cpu,host=A#!~#unsigned 18446744073709551615u 1
`, file.Name())

	report, err := dump.Run(true)
	if err != nil {
		t.Fatal(err)
	}

	gotOut := testOut.String()

	if !cmp.Equal(gotOut, wantOut) {
		t.Fatalf("Error: unexpected output: %v", cmp.Diff(gotOut, wantOut))
	}

	wantReport := []*DumpReport{
		{
			File: file.Name(),
			Writes: []*WriteWALEntry{
				entry,
			},
		},
	}

	unexported := []interface{}{
		value.NewBooleanValue(0, false), value.NewStringValue(0, ""), value.NewIntegerValue(0, 0),
		value.NewUnsignedValue(0, 0), value.NewFloatValue(0, 0.0), WriteWALEntry{},
	}

	if diff := cmp.Diff(report, wantReport, cmp.AllowUnexported(unexported...)); diff != "" {
		t.Fatalf("Error: unexpected output: %v", diff)
	}
}

func TestWalDumpRun_DeleteRangeEntries(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	file := mustTempWalFile(t, dir)

	w := NewWALSegmentWriter(file)
	entry := &DeleteBucketRangeWALEntry{
		OrgID:     influxdb.ID(1),
		BucketID:  influxdb.ID(2),
		Min:       3,
		Max:       4,
		Predicate: []byte(nil),
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	var testOut bytes.Buffer

	dump := &Dump{
		Stderr:    &testOut,
		Stdout:    &testOut,
		FileGlobs: []string{file.Name()},
	}

	name := file.Name()
	file.Close()

	report, err := dump.Run(true)

	if err != nil {
		t.Fatal(err)
	}

	want := fmt.Sprintf(`File: %s
[delete-bucket-range] org=0000000000000001 bucket=0000000000000002 min=3 max=4 sz=48 pred=
`, name)
	got := testOut.String()

	if !cmp.Equal(got, want) {
		t.Fatalf("Unexpected output %v", cmp.Diff(got, want))
	}

	wantReport := []*DumpReport{
		{
			File: file.Name(),
			Deletes: []*DeleteBucketRangeWALEntry{
				entry,
			},
		},
	}

	unexported := []interface{}{
		value.NewBooleanValue(0, false), value.NewStringValue(0, ""), value.NewIntegerValue(0, 0),
		value.NewUnsignedValue(0, 0), value.NewFloatValue(0, 0.0), WriteWALEntry{},
	}
	if diff := cmp.Diff(report, wantReport, cmp.AllowUnexported(unexported...)); diff != "" {
		t.Fatalf("Error: unexpected report: %v", diff)
	}

}

func TestWalDumpRun_EntriesOutOfOrder(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	file := mustTempWalFile(t, dir)

	w := NewWALSegmentWriter(file)

	p1 := value.NewValue(1, 1.1)
	p2 := value.NewValue(1, int64(1))
	p3 := value.NewValue(1, true)
	p4 := value.NewValue(1, "string")
	p5 := value.NewValue(1, ^uint64(0))

	prefix := tsdb.EncodeNameString(influxdb.ID(0xDEAD), influxdb.ID(0xBEEF))

	// write duplicate points to the WAL...
	values := map[string][]value.Value{
		prefix + ",_m=cpu,host=A#!~#float":    {p1},
		prefix + ",_m=cpu,host=A#!~#int":      {p2},
		prefix + ",_m=cpu,host=A#!~#bool":     {p3},
		prefix + ",_m=cpu,host=A#!~#string":   {p4},
		prefix + ",_m=cpu,host=A#!~#unsigned": {p5},
	}

	var entries []*WriteWALEntry

	for i := 0; i < 2; i++ {
		entry := &WriteWALEntry{
			Values: values,
		}
		if err := w.Write(mustMarshalEntry(entry)); err != nil {
			t.Fatalf("error writing points: %v", err)
		}

		if err := w.Flush(); err != nil {
			t.Fatalf("error flushing wal: %v", err)
		}
		entries = append(entries, entry)
	}

	name := file.Name()
	file.Close()

	var testOut bytes.Buffer
	dump := &Dump{
		Stderr:         &testOut,
		Stdout:         &testOut,
		FileGlobs:      []string{name},
		FindDuplicates: true,
	}

	report, err := dump.Run(true)
	if err != nil {
		t.Fatal(err)
	}

	want := []*DumpReport{
		{
			File: name,
			DuplicateKeys: []string{
				prefix + ",_m=cpu,host=A#!~#float",
				prefix + ",_m=cpu,host=A#!~#int",
				prefix + ",_m=cpu,host=A#!~#bool",
				prefix + ",_m=cpu,host=A#!~#string",
				prefix + ",_m=cpu,host=A#!~#unsigned",
			},
			Writes: entries,
		},
	}

	wantOut := fmt.Sprintf(`File: %s
Duplicate/out of order keys:
  000000000000dead000000000000beef,_m=cpu,host=A#!~#bool
  000000000000dead000000000000beef,_m=cpu,host=A#!~#float
  000000000000dead000000000000beef,_m=cpu,host=A#!~#int
  000000000000dead000000000000beef,_m=cpu,host=A#!~#string
  000000000000dead000000000000beef,_m=cpu,host=A#!~#unsigned
`, name)

	gotOut := testOut.String()

	sortFunc := func(a, b string) bool { return a < b }

	unexported := []interface{}{
		value.NewBooleanValue(0, false), value.NewStringValue(0, ""), value.NewIntegerValue(0, 0),
		value.NewUnsignedValue(0, 0), value.NewFloatValue(0, 0.0), WriteWALEntry{},
	}

	if diff := cmp.Diff(report, want, cmpopts.SortSlices(sortFunc), cmp.AllowUnexported(unexported...)); diff != "" {
		t.Fatalf("Error: unexpected report: %v", diff)
	}

	if diff := cmp.Diff(gotOut, wantOut); diff != "" {
		t.Fatalf("Unexpected output: %v", diff)
	}
}

func MustTempFilePattern(dir string, pattern string) *os.File {
	f, err := ioutil.TempFile(dir, pattern)
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	return f
}

func TestGlobAndDedupe(t *testing.T) {
	dir := MustTempDir()
	file := MustTempFilePattern(dir, "pattern")
	file2 := MustTempFilePattern(dir, "pattern")

	fmt.Println(dir)
	globs := []string{dir + "/*"}
	paths, _ := globAndDedupe(globs)
	want := []string{file.Name(), file2.Name()}
	sort.Strings(want)

	if diff := cmp.Diff(paths, want); diff != "" {
		t.Fatalf("Unexpected output: %v", diff)
	}

	globs = append(globs, dir+"/pattern*")
	paths, _ = globAndDedupe(globs)

	if diff := cmp.Diff(paths, want); diff != "" {
		t.Fatalf("Unexpected output: %v", diff)
	}

}
