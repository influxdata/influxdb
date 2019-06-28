package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb/value"
	"os"
	"testing"
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
		prefix+",cpu,host=A#!~#float":    {p1},
		prefix+",cpu,host=A#!~#int":      {p2},
		prefix+",cpu,host=A#!~#bool":     {p3},
		prefix+",cpu,host=A#!~#string":   {p4},
		prefix+",cpu,host=A#!~#unsigned": {p5},
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
		Stderr: &testOut,
		Stdout: &testOut,
		Files: []string{file.Name()},
	}

	want := fmt.Sprintf(`File: %s
[write] sz=291
00000000000000010000000000000002,cpu,host=A#!~#bool true 1
00000000000000010000000000000002,cpu,host=A#!~#float 1.1 1
00000000000000010000000000000002,cpu,host=A#!~#int 1i 1
00000000000000010000000000000002,cpu,host=A#!~#string "string" 1
00000000000000010000000000000002,cpu,host=A#!~#unsigned 18446744073709551615u 1
`, file.Name())

	_, err := dump.Run(true)
	if err != nil {
		t.Fatal(err)
	}

	got := testOut.String()

	if !cmp.Equal(got, want) {
		t.Fatalf("Error: unexpected output: %v", cmp.Diff(got, want))
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
		Predicate: []byte("predicate"),
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	var testOut bytes.Buffer

	dump := &Dump{
		Stderr: &testOut,
		Stdout: &testOut,
		Files: []string{file.Name()},
	}


	name := file.Name()
	file.Close()

	_, err := dump.Run(true)

	if err != nil {
		t.Fatal(err)
	}

	want := fmt.Sprintf(`File: %s
[delete-bucket-range] org=0000000000000001 bucket=0000000000000002 min=3 max=4 sz=57
`, name)
	got := testOut.String()

	if !cmp.Equal(got, want) {
		t.Fatalf("Unexpected output %v", cmp.Diff(got, want))
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
	org := influxdb.ID(0xDEAD)
	bucket := influxdb.ID(0xBEEF)
	orgBytes := make([]byte, 8)
	bucketBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(orgBytes, uint64(org))
	binary.BigEndian.PutUint64(bucketBytes, uint64(bucket))
	prefix := string(orgBytes) + string(bucketBytes)

	// write duplicate points to the WAL...
	for i := 0; i < 2; i++ {
		values := map[string][]value.Value{
			prefix+",_m=cpu,host=A#!~#float":    []value.Value{p1},
			prefix+",_m=cpu,host=A#!~#int":      []value.Value{p2},
			prefix+",_m=cpu,host=A#!~#bool":     []value.Value{p3},
			prefix+",_m=cpu,host=A#!~#string":   []value.Value{p4},
			prefix+",_m=cpu,host=A#!~#unsigned": []value.Value{p5},
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
	}

	name := file.Name()
	file.Close()

	var testOut bytes.Buffer
	dump := &Dump{
		Stderr: &testOut,
		Stdout: &testOut,
		Files: []string{name},
		FindDuplicates: true,
	}

	duplicates, err := dump.Run(true)
	if err != nil {
		t.Fatal(err)
	}

	want := []*DumpReport{
		{File: name, DuplicateKeys: []string{
			prefix+",_m=cpu,host=A#!~#float",
			prefix+",_m=cpu,host=A#!~#int",
			prefix+",_m=cpu,host=A#!~#bool",
			prefix+",_m=cpu,host=A#!~#string",
			prefix+",_m=cpu,host=A#!~#unsigned",
		}},
	}

	wantOut :=   fmt.Sprintf(`File: %s
Duplicate/out of order keys:
  000000000000dead000000000000beef,_m=cpu,host=A#!~#bool
  000000000000dead000000000000beef,_m=cpu,host=A#!~#float
  000000000000dead000000000000beef,_m=cpu,host=A#!~#int
  000000000000dead000000000000beef,_m=cpu,host=A#!~#string
  000000000000dead000000000000beef,_m=cpu,host=A#!~#unsigned
`, name)

	gotOut := testOut.String()

	sortFunc := func(a, b string) bool { return a < b}

	if !cmp.Equal(duplicates, want, cmpopts.SortSlices(sortFunc)) {
		t.Fatal("Error", cmp.Diff(duplicates, want))
	}

	if !cmp.Equal(gotOut, wantOut) {
		t.Fatalf("Unexpected output: %v", cmp.Diff(gotOut, wantOut))
	}
}
