package wal

import (
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

	values := map[string][]value.Value{
		"cpu,host=A#!~#float":    []value.Value{p1},
		"cpu,host=A#!~#int":      []value.Value{p2},
		"cpu,host=A#!~#bool":     []value.Value{p3},
		"cpu,host=A#!~#string":   []value.Value{p4},
		"cpu,host=A#!~#unsigned": []value.Value{p5},
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

	fmt.Println(file.Name())

	file.Close()

	dump := &Dump{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		Files: []string{file.Name()},
	}

	_, err := dump.Run(true)
	if err != nil {
		t.Fatal(err)
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

	dump := &Dump{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		Files: []string{file.Name()},
	}
	file.Close()

	_, err := dump.Run(true)
	if err != nil {
		t.Fatal(err)
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

	values := map[string][]value.Value{
		"cpu,host=A#!~#float":    []value.Value{p1},
		"cpu,host=A#!~#int":      []value.Value{p2},
		"cpu,host=A#!~#bool":     []value.Value{p3},
		"cpu,host=A#!~#string":   []value.Value{p4},
		"cpu,host=A#!~#unsigned": []value.Value{p5},
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

	values = map[string][]value.Value{
		"cpu,host=A#!~#float":    []value.Value{p1},
		"cpu,host=A#!~#int":      []value.Value{p2},
		"cpu,host=A#!~#bool":     []value.Value{p3},
		"cpu,host=A#!~#string":   []value.Value{p4},
		"cpu,host=A#!~#unsigned": []value.Value{p5},
	}

	entry = &WriteWALEntry{
		Values: values,
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	fmt.Println(file.Name())

	name := file.Name()
	file.Close()

	dump := &Dump{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		Files: []string{file.Name()},
		FindDuplicates: true,
	}

	duplicates, err := dump.Run(true)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("duplicates", duplicates[0])
	want := []*DumpReport{
		{File: name, DuplicateKeys: []string{
			"cpu,host=A#!~#float",
			"cpu,host=A#!~#int",
			"cpu,host=A#!~#bool",
			"cpu,host=A#!~#string",
			"cpu,host=A#!~#unsigned",
		}},
	}
	sortFunc := func(a, b string) bool { return a < b}

	if !cmp.Equal(duplicates, want, cmpopts.SortSlices(sortFunc)) {
		t.Fatal("Error", cmp.Diff(duplicates, want))
	}
}
