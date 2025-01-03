package tsm1_test

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Tests compacting a Cache snapshot into a single TSM file
func TestCompactor_Snapshot(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(1, float64(1))
	v2 := tsm1.NewValue(1, float64(1))
	v3 := tsm1.NewValue(2, float64(2))

	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {v1},
		"cpu,host=B#!~#value": {v2, v3},
	}

	c := tsm1.NewCache(0)
	for k, v := range points1 {
		if err := c.Write([]byte(k), v); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = &fakeFileStore{}

	files, err := compactor.WriteSnapshot(c, zap.NewNop())
	if err == nil {
		t.Fatalf("expected error writing snapshot: %v", err)
	}
	if len(files) > 0 {
		t.Fatalf("no files should be compacted: got %v", len(files))

	}

	compactor.Open()

	files, err = compactor.WriteSnapshot(c, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 2; got != exp {
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
		values, err := r.ReadAll([]byte(p.key))
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

func TestCompactor_CompactFullLastTimestamp(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	var vals tsm1.Values
	ts := int64(1e9)
	for i := 0; i < 120; i++ {
		vals = append(vals, tsm1.NewIntegerValue(ts, 1))
		ts += 1e9
	}
	// 121st timestamp skips a second
	ts += 1e9
	vals = append(vals, tsm1.NewIntegerValue(ts, 1))
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals[:100],
	}
	f1 := MustWriteTSM(dir, 1, writes)

	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals[100:],
	}
	f2 := MustWriteTSM(dir, 2, writes)

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs
	compactor.Open()

	files, err := compactor.CompactFull([]string{f1, f2}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %#v", err)
	}

	r := MustOpenTSMReader(files[0])
	entries := r.Entries([]byte("cpu,host=A#!~#value"))
	_, b, err := r.ReadBytes(&entries[0], nil)
	if err != nil {
		t.Fatalf("ReadBytes: unexpected error %v", err)
	}
	var a tsdb.IntegerArray
	err = tsm1.DecodeIntegerArrayBlock(b, &a)
	if err != nil {
		t.Fatalf("DecodeIntegerArrayBlock: unexpected error %v", err)
	}

	if a.MaxTime() != entries[0].MaxTime {
		t.Fatalf("expected MaxTime == a.MaxTime()")
	}
}

// Ensures that a compaction will properly merge multiple TSM files
func TestCompactor_CompactFull(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 3 TSM files with different data and one new point
	a1 := tsm1.NewValue(1, 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	a2 := tsm1.NewValue(2, 1.2)
	b1 := tsm1.NewValue(1, 2.1)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a2},
		"cpu,host=B#!~#value": {b1},
	}
	f2 := MustWriteTSM(dir, 2, writes)

	a3 := tsm1.NewValue(1, 1.3)
	c1 := tsm1.NewValue(1, 3.1)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a3},
		"cpu,host=C#!~#value": {c1},
	}
	f3 := MustWriteTSM(dir, 3, writes)

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs

	files, err := compactor.CompactFull([]string{f1, f2, f3}, zap.NewNop())
	if err == nil {
		t.Fatalf("expected error writing snapshot: %v", err)
	}
	if len(files) > 0 {
		t.Fatalf("no files should be compacted: got %v", len(files))

	}

	compactor.Open()

	files, err = compactor.CompactFull([]string{f1, f2, f3}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	expGen, expSeq, err := tsm1.DefaultParseFileName(f3)
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}
	expSeq = expSeq + 1

	gotGen, gotSeq, err := tsm1.DefaultParseFileName(files[0])
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}

	if gotGen != expGen {
		t.Fatalf("wrong generation for new file: got %v, exp %v", gotGen, expGen)
	}

	if gotSeq != expSeq {
		t.Fatalf("wrong sequence for new file: got %v, exp %v", gotSeq, expSeq)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 3; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{a3, a2}},
		{"cpu,host=B#!~#value", []tsm1.Value{b1}},
		{"cpu,host=C#!~#value", []tsm1.Value{c1}},
	}

	for _, p := range data {
		values, err := r.ReadAll([]byte(p.key))
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch %s: got %v, exp %v", p.key, got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}
}

// Ensures that a compaction will properly merge multiple TSM files
func TestCompactor_DecodeError(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 3 TSM files with different data and one new point
	a1 := tsm1.NewValue(1, 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	a2 := tsm1.NewValue(2, 1.2)
	b1 := tsm1.NewValue(1, 2.1)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a2},
		"cpu,host=B#!~#value": {b1},
	}
	f2 := MustWriteTSM(dir, 2, writes)

	a3 := tsm1.NewValue(1, 1.3)
	c1 := tsm1.NewValue(1, 3.1)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a3},
		"cpu,host=C#!~#value": {c1},
	}
	f3 := MustWriteTSM(dir, 3, writes)
	f, err := os.OpenFile(f3, os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	f.WriteAt([]byte("ffff"), 10) // skip over header
	f.Close()

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs

	files, err := compactor.CompactFull([]string{f1, f2, f3}, zap.NewNop())
	if err == nil {
		t.Fatalf("expected error writing snapshot: %v", err)
	}
	if len(files) > 0 {
		t.Fatalf("no files should be compacted: got %v", len(files))

	}

	compactor.Open()

	if _, err = compactor.CompactFull([]string{f1, f2, f3}, zap.NewNop()); err == nil || !strings.Contains(err.Error(), "decode error: unable to decompress block type float for key 'cpu,host=A#!~#value': unpackBlock: not enough data for timestamp") {
		t.Fatalf("expected error writing snapshot: %v", err)
	}
}

// Ensures that a compaction will properly merge multiple TSM files
func TestCompactor_Compact_OverlappingBlocks(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 3 TSM files with different data and one new point
	a1 := tsm1.NewValue(4, 1.1)
	a2 := tsm1.NewValue(5, 1.1)
	a3 := tsm1.NewValue(7, 1.1)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1, a2, a3},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	c1 := tsm1.NewValue(3, 1.2)
	c2 := tsm1.NewValue(8, 1.2)
	c3 := tsm1.NewValue(9, 1.2)

	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {c1, c2, c3},
	}
	f3 := MustWriteTSM(dir, 3, writes)

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs
	compactor.Size = 2

	compactor.Open()

	files, err := compactor.CompactFast([]string{f1, f3}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 1; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{c1, a1, a2, a3, c2, c3}},
	}

	for _, p := range data {
		values, err := r.ReadAll([]byte(p.key))
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch %s: got %v, exp %v", p.key, got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}
}

// Ensures that a compaction will properly merge multiple TSM files
func TestCompactor_Compact_OverlappingBlocksMultiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 3 TSM files with different data and one new point
	a1 := tsm1.NewValue(4, 1.1)
	a2 := tsm1.NewValue(5, 1.1)
	a3 := tsm1.NewValue(7, 1.1)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1, a2, a3},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	b1 := tsm1.NewValue(1, 1.2)
	b2 := tsm1.NewValue(2, 1.2)
	b3 := tsm1.NewValue(6, 1.2)

	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {b1, b2, b3},
	}
	f2 := MustWriteTSM(dir, 2, writes)

	c1 := tsm1.NewValue(3, 1.2)
	c2 := tsm1.NewValue(8, 1.2)
	c3 := tsm1.NewValue(9, 1.2)

	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {c1, c2, c3},
	}
	f3 := MustWriteTSM(dir, 3, writes)

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs
	compactor.Size = 2

	compactor.Open()

	files, err := compactor.CompactFast([]string{f1, f2, f3}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 1; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{b1, b2, c1, a1, a2, b3, a3, c2, c3}},
	}

	for _, p := range data {
		values, err := r.ReadAll([]byte(p.key))
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch %s: got %v, exp %v", p.key, got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}
}

func TestCompactor_Compact_UnsortedBlocks(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 2 TSM files with different data and one new point
	a1 := tsm1.NewValue(4, 1.1)
	a2 := tsm1.NewValue(5, 1.1)
	a3 := tsm1.NewValue(6, 1.1)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1, a2, a3},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	b1 := tsm1.NewValue(1, 1.2)
	b2 := tsm1.NewValue(2, 1.2)
	b3 := tsm1.NewValue(3, 1.2)

	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {b1, b2, b3},
	}
	f2 := MustWriteTSM(dir, 2, writes)

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = &fakeFileStore{}
	compactor.Size = 2

	compactor.Open()

	files, err := compactor.CompactFast([]string{f1, f2}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 1; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{b1, b2, b3, a1, a2, a3}},
	}

	for _, p := range data {
		values, err := r.ReadAll([]byte(p.key))
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch %s: got %v, exp %v", p.key, got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}
}

func TestCompactor_Compact_UnsortedBlocksOverlapping(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 3 TSM files where two blocks are overlapping and with unsorted order
	a1 := tsm1.NewValue(1, 1.1)
	a2 := tsm1.NewValue(2, 1.1)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1, a2},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	b1 := tsm1.NewValue(3, 1.2)
	b2 := tsm1.NewValue(4, 1.2)

	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {b1, b2},
	}
	f2 := MustWriteTSM(dir, 2, writes)

	c1 := tsm1.NewValue(1, 1.1)
	c2 := tsm1.NewValue(2, 1.1)

	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {c1, c2},
	}
	f3 := MustWriteTSM(dir, 3, writes)

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = &fakeFileStore{}
	compactor.Size = 2

	compactor.Open()

	files, err := compactor.CompactFast([]string{f1, f2, f3}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 1; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{a1, a2, b1, b2}},
	}

	for _, p := range data {
		values, err := r.ReadAll([]byte(p.key))
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch %s: got %v, exp %v", p.key, got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}
}

// Ensures that a compaction will properly merge multiple TSM files
func TestCompactor_CompactFull_SkipFullBlocks(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 3 TSM files with different data and one new point
	a1 := tsm1.NewValue(1, 1.1)
	a2 := tsm1.NewValue(2, 1.2)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1, a2},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	a3 := tsm1.NewValue(3, 1.3)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a3},
	}
	f2 := MustWriteTSM(dir, 2, writes)

	a4 := tsm1.NewValue(4, 1.4)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a4},
	}
	f3 := MustWriteTSM(dir, 3, writes)

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs
	compactor.Size = 2
	compactor.Open()

	files, err := compactor.CompactFull([]string{f1, f2, f3}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	expGen, expSeq, err := tsm1.DefaultParseFileName(f3)
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}
	expSeq = expSeq + 1

	gotGen, gotSeq, err := tsm1.DefaultParseFileName(files[0])
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}

	if gotGen != expGen {
		t.Fatalf("wrong generation for new file: got %v, exp %v", gotGen, expGen)
	}

	if gotSeq != expSeq {
		t.Fatalf("wrong sequence for new file: got %v, exp %v", gotSeq, expSeq)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 1; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{a1, a2, a3, a4}},
	}

	for _, p := range data {
		values, err := r.ReadAll([]byte(p.key))
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch %s: got %v, exp %v", p.key, got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}

	if got, exp := len(r.Entries([]byte("cpu,host=A#!~#value"))), 2; got != exp {
		t.Fatalf("block count mismatch: got %v, exp %v", got, exp)
	}
}

// Ensures that a full compaction will skip over blocks that have the full
// range of time contained in the block tombstoned
func TestCompactor_CompactFull_TombstonedSkipBlock(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 3 TSM files with different data and one new point
	a1 := tsm1.NewValue(1, 1.1)
	a2 := tsm1.NewValue(2, 1.2)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1, a2},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	ts := tsm1.NewTombstoner(f1, nil)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, math.MinInt64, math.MaxInt64)

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	a3 := tsm1.NewValue(3, 1.3)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a3},
	}
	f2 := MustWriteTSM(dir, 2, writes)

	a4 := tsm1.NewValue(4, 1.4)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a4},
	}
	f3 := MustWriteTSM(dir, 3, writes)

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs
	compactor.Size = 2
	compactor.Open()

	files, err := compactor.CompactFull([]string{f1, f2, f3}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	expGen, expSeq, err := tsm1.DefaultParseFileName(f3)
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}
	expSeq = expSeq + 1

	gotGen, gotSeq, err := tsm1.DefaultParseFileName(files[0])
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}

	if gotGen != expGen {
		t.Fatalf("wrong generation for new file: got %v, exp %v", gotGen, expGen)
	}

	if gotSeq != expSeq {
		t.Fatalf("wrong sequence for new file: got %v, exp %v", gotSeq, expSeq)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 1; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{a3, a4}},
	}

	for _, p := range data {
		values, err := r.ReadAll([]byte(p.key))
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch %s: got %v, exp %v", p.key, got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}

	if got, exp := len(r.Entries([]byte("cpu,host=A#!~#value"))), 1; got != exp {
		t.Fatalf("block count mismatch: got %v, exp %v", got, exp)
	}
}

// Ensures that a full compaction will decode and combine blocks with
// partial tombstoned values
func TestCompactor_CompactFull_TombstonedPartialBlock(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 3 TSM files with different data and one new point
	a1 := tsm1.NewValue(1, 1.1)
	a2 := tsm1.NewValue(2, 1.2)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1, a2},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	ts := tsm1.NewTombstoner(f1, nil)
	// a1 should remain after compaction
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 2, math.MaxInt64)

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	a3 := tsm1.NewValue(3, 1.3)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a3},
	}
	f2 := MustWriteTSM(dir, 2, writes)

	a4 := tsm1.NewValue(4, 1.4)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a4},
	}
	f3 := MustWriteTSM(dir, 3, writes)

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs
	compactor.Size = 2
	compactor.Open()

	files, err := compactor.CompactFull([]string{f1, f2, f3}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	expGen, expSeq, err := tsm1.DefaultParseFileName(f3)
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}
	expSeq = expSeq + 1

	gotGen, gotSeq, err := tsm1.DefaultParseFileName(files[0])
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}

	if gotGen != expGen {
		t.Fatalf("wrong generation for new file: got %v, exp %v", gotGen, expGen)
	}

	if gotSeq != expSeq {
		t.Fatalf("wrong sequence for new file: got %v, exp %v", gotSeq, expSeq)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 1; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{a1, a3, a4}},
	}

	for _, p := range data {
		values, err := r.ReadAll([]byte(p.key))
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch %s: got %v, exp %v", p.key, got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}

	if got, exp := len(r.Entries([]byte("cpu,host=A#!~#value"))), 2; got != exp {
		t.Fatalf("block count mismatch: got %v, exp %v", got, exp)
	}
}

// Ensures that a full compaction will decode and combine blocks with
// multiple tombstoned ranges within the block e.g. (t1, t2, t3, t4)
// having t2 and t3 removed
func TestCompactor_CompactFull_TombstonedMultipleRanges(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write 3 TSM files with different data and one new point
	a1 := tsm1.NewValue(1, 1.1)
	a2 := tsm1.NewValue(2, 1.2)
	a3 := tsm1.NewValue(3, 1.3)
	a4 := tsm1.NewValue(4, 1.4)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a1, a2, a3, a4},
	}
	f1 := MustWriteTSM(dir, 1, writes)

	ts := tsm1.NewTombstoner(f1, nil)
	// a1, a3 should remain after compaction
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 2, 2)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 4, 4)

	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	a5 := tsm1.NewValue(5, 1.5)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a5},
	}
	f2 := MustWriteTSM(dir, 2, writes)

	a6 := tsm1.NewValue(6, 1.6)
	writes = map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {a6},
	}
	f3 := MustWriteTSM(dir, 3, writes)

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs
	compactor.Size = 2
	compactor.Open()

	files, err := compactor.CompactFull([]string{f1, f2, f3}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 1; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	expGen, expSeq, err := tsm1.DefaultParseFileName(f3)
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}
	expSeq = expSeq + 1

	gotGen, gotSeq, err := tsm1.DefaultParseFileName(files[0])
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}

	if gotGen != expGen {
		t.Fatalf("wrong generation for new file: got %v, exp %v", gotGen, expGen)
	}

	if gotSeq != expSeq {
		t.Fatalf("wrong sequence for new file: got %v, exp %v", gotSeq, expSeq)
	}

	r := MustOpenTSMReader(files[0])

	if got, exp := r.KeyCount(), 1; got != exp {
		t.Fatalf("keys length mismatch: got %v, exp %v", got, exp)
	}

	var data = []struct {
		key    string
		points []tsm1.Value
	}{
		{"cpu,host=A#!~#value", []tsm1.Value{a1, a3, a5, a6}},
	}

	for _, p := range data {
		values, err := r.ReadAll([]byte(p.key))
		if err != nil {
			t.Fatalf("unexpected error reading: %v", err)
		}

		if got, exp := len(values), len(p.points); got != exp {
			t.Fatalf("values length mismatch %s: got %v, exp %v", p.key, got, exp)
		}

		for i, point := range p.points {
			assertValueEqual(t, values[i], point)
		}
	}

	if got, exp := len(r.Entries([]byte("cpu,host=A#!~#value"))), 2; got != exp {
		t.Fatalf("block count mismatch: got %v, exp %v", got, exp)
	}
}

// Ensures that a compaction will properly rollover to a new file when the
// max keys per blocks is exceeded
func TestCompactor_CompactFull_MaxKeys(t *testing.T) {
	// This test creates a lot of data and causes timeout failures for these envs
	if testing.Short() || os.Getenv("CI") != "" || os.Getenv("GORACE") != "" {
		t.Skip("Skipping max keys compaction test")
	}
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// write two files where the first contains a single key with the maximum
	// number of full blocks that can fit in a TSM file
	f1, f1Name := MustTSMWriter(dir, 1)
	values := make([]tsm1.Value, 1000)
	for i := 0; i < 65534; i++ {
		values = values[:0]
		for j := 0; j < 1000; j++ {
			values = append(values, tsm1.NewValue(int64(i*1000+j), int64(1)))
		}
		if err := f1.Write([]byte("cpu,host=A#!~#value"), values); err != nil {
			t.Fatalf("write tsm f1: %v", err)
		}
	}
	if err := f1.WriteIndex(); err != nil {
		t.Fatalf("write index f1: %v", err)
	}
	f1.Close()

	// Write a new file with 2 blocks that when compacted would exceed the max
	// blocks
	f2, f2Name := MustTSMWriter(dir, 2)
	for i := 0; i < 2; i++ {
		lastTimeStamp := values[len(values)-1].UnixNano() + 1
		values = values[:0]
		for j := lastTimeStamp; j < lastTimeStamp+1000; j++ {
			values = append(values, tsm1.NewValue(int64(j), int64(1)))
		}
		if err := f2.Write([]byte("cpu,host=A#!~#value"), values); err != nil {
			t.Fatalf("write tsm f1: %v", err)
		}
	}

	if err := f2.WriteIndex(); err != nil {
		t.Fatalf("write index f2: %v", err)
	}
	f2.Close()

	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs
	compactor.Open()

	// Compact both files, should get 2 files back
	files, err := compactor.CompactFull([]string{f1Name, f2Name}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	if got, exp := len(files), 2; got != exp {
		t.Fatalf("files length mismatch: got %v, exp %v", got, exp)
	}

	expGen, expSeq, err := tsm1.DefaultParseFileName(f2Name)
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}
	expSeq = expSeq + 1

	gotGen, gotSeq, err := tsm1.DefaultParseFileName(files[0])
	if err != nil {
		t.Fatalf("unexpected error parsing file name: %v", err)
	}

	if gotGen != expGen {
		t.Fatalf("wrong generation for new file: got %v, exp %v", gotGen, expGen)
	}

	if gotSeq != expSeq {
		t.Fatalf("wrong sequence for new file: got %v, exp %v", gotSeq, expSeq)
	}
}

func TestCompactor_CompactFull_InProgress(t *testing.T) {
	// This test creates a lot of data and causes timeout failures for these envs
	if testing.Short() || os.Getenv("CI") != "" || os.Getenv("GORACE") != "" {
		t.Skip("Skipping in progress compaction test")
	}
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f2Name := func() string {
		values := make([]tsm1.Value, 1000)

		// Write a new file with 2 blocks
		f2, f2Name := MustTSMWriter(dir, 2)
		defer func() {
			assert.NoError(t, f2.Close(), "closing TSM file %s", f2Name)
		}()
		for i := 0; i < 2; i++ {
			values = values[:0]
			for j := 0; j < 1000; j++ {
				values = append(values, tsm1.NewValue(int64(i*1000+j), int64(1)))
			}
			assert.NoError(t, f2.Write([]byte("cpu,host=A#!~#value"), values), "writing TSM file: %s", f2Name)
		}
		assert.NoError(t, f2.WriteIndex(), "writing TSM file index for %s", f2Name)
		return f2Name
	}()
	ffs := &fakeFileStore{}
	defer ffs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = ffs
	compactor.Open()

	expGen, expSeq, err := tsm1.DefaultParseFileName(f2Name)
	assert.NoError(t, err, "unexpected error parsing file name %s", f2Name)
	expSeq = expSeq + 1

	fileName := filepath.Join(compactor.Dir, tsm1.DefaultFormatFileName(expGen, expSeq)+"."+tsm1.TSMFileExtension+"."+tsm1.TmpTSMFileExtension)

	// Create a temp file to simulate an in progress compaction
	f, err := os.Create(fileName)
	assert.NoError(t, err, "creating in-progress compaction file %s", fileName)
	defer func() {
		assert.NoError(t, f.Close(), "closing in-progress compaction file %s", fileName)
	}()
	_, err = compactor.CompactFull([]string{f2Name}, zap.NewNop())
	assert.Errorf(t, err, "expected an error writing snapshot for %s", f2Name)
	e := errors.Unwrap(err)
	assert.NotNil(t, e, "expected an error wrapped by errCompactionInProgress")
	assert.Truef(t, errors.Is(e, fs.ErrExist), "error did not indicate file existence: %v", e)
	pathErr := &os.PathError{}
	assert.Truef(t, errors.As(e, &pathErr), "expected path error, got %v", e)
	assert.Truef(t, errors.Is(pathErr, fs.ErrExist), "error did not indicate file existence: %v", pathErr)
}

func newTSMKeyIterator(size int, fast bool, interrupt chan struct{}, readers ...*tsm1.TSMReader) (tsm1.KeyIterator, error) {
	files := []string{}
	for _, r := range readers {
		files = append(files, r.Path())
	}
	return tsm1.NewTSMBatchKeyIterator(size, fast, 0, interrupt, files, readers...)
}

// Tests that a single TSM file can be read and iterated over
func TestTSMKeyIterator_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(1, 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {v1},
	}

	r := MustTSMReader(dir, 1, writes)

	iter, err := newTSMKeyIterator(1, false, nil, r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	for iter.Next() {
		key, _, _, block, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		values, err := tsm1.DecodeBlock(block, nil)
		if err != nil {
			t.Fatalf("unexpected error decode: %v", err)
		}

		if got, exp := string(key), "cpu,host=A#!~#value"; got != exp {
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
func TestTSMKeyIterator_Duplicate(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(1, int64(1))
	v2 := tsm1.NewValue(1, int64(2))

	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {v1},
	}

	r1 := MustTSMReader(dir, 1, writes1)

	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {v2},
	}

	r2 := MustTSMReader(dir, 2, writes2)

	iter, err := newTSMKeyIterator(1, false, nil, r1, r2)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues bool
	for iter.Next() {
		key, _, _, block, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		values, err := tsm1.DecodeBlock(block, nil)
		if err != nil {
			t.Fatalf("unexpected error decode: %v", err)
		}

		if got, exp := string(key), "cpu,host=A#!~#value"; got != exp {
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

// Tests that deleted keys are not seen during iteration with
// TSM files.
func TestTSMKeyIterator_MultipleKeysDeleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(2, int64(1))
	points1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {v1},
	}

	r1 := MustTSMReader(dir, 1, points1)
	if e := r1.Delete([][]byte{[]byte("cpu,host=A#!~#value")}); nil != e {
		t.Fatal(e)
	}

	v2 := tsm1.NewValue(1, float64(1))
	v3 := tsm1.NewValue(1, float64(1))

	points2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#count": {v2},
		"cpu,host=B#!~#value": {v3},
	}

	r2 := MustTSMReader(dir, 2, points2)
	r2.Delete([][]byte{[]byte("cpu,host=A#!~#count")})

	iter, err := newTSMKeyIterator(1, false, nil, r1, r2)
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
		key, _, _, block, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		values, err := tsm1.DecodeBlock(block, nil)
		if err != nil {
			t.Fatalf("unexpected error decode: %v", err)
		}

		if got, exp := string(key), data[0].key; got != exp {
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

// Tests that deleted keys are not seen during iteration with
// TSM files.
func TestTSMKeyIterator_SingleDeletes(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(10, int64(1))
	v2 := tsm1.NewValue(20, int64(1))
	v3 := tsm1.NewValue(30, int64(1))
	v4 := tsm1.NewValue(40, int64(1))
	v5 := tsm1.NewValue(50, int64(1))
	v6 := tsm1.NewValue(60, int64(1))

	points1 := map[string][]tsm1.Value{
		"cpu,host=0#!~#value": {v1, v2},
		"cpu,host=A#!~#value": {v5, v6},
		"cpu,host=B#!~#value": {v3, v4},
		"cpu,host=C#!~#value": {v1, v2},
		"cpu,host=D#!~#value": {v1, v2},
	}

	r1 := MustTSMReader(dir, 1, points1)

	if e := r1.DeleteRange([][]byte{[]byte("cpu,host=A#!~#value")}, 50, 50); nil != e {
		t.Fatal(e)
	}
	if e := r1.DeleteRange([][]byte{[]byte("cpu,host=A#!~#value")}, 60, 60); nil != e {
		t.Fatal(e)
	}
	if e := r1.DeleteRange([][]byte{[]byte("cpu,host=C#!~#value")}, 10, 10); nil != e {
		t.Fatal(e)
	}
	if e := r1.DeleteRange([][]byte{[]byte("cpu,host=C#!~#value")}, 60, 60); nil != e {
		t.Fatal(e)
	}
	if e := r1.DeleteRange([][]byte{[]byte("cpu,host=C#!~#value")}, 20, 20); nil != e {
		t.Fatal(e)
	}

	iter, err := newTSMKeyIterator(1, false, nil, r1)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var readValues int
	var data = []struct {
		key   string
		value tsm1.Value
	}{
		{"cpu,host=0#!~#value", v1},
		{"cpu,host=B#!~#value", v3},
		{"cpu,host=D#!~#value", v1},
	}

	for iter.Next() {
		key, _, _, block, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		values, err := tsm1.DecodeBlock(block, nil)
		if err != nil {
			t.Fatalf("unexpected error decode: %v", err)
		}

		if exp, got := string(key), data[0].key; exp != got {
			t.Fatalf("key mismatch: got %v, exp %v", exp, got)
		}

		if exp, got := len(values), 2; exp != got {
			t.Fatalf("values length mismatch: exp %v, got %v", exp, got)
		}
		readValues++

		assertValueEqual(t, values[0], data[0].value)
		data = data[1:]
	}

	if exp, got := 3, readValues; exp != got {
		t.Fatalf("failed to read expected values: exp %v, got %v", exp, got)
	}
}

// Tests that the TSMKeyIterator will abort if the interrupt channel is closed
func TestTSMKeyIterator_Abort(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	v1 := tsm1.NewValue(1, 1.1)
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {v1},
	}

	r := MustTSMReader(dir, 1, writes)

	intC := make(chan struct{})
	iter, err := newTSMKeyIterator(1, false, intC, r)
	if err != nil {
		t.Fatalf("unexpected error creating WALKeyIterator: %v", err)
	}

	var aborted bool
	for iter.Next() {
		// Abort
		close(intC)

		_, _, _, _, err := iter.Read()
		if err == nil {
			t.Fatalf("unexpected error read: %v", err)
		}
		aborted = err != nil
	}

	if !aborted {
		t.Fatalf("iteration not aborted")
	}
}

func TestCacheKeyIterator_Single(t *testing.T) {
	v0 := tsm1.NewValue(1, 1.0)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {v0},
	}

	c := tsm1.NewCache(0)

	for k, v := range writes {
		if err := c.Write([]byte(k), v); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	iter := tsm1.NewCacheKeyIterator(c, 1, nil)
	var readValues bool
	for iter.Next() {
		key, _, _, block, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		values, err := tsm1.DecodeBlock(block, nil)
		if err != nil {
			t.Fatalf("unexpected error decode: %v", err)
		}

		if got, exp := string(key), "cpu,host=A#!~#value"; got != exp {
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

func TestCacheKeyIterator_Chunked(t *testing.T) {
	v0 := tsm1.NewValue(1, 1.0)
	v1 := tsm1.NewValue(2, 2.0)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {v0, v1},
	}

	c := tsm1.NewCache(0)

	for k, v := range writes {
		if err := c.Write([]byte(k), v); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	iter := tsm1.NewCacheKeyIterator(c, 1, nil)
	var readValues bool
	var chunk int
	for iter.Next() {
		key, _, _, block, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error read: %v", err)
		}

		values, err := tsm1.DecodeBlock(block, nil)
		if err != nil {
			t.Fatalf("unexpected error decode: %v", err)
		}

		if got, exp := string(key), "cpu,host=A#!~#value"; got != exp {
			t.Fatalf("key mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for _, v := range values {
			readValues = true
			assertValueEqual(t, v, writes["cpu,host=A#!~#value"][chunk])
		}
		chunk++
	}

	if !readValues {
		t.Fatalf("failed to read any values")
	}
}

// Tests that the CacheKeyIterator will abort if the interrupt channel is closed
func TestCacheKeyIterator_Abort(t *testing.T) {
	v0 := tsm1.NewValue(1, 1.0)

	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {v0},
	}

	c := tsm1.NewCache(0)

	for k, v := range writes {
		if err := c.Write([]byte(k), v); err != nil {
			t.Fatalf("failed to write key foo to cache: %s", err.Error())
		}
	}

	intC := make(chan struct{})

	iter := tsm1.NewCacheKeyIterator(c, 1, intC)

	var aborted bool
	for iter.Next() {
		//Abort
		close(intC)

		_, _, _, _, err := iter.Read()
		if err == nil {
			t.Fatalf("unexpected error read: %v", err)
		}
		aborted = err != nil
	}

	if !aborted {
		t.Fatalf("iteration not aborted")
	}
}

func TestDefaultPlanner_Plan_Min(t *testing.T) {
	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					{
						Path: "01-01.tsm1",
						Size: 1 * 1024 * 1024,
					},
					{
						Path: "02-01.tsm1",
						Size: 1 * 1024 * 1024,
					},
					{
						Path: "03-1.tsm1",
						Size: 251 * 1024 * 1024,
					},
				}
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	tsm, pLen := cp.Plan(time.Now())
	if exp, got := 0, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

// Ensure that if there are older files that can be compacted together but a newer
// file that is in a larger step, the older ones will get compacted.
func TestDefaultPlanner_Plan_CombineSequence(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-04.tsm1",
			Size: 128 * 1024 * 1024,
		},
		{
			Path: "02-04.tsm1",
			Size: 128 * 1024 * 1024,
		},
		{
			Path: "03-04.tsm1",
			Size: 128 * 1024 * 1024,
		},
		{
			Path: "04-04.tsm1",
			Size: 128 * 1024 * 1024,
		},
		{
			Path: "06-02.tsm1",
			Size: 67 * 1024 * 1024,
		},
		{
			Path: "07-02.tsm1",
			Size: 128 * 1024 * 1024,
		},
		{
			Path: "08-01.tsm1",
			Size: 251 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{data[0], data[1], data[2], data[3]}
	tsm, pLen := cp.Plan(time.Now())
	if exp, got := len(expFiles), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range expFiles {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

// Ensure that the planner grabs the smallest compaction step
func TestDefaultPlanner_Plan_MultipleGroups(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-04.tsm1",
			Size: 64 * 1024 * 1024,
		},
		{
			Path: "02-04.tsm1",
			Size: 64 * 1024 * 1024,
		},
		{
			Path: "03-04.tsm1",
			Size: 64 * 1024 * 1024,
		},
		{
			Path: "04-04.tsm1",
			Size: 129 * 1024 * 1024,
		},
		{
			Path: "05-04.tsm1",
			Size: 129 * 1024 * 1024,
		},
		{
			Path: "06-04.tsm1",
			Size: 129 * 1024 * 1024,
		},
		{
			Path: "07-04.tsm1",
			Size: 129 * 1024 * 1024,
		},
		{
			Path: "08-04.tsm1",
			Size: 129 * 1024 * 1024,
		},
		{
			Path: "09-04.tsm1", // should be skipped
			Size: 129 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(&fakeFileStore{
		PathsFn: func() []tsm1.FileStat {
			return data
		},
	}, tsdb.DefaultCompactFullWriteColdDuration)

	expFiles := []tsm1.FileStat{data[0], data[1], data[2], data[3],
		data[4], data[5], data[6], data[7]}
	tsm, pLen := cp.Plan(time.Now())

	if got, exp := len(tsm), 2; got != exp {
		t.Fatalf("compaction group length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	if exp, got := len(expFiles[:4]), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	if exp, got := len(expFiles[4:]), len(tsm[1]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	for i, p := range expFiles[:4] {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}

	for i, p := range expFiles[4:] {
		if got, exp := tsm[1][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

// Ensure that the planner grabs the smallest compaction step
func TestDefaultPlanner_PlanLevel_SmallestCompactionStep(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-03.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "02-03.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-03.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "04-03.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "05-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "06-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "07-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "08-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "09-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "10-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "11-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "12-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11]}
	tsm, pLen := cp.PlanLevel(1)
	if exp, got := len(expFiles), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range expFiles {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestDefaultPlanner_PlanLevel_SplitFile(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-03.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "02-03.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-03.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "03-04.tsm1",
			Size: 10 * 1024 * 1024,
		},
		{
			Path: "04-03.tsm1",
			Size: 10 * 1024 * 1024,
		},
		{
			Path: "05-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "06-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{data[0], data[1], data[2], data[3], data[4]}
	tsm, pLen := cp.PlanLevel(3)
	if exp, got := len(expFiles), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range expFiles {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestDefaultPlanner_PlanLevel_IsolatedHighLevel(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-02.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "02-02.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-03.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "03-04.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "04-02.tsm1",
			Size: 10 * 1024 * 1024,
		},
		{
			Path: "05-02.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "06-02.tsm1",
			Size: 1 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{}
	tsm, pLen := cp.PlanLevel(3)
	if exp, got := len(expFiles), len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

func TestDefaultPlanner_PlanLevel3_MinFiles(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-03.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "02-03.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-01.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "04-01.tsm1",
			Size: 10 * 1024 * 1024,
		},
		{
			Path: "05-02.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "06-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{}
	tsm, pLen := cp.PlanLevel(3)
	if exp, got := len(expFiles), len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

func TestDefaultPlanner_PlanLevel2_MinFiles(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "02-04.tsm1",
			Size: 251 * 1024 * 1024,
		},

		{
			Path: "03-02.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "03-03.tsm1",
			Size: 1 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{}
	tsm, pLen := cp.PlanLevel(2)
	if exp, got := len(expFiles), len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

func TestDefaultPlanner_PlanLevel_Tombstone(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path:         "01-03.tsm1",
			Size:         251 * 1024 * 1024,
			HasTombstone: true,
		},
		{
			Path: "02-03.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-01.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "04-01.tsm1",
			Size: 10 * 1024 * 1024,
		},
		{
			Path: "05-02.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "06-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{data[0], data[1]}
	tsm, pLen := cp.PlanLevel(3)
	if exp, got := len(expFiles), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range expFiles {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestDefaultPlanner_PlanLevel_Multiple(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-01.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "02-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-01.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "04-01.tsm1",
			Size: 10 * 1024 * 1024,
		},
		{
			Path: "05-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "06-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "07-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "08-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles1 := []tsm1.FileStat{data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]}

	tsm, pLen := cp.PlanLevel(1)
	if exp, got := len(expFiles1), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range expFiles1 {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestDefaultPlanner_PlanLevel_InUse(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-01.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "02-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-01.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "04-01.tsm1",
			Size: 10 * 1024 * 1024,
		},
		{
			Path: "05-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "06-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "07-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "08-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "09-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "10-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "11-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "12-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "13-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "14-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "15-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "16-01.tsm1",
			Size: 1 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles1 := data[0:8]
	expFiles2 := data[8:16]

	tsm, pLen := cp.PlanLevel(1)
	if exp, got := len(expFiles1), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range expFiles1 {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}

	if exp, got := len(expFiles2), len(tsm[1]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	for i, p := range expFiles2 {
		if got, exp := tsm[1][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}

	cp.Release(tsm[1:])

	tsm, pLen = cp.PlanLevel(1)
	if exp, got := len(expFiles2), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range expFiles2 {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestDefaultPlanner_PlanOptimize_NoLevel4(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-03.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "02-03.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-03.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{}
	tsm, pLen := cp.PlanOptimize()
	if exp, got := len(expFiles), len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

func TestDefaultPlanner_PlanOptimize_Level4(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-04.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "02-04.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-04.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "04-04.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "05-03.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "06-04.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "07-03.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles1 := []tsm1.FileStat{data[0], data[1], data[2], data[3], data[4], data[5]}
	tsm, pLen := cp.PlanOptimize()
	if exp, got := 1, len(tsm); exp != got {
		t.Fatalf("group length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	if exp, got := len(expFiles1), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	for i, p := range expFiles1 {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestDefaultPlanner_PlanOptimize_Multiple(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-04.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "02-04.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "03-04.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "04-04.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "05-03.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "06-03.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "07-04.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "08-04.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "09-04.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
		{
			Path: "10-04.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles1 := []tsm1.FileStat{data[0], data[1], data[2], data[3]}
	expFiles2 := []tsm1.FileStat{data[6], data[7], data[8], data[9]}

	tsm, pLen := cp.PlanOptimize()
	if exp, got := 2, len(tsm); exp != got {
		t.Fatalf("group length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	if exp, got := len(expFiles1), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	for i, p := range expFiles1 {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}

	if exp, got := len(expFiles2), len(tsm[1]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	}

	for i, p := range expFiles2 {
		if got, exp := tsm[1][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestDefaultPlanner_PlanOptimize_Optimized(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-03.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path: "01-04.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "01-05.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{}
	tsm, pLen := cp.PlanOptimize()
	if exp, got := len(expFiles), len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

func TestDefaultPlanner_PlanOptimize_Tombstones(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-04.tsm1",
			Size: 251 * 1024 * 1024,
		},
		{
			Path:         "01-05.tsm1",
			Size:         1 * 1024 * 1024,
			HasTombstone: true,
		},
		{
			Path: "02-06.tsm1",
			Size: 2 * 1024 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{data[0], data[1], data[2]}
	tsm, pLen := cp.PlanOptimize()
	if exp, got := len(expFiles), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range expFiles {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}

}

// Ensure that the planner will compact all files if no writes
// have happened in some interval
func TestDefaultPlanner_Plan_FullOnCold(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-01.tsm1",
			Size: 513 * 1024 * 1024,
		},
		{
			Path: "02-02.tsm1",
			Size: 129 * 1024 * 1024,
		},
		{
			Path: "03-02.tsm1",
			Size: 33 * 1024 * 1024,
		},
		{
			Path: "04-02.tsm1",
			Size: 1 * 1024 * 1024,
		},
		{
			Path: "05-02.tsm1",
			Size: 10 * 1024 * 1024,
		},
		{
			Path: "06-01.tsm1",
			Size: 2 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		},
		time.Nanosecond,
	)

	tsm, pLen := cp.Plan(time.Now().Add(-time.Second))
	if exp, got := len(data), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range data {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

// Ensure that the planner will not return files that are over the max
// allowable size
func TestDefaultPlanner_Plan_SkipMaxSizeFiles(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-01.tsm1",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "02-02.tsm1",
			Size: 2049 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	tsm, pLen := cp.Plan(time.Now())
	if exp, got := 0, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

// Ensure that the planner will not return files that are over the max
// allowable size
func TestDefaultPlanner_Plan_SkipPlanningAfterFull(t *testing.T) {
	testSet := []tsm1.FileStat{
		{
			Path: "01-05.tsm1",
			Size: 256 * 1024 * 1024,
		},
		{
			Path: "02-05.tsm1",
			Size: 256 * 1024 * 1024,
		},
		{
			Path: "03-05.tsm1",
			Size: 256 * 1024 * 1024,
		},
		{
			Path: "04-04.tsm1",
			Size: 256 * 1024 * 1024,
		},
	}

	ffs := &fakeFileStore{
		PathsFn: func() []tsm1.FileStat {
			return testSet
		},
		blockCount: 1000,
	}

	cp := tsm1.NewDefaultPlanner(ffs, time.Nanosecond)
	plan, pLen := cp.Plan(time.Now().Add(-time.Second))
	// first verify that our test set would return files
	if exp, got := 4, len(plan[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(plan)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(plan)

	// skip planning if all files are over the limit
	over := []tsm1.FileStat{
		{
			Path: "01-05.tsm1",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "02-05.tsm1",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "03-05.tsm1",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "04-05.tsm1",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "05-05.tsm1",
			Size: 2049 * 1024 * 1024,
		},
	}

	overFs := &fakeFileStore{
		PathsFn: func() []tsm1.FileStat {
			return over
		},
		blockCount: 1000,
	}

	cp.FileStore = overFs
	plan, pLen = cp.Plan(time.Now().Add(-time.Second))
	if exp, got := 0, len(plan); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(plan)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(plan)

	plan, pLen = cp.PlanOptimize()
	// ensure the optimize planner would pick this up
	if exp, got := 1, len(plan); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(plan)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(plan)

	cp.FileStore = ffs
	// ensure that it will plan if last modified has changed
	ffs.lastModified = time.Now()

	cGroups, pLen := cp.Plan(time.Now())
	if exp, got := 4, len(cGroups[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(cGroups)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

// Tests that 2 generations, each over 2 GB and the second in level 2 does
// not return just the first generation.  This was a case where full planning
// would get repeatedly plan the same files and never stop.
func TestDefaultPlanner_Plan_TwoGenLevel3(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "000002245-000001666.tsm",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "000002245-000001667.tsm",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "000002245-000001668.tsm",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "000002245-000001669.tsm",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "000002245-000001670.tsm",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "000002245-000001671.tsm",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "000002245-000001672.tsm",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "000002245-000001673.tsm",
			Size: 192631258,
		},
		{
			Path: "000002246-000000002.tsm",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "000002246-000000003.tsm",
			Size: 192631258,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			blockCount: 1000,
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		},
		time.Hour)

	tsm, pLen := cp.Plan(time.Now().Add(-24 * time.Hour))
	if exp, got := 1, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

// Ensure that the planner will return files over the max file
// size, but do not contain full blocks
func TestDefaultPlanner_Plan_NotFullOverMaxsize(t *testing.T) {
	testSet := []tsm1.FileStat{
		{
			Path: "01-05.tsm1",
			Size: 256 * 1024 * 1024,
		},
		{
			Path: "02-05.tsm1",
			Size: 256 * 1024 * 1024,
		},
		{
			Path: "03-05.tsm1",
			Size: 256 * 1024 * 1024,
		},
		{
			Path: "04-04.tsm1",
			Size: 256 * 1024 * 1024,
		},
	}

	ffs := &fakeFileStore{
		PathsFn: func() []tsm1.FileStat {
			return testSet
		},
		blockCount: 100,
	}

	cp := tsm1.NewDefaultPlanner(
		ffs,
		time.Nanosecond,
	)

	plan, pLen := cp.Plan(time.Now().Add(-time.Second))
	// first verify that our test set would return files
	if exp, got := 4, len(plan[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(plan)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(plan)

	// skip planning if all files are over the limit
	over := []tsm1.FileStat{
		{
			Path: "01-05.tsm1",
			Size: 2049 * 1024 * 1024,
		},
		{
			Path: "02-05.tsm1",
			Size: 2049 * 1024 * 1024,
		},
	}

	overFs := &fakeFileStore{
		PathsFn: func() []tsm1.FileStat {
			return over
		},
		blockCount: 100,
	}

	cp.FileStore = overFs
	cGroups, pLen := cp.Plan(time.Now().Add(-time.Second))
	if exp, got := 1, len(cGroups); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(cGroups)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

// Ensure that the planner will compact files that are past the smallest step
// size even if there is a single file in the smaller step size
func TestDefaultPlanner_Plan_CompactsMiddleSteps(t *testing.T) {
	data := []tsm1.FileStat{
		{
			Path: "01-04.tsm1",
			Size: 64 * 1024 * 1024,
		},
		{
			Path: "02-04.tsm1",
			Size: 64 * 1024 * 1024,
		},
		{
			Path: "03-04.tsm1",
			Size: 64 * 1024 * 1024,
		},
		{
			Path: "04-04.tsm1",
			Size: 64 * 1024 * 1024,
		},
		{
			Path: "05-02.tsm1",
			Size: 2 * 1024 * 1024,
		},
	}

	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return data
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	expFiles := []tsm1.FileStat{data[0], data[1], data[2], data[3]}
	tsm, pLen := cp.Plan(time.Now())
	if exp, got := len(expFiles), len(tsm[0]); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	for i, p := range expFiles {
		if got, exp := tsm[0][i], p.Path; got != exp {
			t.Fatalf("tsm file mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestDefaultPlanner_Plan_LargeGeneration(t *testing.T) {
	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					{
						Path: "000000278-000000006.tsm",
						Size: 2148340232,
					},
					{
						Path: "000000278-000000007.tsm",
						Size: 2148356556,
					},
					{
						Path: "000000278-000000008.tsm",
						Size: 167780181,
					},
					{
						Path: "000000278-000047040.tsm",
						Size: 2148728539,
					},
					{
						Path: "000000278-000047041.tsm",
						Size: 701863692,
					},
				}
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	tsm, pLen := cp.Plan(time.Now())
	if exp, got := 0, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
}

func TestDefaultPlanner_Plan_ForceFull(t *testing.T) {
	cp := tsm1.NewDefaultPlanner(
		&fakeFileStore{
			PathsFn: func() []tsm1.FileStat {
				return []tsm1.FileStat{
					{
						Path: "000000001-000000001.tsm",
						Size: 2148340232,
					},
					{
						Path: "000000002-000000001.tsm",
						Size: 2148356556,
					},
					{
						Path: "000000003-000000001.tsm",
						Size: 167780181,
					},
					{
						Path: "000000004-000000001.tsm",
						Size: 2148728539,
					},
					{
						Path: "000000005-000000001.tsm",
						Size: 2148340232,
					},
					{
						Path: "000000006-000000001.tsm",
						Size: 2148356556,
					},
					{
						Path: "000000007-000000001.tsm",
						Size: 167780181,
					},
					{
						Path: "000000008-000000001.tsm",
						Size: 2148728539,
					},
					{
						Path: "000000009-000000002.tsm",
						Size: 701863692,
					},
					{
						Path: "000000010-000000002.tsm",
						Size: 701863692,
					},
					{
						Path: "000000011-000000002.tsm",
						Size: 701863692,
					},
					{
						Path: "000000012-000000002.tsm",
						Size: 701863692,
					},
					{
						Path: "000000013-000000002.tsm",
						Size: 701863692,
					},
				}
			},
		}, tsdb.DefaultCompactFullWriteColdDuration,
	)

	tsm, pLen := cp.PlanLevel(1)
	if exp, got := 1, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(tsm)

	tsm, pLen = cp.PlanLevel(2)
	if exp, got := 1, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(tsm)

	cp.ForceFull()

	// Level plans should not return any plans
	tsm, pLen = cp.PlanLevel(1)
	if exp, got := 0, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(tsm)

	tsm, pLen = cp.PlanLevel(2)
	if exp, got := 0, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(tsm)

	tsm, pLen = cp.Plan(time.Now())
	if exp, got := 1, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}

	if got, exp := len(tsm[0]), 13; got != exp {
		t.Fatalf("plan length mismatch: got %v, exp %v", got, exp)
	}
	cp.Release(tsm)

	// Level plans should return plans now that Plan has been called
	tsm, pLen = cp.PlanLevel(1)
	if exp, got := 1, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(tsm)

	tsm, pLen = cp.PlanLevel(2)
	if exp, got := 1, len(tsm); got != exp {
		t.Fatalf("tsm file length mismatch: got %v, exp %v", got, exp)
	} else if pLen != int64(len(tsm)) {
		t.Fatalf("tsm file plan length mismatch: got %v, exp %v", pLen, exp)
	}
	cp.Release(tsm)

}

func assertValueEqual(t *testing.T, a, b tsm1.Value) {
	if got, exp := a.UnixNano(), b.UnixNano(); got != exp {
		t.Fatalf("time mismatch: got %v, exp %v", got, exp)
	}
	if got, exp := a.Value(), b.Value(); got != exp {
		t.Fatalf("value mismatch: got %v, exp %v", got, exp)
	}
}

func MustTSMWriter(dir string, gen int) (tsm1.TSMWriter, string) {
	f := MustTempFile(dir)
	oldName := f.Name()

	// Windows can't rename a file while it's open.  Close first, rename and
	// then re-open
	if err := f.Close(); err != nil {
		panic(fmt.Sprintf("close temp file: %v", err))
	}

	newName := filepath.Join(filepath.Dir(oldName), tsm1.DefaultFormatFileName(gen, 1)+".tsm")
	if err := os.Rename(oldName, newName); err != nil {
		panic(fmt.Sprintf("create tsm file: %v", err))
	}

	var err error
	f, err = os.OpenFile(newName, os.O_RDWR, 0666)
	if err != nil {
		panic(fmt.Sprintf("open tsm files: %v", err))
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		panic(fmt.Sprintf("create TSM writer: %v", err))
	}

	return w, newName
}

func MustWriteTSM(dir string, gen int, values map[string][]tsm1.Value) string {
	w, name := MustTSMWriter(dir, gen)

	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if err := w.Write([]byte(k), values[k]); err != nil {
			panic(fmt.Sprintf("write TSM value: %v", err))
		}
	}

	if err := w.WriteIndex(); err != nil {
		panic(fmt.Sprintf("write TSM index: %v", err))
	}

	if err := w.Close(); err != nil {
		panic(fmt.Sprintf("write TSM close: %v", err))
	}

	return name
}

func MustTSMReader(dir string, gen int, values map[string][]tsm1.Value) *tsm1.TSMReader {
	return MustOpenTSMReader(MustWriteTSM(dir, gen, values))
}

func MustOpenTSMReader(name string) *tsm1.TSMReader {
	f, err := os.Open(name)
	if err != nil {
		panic(fmt.Sprintf("open file: %v", err))
	}

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		panic(fmt.Sprintf("new reader: %v", err))
	}
	return r
}

type fakeFileStore struct {
	PathsFn      func() []tsm1.FileStat
	lastModified time.Time
	blockCount   int
	readers      []*tsm1.TSMReader
}

func (w *fakeFileStore) Stats() []tsm1.FileStat {
	return w.PathsFn()
}

func (w *fakeFileStore) NextGeneration() int {
	return 1
}

func (w *fakeFileStore) LastModified() time.Time {
	return w.lastModified
}

func (w *fakeFileStore) BlockCount(path string, idx int) int {
	return w.blockCount
}

func (w *fakeFileStore) TSMReader(path string) (*tsm1.TSMReader, error) {
	r := MustOpenTSMReader(path)
	w.readers = append(w.readers, r)
	r.Ref()
	return r, nil
}

func (w *fakeFileStore) Close() {
	for _, r := range w.readers {
		r.Close()
	}
	w.readers = nil
}

func (w *fakeFileStore) ParseFileName(path string) (int, int, error) {
	return tsm1.DefaultParseFileName(path)
}
