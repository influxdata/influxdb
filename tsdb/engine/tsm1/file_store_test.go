package tsm1_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestFileStore_Read(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	values, err := fs.Read([]byte("cpu"), 1)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[1]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToAsc_FromStart(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	// Search for an entry that exists in the second file
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[0]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToAsc_Duplicate(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 4.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	// Search for an entry that exists in the second file
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[1].values[0],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[3].values[0],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToAsc_BeforeStart(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[0]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapFloat(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 4.0), tsm1.NewValue(2, 7.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapInteger(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(0)), tsm1.NewValue(1, int64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(4)), tsm1.NewValue(2, int64(7))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapUnsigned(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(0)), tsm1.NewValue(1, uint64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(4)), tsm1.NewValue(2, uint64(7))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapBoolean(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, true), tsm1.NewValue(1, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, false), tsm1.NewValue(2, true)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapString(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "zero"), tsm1.NewValue(1, "one")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "two")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, "three")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "four"), tsm1.NewValue(2, "seven")}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinFloat(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0), tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0), tsm1.NewValue(4, 4.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.1)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.2)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	// Search for an entry that exists in the second file
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinInteger(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, int64(1)), tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2)), tsm1.NewValue(4, int64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(0)), tsm1.NewValue(1, int64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(5))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	// Search for an entry that exists in the second file
	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinUnsigned(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, uint64(1)), tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2)), tsm1.NewValue(4, uint64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(0)), tsm1.NewValue(1, uint64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(5))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	// Search for an entry that exists in the second file
	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinBoolean(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, true), tsm1.NewValue(3, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true), tsm1.NewValue(4, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, true), tsm1.NewValue(1, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, false)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	// Search for an entry that exists in the second file
	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinString(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, "1.0"), tsm1.NewValue(3, "3.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.0"), tsm1.NewValue(4, "4.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "0.0"), tsm1.NewValue(1, "1.1")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.2")}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	// Search for an entry that exists in the second file
	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToAsc_Middle(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0),
			tsm1.NewValue(2, 2.0),
			tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(4, 4.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 3, true)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{data[0].values[2]}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{data[1].values[0]}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

}

func TestFileStore_SeekToAsc_End(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 2, true)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[2]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_FromStart(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, false)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp := data[0]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_Duplicate(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 4.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 2, false)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp := []tsm1.Value{
		data[3].values[0],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[1].values[0],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_OverlapMaxFloat(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0), tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0), tsm1.NewValue(4, 4.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.1)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.2)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{

		data[2].values[0],
		data[2].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_OverlapMaxInteger(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, int64(1)), tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2)), tsm1.NewValue(4, int64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(0)), tsm1.NewValue(1, int64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(5))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	t.Cleanup(c.Close)

	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}
func TestFileStore_SeekToDesc_OverlapMaxUnsigned(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, uint64(1)), tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2)), tsm1.NewValue(4, uint64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(0)), tsm1.NewValue(1, uint64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(5))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	t.Cleanup(c.Close)

	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_OverlapMaxBoolean(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, true), tsm1.NewValue(3, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true), tsm1.NewValue(4, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, true), tsm1.NewValue(1, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, false)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	t.Cleanup(c.Close)

	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_OverlapMaxString(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, "1.0"), tsm1.NewValue(3, "3.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.0"), tsm1.NewValue(4, "4.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "0.0"), tsm1.NewValue(1, "1.1")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.2")}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	t.Cleanup(c.Close)

	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_AfterEnd(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 4, false)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[2]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapFloat(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 4 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, 0.0), tsm1.NewValue(9, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 4.0), tsm1.NewValue(7, 7.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 10, false)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[0].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[3].values[0],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapInteger(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, int64(0)), tsm1.NewValue(9, int64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, int64(4)), tsm1.NewValue(10, int64(7))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
	t.Cleanup(c.Close)

	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[0],
		data[0].values[1],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapUnsigned(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, uint64(0)), tsm1.NewValue(9, uint64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, uint64(4)), tsm1.NewValue(10, uint64(7))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
	t.Cleanup(c.Close)

	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[0],
		data[0].values[1],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapBoolean(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, true), tsm1.NewValue(9, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, true), tsm1.NewValue(7, false)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
	t.Cleanup(c.Close)

	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[0].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[3].values[0],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapString(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, "eight"), tsm1.NewValue(9, "nine")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "two")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, "three")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, "four"), tsm1.NewValue(7, "seven")}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
	t.Cleanup(c.Close)

	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[0].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[3].values[0],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_Middle(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(2, 2.0),
			tsm1.NewValue(3, 3.0),
			tsm1.NewValue(4, 4.0)},
		},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 3, false)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[1].values[0],
		data[1].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[0].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_End(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 2, false)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[2]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	expValues := []int{0, 2}
	for _, v := range expValues {
		values, err := c.ReadFloatBlock(&buf)
		if err != nil {
			t.Fatalf("unexpected error reading values: %v", err)
		}

		exp := data[v]
		if got, exp := len(values), 1; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[0].String(), exp.values[0].String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
		c.Next()
	}
}

func TestKeyCursor_TombstoneRange_PartialFirst(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	// Delete part of the block in the first file.
	r := MustOpenTSMReader(files[0])
	r.DeleteRange([][]byte{[]byte("cpu")}, 1, 3)
	t.Cleanup(func() { r.Close() })

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	expValues := []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(2, 2.0)}

	for _, exp := range expValues {
		values, err := c.ReadFloatBlock(&buf)
		if err != nil {
			t.Fatalf("unexpected error reading values: %v", err)
		}

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[0].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
		c.Next()
	}
}

func TestKeyCursor_TombstoneRange_PartialFloat(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, 1.0),
			tsm1.NewValue(1, 2.0),
			tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange_PartialInteger(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, int64(1)),
			tsm1.NewValue(1, int64(2)),
			tsm1.NewValue(2, int64(3))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange_PartialUnsigned(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, uint64(1)),
			tsm1.NewValue(1, uint64(2)),
			tsm1.NewValue(2, uint64(3))}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange_PartialString(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, "1"),
			tsm1.NewValue(1, "2"),
			tsm1.NewValue(2, "3")}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange_PartialBoolean(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, true),
			tsm1.NewValue(1, false),
			tsm1.NewValue(2, true)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	t.Cleanup(c.Close)

	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestFileStore_Open(t *testing.T) {
	dir := t.TempDir()

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	_, err := newFileDir(t, dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := newTestFileStore(t, dir)
	if err := fs.Open(context.Background()); err != nil {
		fatal(t, "opening file store", err)
	}

	if got, exp := fs.Count(), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := fs.CurrentGeneration(), 4; got != exp {
		t.Fatalf("current ID mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_OpenFail(t *testing.T) {
	var err error
	dir := t.TempDir()

	// Create a TSM file...
	data := keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}}

	files, err := newFileDir(t, dir, data)
	if err != nil {
		fatal(t, "creating test files", err)
	}
	assert.Equal(t, 1, len(files))
	f := files[0]

	const mmapErrMsg = "mmap failure in test"
	const fullMmapErrMsg = "system limit for vm.max_map_count may be too low: " + mmapErrMsg
	// With an mmap failure, the files should all be left where they are, because they are not corrupt
	openFail(t, dir, fullMmapErrMsg, tsm1.NewMmapError(fmt.Errorf(mmapErrMsg)))
	assert.FileExistsf(t, f, "file not found, but should not have been moved for mmap failure")

	// With a non-mmap failure, the file failing to open should be moved aside
	const otherErrMsg = "some Random Init Failure"
	openFail(t, dir, otherErrMsg, fmt.Errorf(otherErrMsg))
	assert.NoFileExistsf(t, f, "file found, but should have been moved for open failure")
	assert.FileExistsf(t, f+"."+tsm1.BadTSMFileExtension, "file not found, but should have been moved here for open failure")
}

func openFail(t *testing.T, dir string, fullErrMsg string, initErr error) {
	fs := tsm1.NewFileStore(dir, tsdb.EngineTags{}, tsm1.TestMmapInitFailOption(initErr))
	err := fs.Open(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), fullErrMsg)
	defer func() { assert.NoError(t, fs.Close(), "unexpected error on FileStore.Close") }()
	assert.Equal(t, 0, fs.Count(), "file count mismatch")
}

func TestFileStore_Remove(t *testing.T) {
	dir := t.TempDir()

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFileDir(t, dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := newTestFileStore(t, dir)
	if err := fs.Open(context.Background()); err != nil {
		fatal(t, "opening file store", err)
	}

	if got, exp := fs.Count(), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := fs.CurrentGeneration(), 4; got != exp {
		t.Fatalf("current ID mismatch: got %v, exp %v", got, exp)
	}

	fs.Replace(files[2:3], nil)

	if got, exp := fs.Count(), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := fs.CurrentGeneration(), 4; got != exp {
		t.Fatalf("current ID mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Replace(t *testing.T) {
	dir := t.TempDir()

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFileDir(t, dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	// Replace requires assumes new files have a .tmp extension
	replacement := fmt.Sprintf("%s.%s", files[2], tsm1.TmpTSMFileExtension)
	os.Rename(files[2], replacement)

	fs := newTestFileStore(t, dir)
	if err := fs.Open(context.Background()); err != nil {
		fatal(t, "opening file store", err)
	}

	if got, exp := fs.Count(), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Should record references to the two existing TSM files
	cur := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)

	// Should move the existing files out of the way, but allow query to complete
	if err := fs.Replace(files[:2], []string{replacement}); err != nil {
		t.Fatalf("replace: %v", err)
	}

	if got, exp := fs.Count(), 1; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// There should be two blocks (1 in each file)
	cur.Next()
	buf := make([]tsm1.FloatValue, 10)
	values, err := cur.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := len(values), 1; got != exp {
		t.Fatalf("value len mismatch: got %v, exp %v", got, exp)
	}

	cur.Next()
	values, err = cur.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := len(values), 1; got != exp {
		t.Fatalf("value len mismatch: got %v, exp %v", got, exp)
	}

	// No more blocks for this cursor
	cur.Next()
	values, err = cur.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value len mismatch: got %v, exp %v", got, exp)
	}

	// Release the references (files should get evicted by purger shortly)
	cur.Close()

	time.Sleep(time.Second)
	// Make sure the two TSM files used by the cursor are gone
	if _, err := os.Stat(files[0]); !os.IsNotExist(err) {
		t.Fatalf("stat file: %v", err)
	}
	if _, err := os.Stat(files[1]); !os.IsNotExist(err) {
		t.Fatalf("stat file: %v", err)
	}

	// Make sure the new file exists
	if _, err := os.Stat(files[2]); err != nil {
		t.Fatalf("stat file: %v", err)
	}

}

func TestFileStore_Open_Deleted(t *testing.T) {
	dir := t.TempDir()

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu,host=server2!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	_, err := newFileDir(t, dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := newTestFileStore(t, dir)
	if err := fs.Open(context.Background()); err != nil {
		fatal(t, "opening file store", err)
	}

	if got, exp := len(fs.Keys()), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if err := fs.Delete([][]byte{[]byte("cpu,host=server2!~#!value")}); err != nil {
		fatal(t, "deleting", err)
	}

	fs2 := newTestFileStore(t, dir)
	if err := fs2.Open(context.Background()); err != nil {
		fatal(t, "opening file store", err)
	}

	if got, exp := len(fs2.Keys()), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Delete(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu,host=server2!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	keys := fs.Keys()
	if got, exp := len(keys), 3; got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	if err := fs.Delete([][]byte{[]byte("cpu,host=server2!~#!value")}); err != nil {
		fatal(t, "deleting", err)
	}

	keys = fs.Keys()
	if got, exp := len(keys), 2; got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Apply(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu,host=server2#!~#value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu,host=server1#!~#value", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem,host=server1#!~#value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	keys := fs.Keys()
	if got, exp := len(keys), 3; got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	var n int64
	if err := fs.Apply(context.Background(), func(r tsm1.TSMFile) error {
		atomic.AddInt64(&n, 1)
		return nil
	}); err != nil {
		t.Fatalf("unexpected error deleting: %v", err)
	}

	if got, exp := n, int64(3); got != exp {
		t.Fatalf("apply mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Stats(t *testing.T) {
	dir := t.TempDir()

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFileDir(t, dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := newTestFileStore(t, dir)
	if err := fs.Open(context.Background()); err != nil {
		fatal(t, "opening file store", err)
	}

	stats := fs.Stats()
	if got, exp := len(stats), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Another call should result in the same stats being returned.
	if got, exp := fs.Stats(), stats; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, exp %v", got, exp)
	}

	// Removing one of the files should invalidate the cache.
	fs.Replace(files[0:1], nil)
	if got, exp := len(fs.Stats()), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Write a new TSM file that that is not open
	newFile := MustWriteTSM(t, dir, 4, map[string][]tsm1.Value{
		"mem": []tsm1.Value{tsm1.NewValue(0, 1.0)},
	})

	replacement := fmt.Sprintf("%s.%s.%s", files[2], tsm1.TmpTSMFileExtension, tsm1.TSMFileExtension) // Assumes new files have a .tmp extension
	if err := os.Rename(newFile, replacement); err != nil {
		t.Fatalf("rename: %v", err)
	}
	// Replace 3 w/ 1
	if err := fs.Replace(files, []string{replacement}); err != nil {
		t.Fatalf("replace: %v", err)
	}

	var found bool
	stats = fs.Stats()
	for _, stat := range stats {
		if strings.HasSuffix(stat.Path, fmt.Sprintf("%s.%s.%s", tsm1.TSMFileExtension, tsm1.TmpTSMFileExtension, tsm1.TSMFileExtension)) {
			found = true
		}
	}

	if !found {
		t.Fatalf("Didn't find %s in stats: %v", "foo", stats)
	}

	newFile = MustWriteTSM(t, dir, 5, map[string][]tsm1.Value{
		"mem": []tsm1.Value{tsm1.NewValue(0, 1.0)},
	})

	// Adding some files should invalidate the cache.
	fs.Replace(nil, []string{newFile})
	if got, exp := len(fs.Stats()), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_CreateSnapshot(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Create a tombstone
	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	s, e := fs.CreateSnapshot()
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("temp file for hard links: %q", s)

	tfs, e := os.ReadDir(s)
	if e != nil {
		t.Fatal(e)
	}
	if len(tfs) == 0 {
		t.Fatal("no files found")
	}

	for _, f := range fs.Files() {
		p := filepath.Join(s, filepath.Base(f.Path()))
		t.Logf("checking for existence of hard link %q", p)
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Fatalf("unable to find file %q", p)
		}
		if ts := f.TombstoneStats(); ts.TombstoneExists {
			p := filepath.Join(s, filepath.Base(ts.Path))
			t.Logf("checking for existence of hard link %q", p)
			if _, err := os.Stat(p); os.IsNotExist(err) {
				t.Fatalf("unable to find file %q", p)
			}
		}
	}
}

// newTestFileStore returns a FileStore for testing. The FileStore is closed by
// tb.Cleanup when the test and all its subtests complete.
func newTestFileStore(tb testing.TB, dir string) *tsm1.FileStore {
	fs := tsm1.NewFileStore(dir, tsdb.EngineTags{})

	tb.Cleanup(func() {
		fs.Close()
	})

	return fs
}

func TestFileStore_ReaderBlocking(t *testing.T) {
	dir := t.TempDir()

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFileDir(t, dir, data...)
	require.NoError(t, err)

	fs := newTestFileStore(t, dir)
	require.NoError(t, fs.Open(context.Background()))

	fsInUse := func() bool {
		t.Helper()
		require.NoError(t, fs.SetNewReadersBlocked(true))
		defer func() {
			t.Helper()
			require.NoError(t, fs.SetNewReadersBlocked(false))
		}()
		inUse, err := fs.InUse()
		require.NoError(t, err)
		return inUse
	}

	checkUnblocked := func() {
		t.Helper()

		require.False(t, fsInUse())

		var applyCount atomic.Uint32
		err = fs.Apply(context.Background(), func(r tsm1.TSMFile) error {
			applyCount.Add(1)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, uint32(len(files)), applyCount.Load(), "Apply should be called for all files")

		snap, err := fs.CreateSnapshot()
		require.NoError(t, err)
		require.NotEmpty(t, snap)

		buf := make([]tsm1.FloatValue, 1000)
		c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
		// closeC exists because we want to call c.Close() if a test fails in a defer,
		// but we also need to call c.Close() as part of the test. closeC makes sure we
		// don't double close it.
		closeC := func() {
			t.Helper()
			if c != nil {
				c.Close() // Close does not return anything
				c = nil
			}
		}
		defer closeC()
		require.NotNil(t, c)
		require.True(t, fsInUse())

		values, err := c.ReadFloatBlock(&buf)
		require.NoError(t, err)
		require.Len(t, values, 1)
		c.Next()
		values, err = c.ReadFloatBlock(&buf)
		require.NoError(t, err)
		require.Len(t, values, 1)
		c.Next()
		values, err = c.ReadFloatBlock(&buf)
		require.NoError(t, err)
		require.Empty(t, values)
		closeC()
		require.False(t, fsInUse())

		r, err := fs.TSMReader(files[0])
		require.NoError(t, err)
		require.NotNil(t, r)
		require.True(t, fsInUse())
		r.Unref()
		require.False(t, fsInUse())

		// Keys() will call WalkKeys() which will can be blocked.
		keys := fs.Keys()
		require.NotNil(t, keys)
		require.Len(t, keys, 2)

		require.False(t, fsInUse())
	}

	checkBlocked := func() {
		t.Helper()

		require.False(t, fsInUse())

		applyCount := 0
		err := fs.Apply(context.Background(), func(r tsm1.TSMFile) error {
			applyCount++
			return nil
		})
		require.ErrorIs(t, err, tsm1.ErrNewReadersBlocked)
		require.Zero(t, applyCount, "Apply should not be called for any files when new readers are blocked")

		snap, err := fs.CreateSnapshot()
		require.ErrorIs(t, err, tsm1.ErrNewReadersBlocked)
		require.Empty(t, snap)

		buf := make([]tsm1.FloatValue, 1000)
		c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
		require.NotNil(t, c)
		defer c.Close()
		values, err := c.ReadFloatBlock(&buf)
		require.NoError(t, err)
		require.Empty(t, values)

		r, err := fs.TSMReader(files[0])
		require.ErrorIs(t, err, tsm1.ErrNewReadersBlocked)
		require.Nil(t, r)

		// Keys() will call WalkKeys() which will can be blocked.
		keys := fs.Keys()
		require.Nil(t, keys)

		require.False(t, fsInUse())
	}

	checkUnblocked()
	require.NoError(t, fs.SetNewReadersBlocked(true))
	checkBlocked()

	// nested block
	require.NoError(t, fs.SetNewReadersBlocked(true))
	checkBlocked()

	// still blocked
	require.NoError(t, fs.SetNewReadersBlocked(false))
	checkBlocked()

	// unblocked
	require.NoError(t, fs.SetNewReadersBlocked(false))
	checkUnblocked()

	// Too many unblocks, sir, too many.
	require.Error(t, fs.SetNewReadersBlocked(false))
	checkUnblocked()
}

type mockObserver struct {
	fileFinishing func(path string) error
	fileUnlinking func(path string) error
}

func (m mockObserver) FileFinishing(path string) error {
	return m.fileFinishing(path)
}

func (m mockObserver) FileUnlinking(path string) error {
	return m.fileUnlinking(path)
}

func TestFileStore_Observer(t *testing.T) {
	var finishes, unlinks []string
	m := mockObserver{
		fileFinishing: func(path string) error {
			finishes = append(finishes, path)
			return nil
		},
		fileUnlinking: func(path string) error {
			unlinks = append(unlinks, path)
			return nil
		},
	}

	check := func(results []string, expect ...string) {
		t.Helper()
		if len(results) != len(expect) {
			t.Fatalf("wrong number of results: %d results != %d expected", len(results), len(expect))
		}
		for i, ex := range expect {
			if got := filepath.Base(results[i]); got != ex {
				t.Fatalf("unexpected result: got %q != expected %q", got, ex)
			}
		}
	}

	dir := t.TempDir()
	fs := newTestFileStore(t, dir)
	fs.WithObserver(m)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0), tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(10, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(20, 3.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	if err := fs.Replace(nil, files); err != nil {
		t.Fatalf("error replacing: %v", err)
	}

	// Create a tombstone
	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 10, 10); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	// Check that we observed finishes correctly
	check(finishes,
		"000000001-000000001.tsm",
		"000000002-000000001.tsm",
		"000000003-000000001.tsm",
		"000000002-000000001.tombstone.tmp",
	)
	check(unlinks)
	unlinks, finishes = nil, nil

	// remove files including a tombstone
	if err := fs.Replace(files[1:3], nil); err != nil {
		t.Fatal("error replacing")
	}

	// Check that we observed unlinks correctly
	check(finishes)
	check(unlinks,
		"000000002-000000001.tsm",
		"000000002-000000001.tombstone",
		"000000003-000000001.tsm",
	)
	unlinks, finishes = nil, nil

	// add a tombstone for the first file multiple times.
	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 0, 0); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}
	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	check(finishes,
		"000000001-000000001.tombstone.tmp",
		"000000001-000000001.tombstone.tmp",
	)
	check(unlinks)
	unlinks, finishes = nil, nil
}

func newFileDir(tb testing.TB, dir string, values ...keyValues) ([]string, error) {
	var files []string

	id := 1
	for _, v := range values {
		f := MustTempFile(tb, dir)
		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			return nil, err
		}

		if err := w.Write([]byte(v.key), v.values); err != nil {
			return nil, err
		}

		if err := w.WriteIndex(); err != nil {
			return nil, err
		}

		if err := w.Close(); err != nil {
			return nil, err
		}
		newName := filepath.Join(filepath.Dir(f.Name()), tsm1.DefaultFormatFileName(id, 1)+".tsm")
		if err := os.Rename(f.Name(), newName); err != nil {
			return nil, err
		}
		id++

		files = append(files, newName)
	}
	return files, nil

}

func newFiles(tb testing.TB, dir string, values ...keyValues) ([]string, error) {
	var files []string

	id := 1
	for _, v := range values {
		f := MustTempFile(tb, dir)
		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			return nil, err
		}

		if err := w.Write([]byte(v.key), v.values); err != nil {
			return nil, err
		}

		if err := w.WriteIndex(); err != nil {
			return nil, err
		}

		if err := w.Close(); err != nil {
			return nil, err
		}

		newName := filepath.Join(filepath.Dir(f.Name()), tsm1.DefaultFormatFileName(id, 1)+".tsm")
		if err := os.Rename(f.Name(), newName); err != nil {
			return nil, err
		}
		id++

		files = append(files, newName)
	}
	return files, nil
}

type keyValues struct {
	key    string
	values []tsm1.Value
}

func MustTempFile(tb testing.TB, dir string) *os.File {
	f, err := os.CreateTemp(dir, "tsm1test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	tb.Cleanup(func() {
		f.Close()
	})

	return f
}

func fatal(t *testing.T, msg string, err error) {
	t.Fatalf("unexpected error %v: %v", msg, err)
}

var fsResult []tsm1.FileStat

func BenchmarkFileStore_Stats(b *testing.B) {
	dir := b.TempDir()

	// Create some TSM files...
	data := make([]keyValues, 0, 1000)
	for i := 0; i < 1000; i++ {
		data = append(data, keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}})
	}

	_, err := newFileDir(b, dir, data...)
	if err != nil {
		b.Fatalf("creating benchmark files %v", err)
	}

	fs := newTestFileStore(b, dir)
	fs.WithLogger(zaptest.NewLogger(b))

	if err := fs.Open(context.Background()); err != nil {
		b.Fatalf("opening file store %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fsResult = fs.Stats()
	}
}
