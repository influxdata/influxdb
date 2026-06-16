package tsm1_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileStore_Read(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFiles(dir, data...)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 4.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 4.0), tsm1.NewValue(2, 7.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(0)), tsm1.NewValue(1, int64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(4)), tsm1.NewValue(2, int64(7))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(0)), tsm1.NewValue(1, uint64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(4)), tsm1.NewValue(2, uint64(7))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, true), tsm1.NewValue(1, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, false), tsm1.NewValue(2, true)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "zero"), tsm1.NewValue(1, "one")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "two")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, "three")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "four"), tsm1.NewValue(2, "seven")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0), tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0), tsm1.NewValue(4, 4.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.1)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.2)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, int64(1)), tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2)), tsm1.NewValue(4, int64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(0)), tsm1.NewValue(1, int64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(5))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, uint64(1)), tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2)), tsm1.NewValue(4, uint64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(0)), tsm1.NewValue(1, uint64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(5))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, true), tsm1.NewValue(3, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true), tsm1.NewValue(4, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, true), tsm1.NewValue(1, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, false)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, "1.0"), tsm1.NewValue(3, "3.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.0"), tsm1.NewValue(4, "4.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "0.0"), tsm1.NewValue(1, "1.1")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.2")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0),
			tsm1.NewValue(2, 2.0),
			tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(4, 4.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 3, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 2, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 4.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 2, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0), tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0), tsm1.NewValue(4, 4.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.1)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.2)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, int64(1)), tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2)), tsm1.NewValue(4, int64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(0)), tsm1.NewValue(1, int64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(5))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, uint64(1)), tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2)), tsm1.NewValue(4, uint64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(0)), tsm1.NewValue(1, uint64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(5))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, true), tsm1.NewValue(3, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true), tsm1.NewValue(4, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, true), tsm1.NewValue(1, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, false)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, "1.0"), tsm1.NewValue(3, "3.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.0"), tsm1.NewValue(4, "4.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "0.0"), tsm1.NewValue(1, "1.1")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.2")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 4, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 4 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, 0.0), tsm1.NewValue(9, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 4.0), tsm1.NewValue(7, 7.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 10, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, int64(0)), tsm1.NewValue(9, int64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, int64(4)), tsm1.NewValue(10, int64(7))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, uint64(0)), tsm1.NewValue(9, uint64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, uint64(4)), tsm1.NewValue(10, uint64(7))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, true), tsm1.NewValue(9, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, true), tsm1.NewValue(7, false)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, "eight"), tsm1.NewValue(9, "nine")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "two")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, "three")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, "four"), tsm1.NewValue(7, "seven")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(2, 2.0),
			tsm1.NewValue(3, 3.0),
			tsm1.NewValue(4, 4.0)},
		},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 3, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 2, false)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	// Delete part of the block in the first file.
	r := MustOpenTSMReader(files[0])
	r.DeleteRange([][]byte{[]byte("cpu")}, 1, 3)

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, 1.0),
			tsm1.NewValue(1, 2.0),
			tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, int64(1)),
			tsm1.NewValue(1, int64(2)),
			tsm1.NewValue(2, int64(3))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, uint64(1)),
			tsm1.NewValue(1, uint64(2)),
			tsm1.NewValue(2, uint64(3))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, "1"),
			tsm1.NewValue(1, "2"),
			tsm1.NewValue(2, "3")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, true),
			tsm1.NewValue(1, false),
			tsm1.NewValue(2, true)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	_, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := fs.CurrentGeneration(), 4; got != exp {
		t.Fatalf("current ID mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_OpenFail(t *testing.T) {
	var err error
	dir := MustTempDir()
	defer func() { assert.NoError(t, os.RemoveAll(dir), "failed to remove temporary directory") }()

	// Create a TSM file...
	data := keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}}

	files, err := newFileDir(dir, data)
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
	fs := tsm1.NewFileStore(dir, tsm1.TestMmapInitFailOption(initErr))
	err := fs.Open()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), fullErrMsg)
	defer func() { assert.NoError(t, fs.Close(), "unexpected error on FileStore.Close") }()
	assert.Equal(t, 0, fs.Count(), "file count mismatch")
}

func TestFileStore_Remove(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

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
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	// Replace requires assumes new files have a .tmp extension
	replacement := fmt.Sprintf("%s.%s", files[2], tsm1.TmpTSMFileExtension)
	os.Rename(files[2], replacement)

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

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
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu,host=server2!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	_, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := len(fs.Keys()), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if err := fs.Delete([][]byte{[]byte("cpu,host=server2!~#!value")}); err != nil {
		fatal(t, "deleting", err)
	}

	fs2 := tsm1.NewFileStore(dir)
	if err := fs2.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs2.Close()

	if got, exp := len(fs2.Keys()), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Delete(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu,host=server2!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFiles(dir, data...)
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu,host=server2#!~#value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu,host=server1#!~#value", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem,host=server1#!~#value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	keys := fs.Keys()
	if got, exp := len(keys), 3; got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	var n int64
	if err := fs.Apply(func(r tsm1.TSMFile) error {
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
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

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
	newFile := MustWriteTSM(dir, 4, map[string][]tsm1.Value{
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

	newFile = MustWriteTSM(dir, 5, map[string][]tsm1.Value{
		"mem": []tsm1.Value{tsm1.NewValue(0, 1.0)},
	})

	// Adding some files should invalidate the cache.
	fs.Replace(nil, []string{newFile})
	if got, exp := len(fs.Stats()), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_CreateSnapshot(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
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

func TestFileStore_ReaderBlocking(t *testing.T) {
	dir := t.TempDir()

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFileDir(dir, data...)
	require.NoError(t, err)

	fs := tsm1.NewFileStore(dir)
	require.NoError(t, fs.Open())
	defer func() {
		t.Helper()
		require.NoError(t, fs.Close())
	}()

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
		_, err := fs.InUse()

		var applyCount atomic.Uint32
		err = fs.Apply(func(r tsm1.TSMFile) error {
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
		err := fs.Apply(func(r tsm1.TSMFile) error {
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

	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)
	fs.WithObserver(m)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0), tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(10, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(20, 3.0)}},
	}

	files, err := newFiles(dir, data...)
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

func newFileDir(dir string, values ...keyValues) ([]string, error) {
	var files []string

	id := 1
	for _, v := range values {
		f := MustTempFile(dir)
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

func newFiles(dir string, values ...keyValues) ([]string, error) {
	return newFilesId(dir, 1, values...)
}

// newFilesId allows specifying a starting file id so that multiple calls into the
// same directory do not collide on file names.
func newFilesId(dir string, id int, values ...keyValues) ([]string, error) {
	var files []string

	for _, v := range values {
		f := MustTempFile(dir)
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

func MustTempDir() string {
	dir, err := os.MkdirTemp("", "tsm1-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}

func MustTempFile(dir string) *os.File {
	f, err := os.CreateTemp(dir, "tsm1test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	return f
}

func fatal(t *testing.T, msg string, err error) {
	t.Fatalf("unexpected error %v: %v", msg, err)
}

var fsResult []tsm1.ExtFileStat

func BenchmarkFileStore_Stats(b *testing.B) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create some TSM files...
	data := make([]keyValues, 0, 1000)
	for i := 0; i < 1000; i++ {
		data = append(data, keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}})
	}

	_, err := newFileDir(dir, data...)
	if err != nil {
		b.Fatalf("creating benchmark files %v", err)
	}

	fs := tsm1.NewFileStore(dir)
	if testing.Verbose() {
		fs.WithLogger(logger.New(os.Stderr))
	}

	if err := fs.Open(); err != nil {
		b.Fatalf("opening file store %v", err)
	}
	defer fs.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fsResult = fs.Stats()
	}
}

// applyHoldTime is how long the Apply callback holds the read lock, to stall
// write-lock acquisition and create the contended environment.
const applyHoldTime = 100 * time.Microsecond

// BenchmarkFileStore_FastLaneContention measures each fast-lane reader's exposure
// to FileStore lock contention. Background goroutines create the contention for the
// whole sub-benchmark: one holds the read lock for a while via Apply, another stalls
// on the write lock via Replace. The Replace writer churns a separate "mem" key (it
// never removes the "cpu" file the victim reads) and threads oldFiles so the live
// file set stays bounded. The victim fast-lane reader is then measured across b.N
// operations. Splitting mu into fastMu/slowMu keeps these readers off the slow drain
// path, so their latency should drop substantially compared to a single shared mutex.
func BenchmarkFileStore_FastLaneContention(b *testing.B) {
	// fn takes the FileStore explicitly because a fresh one is built per sub-benchmark.
	victims := []struct {
		name string
		fn   func(fs *tsm1.FileStore)
	}{
		{"KeyCursor", func(fs *tsm1.FileStore) { fs.KeyCursor(context.Background(), []byte("cpu"), 0, true).Close() }},
		{"Count", func(fs *tsm1.FileStore) { _ = fs.Count() }},
		{"Files", func(fs *tsm1.FileStore) { _ = fs.Files() }},
		{"CurrentGeneration", func(fs *tsm1.FileStore) { _ = fs.CurrentGeneration() }},
		{"LastModified", func(fs *tsm1.FileStore) { _ = fs.LastModified() }},
	}

	for _, v := range victims {
		b.Run(v.name, func(b *testing.B) {
			// Fresh FileStore per sub-benchmark so churned files do not accumulate
			// across runs.
			dir := MustTempDir()
			data := []keyValues{{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}}}
			_, err := newFileDir(dir, data...)
			require.NoError(b, err)

			fs := tsm1.NewFileStore(dir)
			if testing.Verbose() {
				fs.WithLogger(logger.New(os.Stderr))
			}
			require.NoError(b, fs.Open())

			// Throwaway files for the Replace writer to churn.
			churn := MustTempDir()

			// Start the contended environment once and run it for the whole
			// sub-benchmark; only the victim loop below is timed.
			var wg sync.WaitGroup
			done := make(chan struct{})

			// Apply - hold the read lock for a while to slow down write-lock
			// acquisition. Not measured; it just creates the contended environment.
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
						assert.NoError(b, fs.Apply(func(r tsm1.TSMFile) error {
							// Hold the read lock a while.
							time.Sleep(applyHoldTime)
							return nil
						}))
					}
				}
			}()

			// Replace - request the write lock but be stalled by Apply. Churns a
			// "mem" key (never "cpu") and threads oldFiles so the file set stays
			// bounded. Not measured; it just creates the contended environment.
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := 1
				var oldFiles []string
				for {
					select {
					case <-done:
						return
					default:
						churnData := []keyValues{
							{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0), tsm1.NewValue(1, 2.0)}},
						}
						id++
						files, err := newFilesId(churn, id, churnData...)
						assert.NoError(b, err)
						assert.NoError(b, fs.Replace(oldFiles, files))
						oldFiles = files
					}
				}
			}()

			// Measure the victim across b.N operations under contention.
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v.fn(fs)
			}
			b.StopTimer()

			// Stop and join the contention goroutines, then close the FileStore
			// before deleting its backing directories (owner before storage).
			close(done)
			wg.Wait()
			assert.NoError(b, fs.Close())
			assert.NoError(b, os.RemoveAll(churn))
			assert.NoError(b, os.RemoveAll(dir))
		})
	}
}

// TestFileStore_ConcurrentLocking exercises the fastMu/slowMu split under a heavy
// concurrent mix of fast-lane readers (KeyCursor/Count/Files/CurrentGeneration/
// LastModified), slow-lane readers (Read/Keys/WalkKeys/Apply), and writers
// (NextGeneration via wlock, plus a file-mutating Replace loop). Run with -race to
// catch data races and any lock-ordering deadlock. The "cpu" file is never removed,
// so reads of it must always observe the original value.
func TestFileStore_ConcurrentLocking(t *testing.T) {
	const (
		numReaders = 16
		iters      = 200
	)

	// Register both temp-dir cleanups BEFORE creating the FileStore so that the
	// deferred fs.Close() (registered last) runs FIRST under LIFO ordering: the
	// store is closed before its backing directories are removed. The Replace
	// churn can hand still-in-use files to the async purger, so closing the owner
	// before deleting its storage keeps teardown ordered.
	dir := MustTempDir()
	defer func() { assert.NoError(t, os.RemoveAll(dir)) }()

	// Throwaway files for the Replace writer to churn.
	churnDir := MustTempDir()
	defer func() { assert.NoError(t, os.RemoveAll(churnDir)) }()

	// Permanent data file the readers depend on; never passed as oldFiles to Replace.
	files, err := newFiles(dir, keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}})
	require.NoError(t, err)

	fs := tsm1.NewFileStore(dir)
	require.NoError(t, fs.Replace(nil, files))
	defer func() { require.NoError(t, fs.Close()) }()

	var concurrency, maxConcurrency atomic.Int64
	bump := func() {
		c := concurrency.Add(1)
		for {
			old := maxConcurrency.Load()
			if c <= old || maxConcurrency.CompareAndSwap(old, c) {
				break
			}
		}
	}

	// Each op exercises one lock path AND asserts its result, so the test catches
	// not just races/deadlocks but also wrong values handed back under contention
	// (e.g. a torn read of the files slice). The "cpu" key is stable, so reads of it
	// are exact; the Replace writer churns a single "mem" file, so the file set is
	// always {cpu} or {cpu, mem} and the generation counter is bounded by iters.
	// Use assert (not require) since these run in spawned goroutines, where require's
	// FailNow is unsafe.
	ops := []func(){
		func() {
			c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
			defer c.Close()
			var buf []tsm1.FloatValue
			values, err := c.ReadFloatBlock(&buf)
			if assert.NoError(t, err) && assert.Len(t, values, 1) {
				assert.Equal(t, int64(0), values[0].UnixNano())
				assert.Equal(t, 1.0, values[0].Value())
			}
		},
		func() {
			c := fs.Count()
			assert.GreaterOrEqual(t, c, 1) // cpu is always present
			assert.LessOrEqual(t, c, 2)    // cpu + at most one churned mem file
		},
		func() {
			fl := fs.Files()
			assert.GreaterOrEqual(t, len(fl), 1)
			assert.LessOrEqual(t, len(fl), 2)
			for _, f := range fl {
				assert.NotNil(t, f)
			}
		},
		func() {
			g := fs.CurrentGeneration()
			assert.GreaterOrEqual(t, g, 0)  // never set via Open; bumped by NextGeneration
			assert.LessOrEqual(t, g, iters) // at most one increment per NextGeneration op call
		},
		func() { assert.False(t, fs.LastModified().IsZero()) }, // set when cpu was loaded
		func() {
			v, err := fs.Read([]byte("cpu"), 0)
			if assert.NoError(t, err) && assert.Len(t, v, 1) {
				assert.Equal(t, 1.0, v[0].Value())
			}
		},
		func() {
			keys := fs.Keys()
			if typ, ok := keys["cpu"]; assert.True(t, ok) {
				assert.Equal(t, byte(tsm1.BlockFloat64), typ)
			}
		},
		func() {
			var sawCPU bool
			err := fs.WalkKeys(nil, func(key []byte, typ byte) error {
				if string(key) == "cpu" {
					sawCPU = true
					assert.Equal(t, byte(tsm1.BlockFloat64), typ)
				}
				return nil
			})
			if assert.NoError(t, err) {
				assert.True(t, sawCPU)
			}
		},
		func() {
			// Apply runs fn in parallel goroutines, so count invocations atomically.
			var applied atomic.Int64
			err := fs.Apply(func(r tsm1.TSMFile) error {
				applied.Add(1)
				return nil
			})
			if assert.NoError(t, err) {
				assert.GreaterOrEqual(t, applied.Load(), int64(1)) // cpu at least
			}
		},
		func() {
			g := fs.NextGeneration() // scalar writer through wlock
			assert.GreaterOrEqual(t, g, 1)
			assert.LessOrEqual(t, g, iters)
		},
	}

	// Gate all goroutines so they start as simultaneously as possible.
	var startGate sync.RWMutex
	startGate.Lock()

	var wg sync.WaitGroup
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		op := ops[i%len(ops)]
		go func() {
			startGate.RLock()
			defer startGate.RUnlock()
			defer wg.Done()
			for j := 0; j < iters; j++ {
				bump()
				op()
				concurrency.Add(-1)
			}
		}()
	}

	// A single dedicated file-mutating writer. Kept to one goroutine so the file
	// lifecycle (create, then hand the previous set to Replace as oldFiles) stays
	// well-defined; writer-vs-writer ordering is still exercised against the
	// NextGeneration op above.
	wg.Add(1)
	go func() {
		startGate.RLock()
		defer startGate.RUnlock()
		defer wg.Done()
		var oldFiles []string
		for j := 0; j < iters; j++ {
			bump()
			nf, err := newFilesId(churnDir, j+2, keyValues{"mem", []tsm1.Value{tsm1.NewValue(int64(j), 2.0)}})
			if assert.NoError(t, err) {
				assert.NoError(t, fs.Replace(oldFiles, nf))
				oldFiles = nf
			}
			concurrency.Add(-1)
		}
	}()

	startGate.Unlock() // release everyone at once
	wg.Wait()
	t.Logf("max concurrency: %d", maxConcurrency.Load())
}
