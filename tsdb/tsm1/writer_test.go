package tsm1_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb/tsm1"
)

func TestTSMWriter_Write_Empty(t *testing.T) {
	var b bytes.Buffer
	w, err := tsm1.NewTSMWriter(&b)
	if err != nil {
		t.Fatalf("unexpected error created writer: %v", err)
	}

	if err := w.WriteIndex(); err != tsm1.ErrNoValues {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if got, exp := len(b.Bytes()), 0; got < exp {
		t.Fatalf("file size mismatch: got %v, exp %v", got, exp)
	}
}

func TestTSMWriter_Write_NoValues(t *testing.T) {
	var b bytes.Buffer
	w, err := tsm1.NewTSMWriter(&b)
	if err != nil {
		t.Fatalf("unexpected error created writer: %v", err)
	}

	if err := w.Write([]byte("foo"), []tsm1.Value{}); err != nil {
		t.Fatalf("unexpected error writing: %v", err)
	}

	if err := w.WriteIndex(); err != tsm1.ErrNoValues {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if got, exp := len(b.Bytes()), 0; got < exp {
		t.Fatalf("file size mismatch: got %v, exp %v", got, exp)
	}
}

func TestTSMWriter_Write_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	values := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	if err := w.Write([]byte("cpu"), values); err != nil {
		t.Fatalf("unexpected error writing: %v", err)

	}
	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error writing index: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error open file: %v", err)
	}

	b, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatalf("unexpected error reading: %v", err)
	}

	if got, exp := len(b), 5; got < exp {
		t.Fatalf("file size mismatch: got %v, exp %v", got, exp)
	}
	if got := binary.BigEndian.Uint32(b[0:4]); got != tsm1.MagicNumber {
		t.Fatalf("magic number mismatch: got %v, exp %v", got, tsm1.MagicNumber)
	}

	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("unexpected error seeking: %v", err)
	}

	r, err := tsm1.NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}
	defer r.Close()

	readValues, err := r.ReadAll([]byte("cpu"))
	if err != nil {
		t.Fatalf("unexpected error readin: %v", err)
	}

	if len(readValues) != len(values) {
		t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), len(values))
	}

	for i, v := range values {
		if v.Value() != readValues[i].Value() {
			t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
		}
	}
}

func TestTSMWriter_Write_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		{"mem", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
	}

	for _, d := range data {
		if err := w.Write([]byte(d.key), d.values); err != nil {
			t.Fatalf("unexpected error writing: %v", err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error open file: %v", err)
	}

	r, err := tsm1.NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}
	defer r.Close()

	for _, d := range data {
		readValues, err := r.ReadAll([]byte(d.key))
		if err != nil {
			t.Fatalf("unexpected error readin: %v", err)
		}

		if exp := len(d.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range d.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}
	}
}

func TestTSMWriter_Write_MultipleKeyValues(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu", []tsm1.Value{
			tsm1.NewValue(0, 1.0),
			tsm1.NewValue(1, 2.0)},
		},
		{"mem", []tsm1.Value{
			tsm1.NewValue(0, 1.5),
			tsm1.NewValue(1, 2.5)},
		},
	}

	for _, d := range data {
		if err := w.Write([]byte(d.key), d.values); err != nil {
			t.Fatalf("unexpected error writing: %v", err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error open file: %v", err)
	}

	r, err := tsm1.NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}
	defer r.Close()

	for _, d := range data {
		readValues, err := r.ReadAll([]byte(d.key))
		if err != nil {
			t.Fatalf("unexpected error readin: %v", err)
		}

		if exp := len(d.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range d.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}
	}
}

// Tests that writing keys in reverse is able to read them back.
func TestTSMWriter_Write_SameKey(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu", []tsm1.Value{
			tsm1.NewValue(0, 1.0),
			tsm1.NewValue(1, 2.0)},
		},
		{"cpu", []tsm1.Value{
			tsm1.NewValue(2, 3.0),
			tsm1.NewValue(3, 4.0)},
		},
	}

	for _, d := range data {
		if err := w.Write([]byte(d.key), d.values); err != nil {
			t.Fatalf("unexpected error writing: %v", err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error open file: %v", err)
	}

	r, err := tsm1.NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}
	defer r.Close()

	values := append(data[0].values, data[1].values...)

	readValues, err := r.ReadAll([]byte("cpu"))
	if err != nil {
		t.Fatalf("unexpected error readin: %v", err)
	}

	if exp := len(values); exp != len(readValues) {
		t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
	}

	for i, v := range values {
		if v.Value() != readValues[i].Value() {
			t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
		}
	}
}

// Tests that calling Read returns all the values for block matching the key
// and timestamp
func TestTSMWriter_Read_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu", []tsm1.Value{
			tsm1.NewValue(0, 1.0),
			tsm1.NewValue(1, 2.0)},
		},
		{"cpu", []tsm1.Value{
			tsm1.NewValue(2, 3.0),
			tsm1.NewValue(3, 4.0)},
		},
	}

	for _, d := range data {
		if err := w.Write([]byte(d.key), d.values); err != nil {
			t.Fatalf("unexpected error writing: %v", err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error open file: %v", err)
	}

	r, err := tsm1.NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}
	defer r.Close()

	for _, values := range data {
		// Try the first timestamp
		readValues, err := r.Read([]byte("cpu"), values.values[0].UnixNano())
		if err != nil {
			t.Fatalf("unexpected error readin: %v", err)
		}

		if exp := len(values.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range values.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}

		// Try the last timestamp too
		readValues, err = r.Read([]byte("cpu"), values.values[1].UnixNano())
		if err != nil {
			t.Fatalf("unexpected error readin: %v", err)
		}

		if exp := len(values.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range values.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}
	}
}

func TestTSMWriter_WriteBlock_Empty(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	if err := w.WriteBlock([]byte("cpu"), 0, 0, nil); err != nil {
		t.Fatalf("unexpected error writing block: %v", err)
	}

	if err := w.WriteIndex(); err != tsm1.ErrNoValues {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error open file: %v", err)
	}
	defer fd.Close()

	b, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatalf("unexpected error read all: %v", err)
	}

	if got, exp := len(b), 0; got < exp {
		t.Fatalf("file size mismatch: got %v, exp %v", got, exp)
	}
}

func TestTSMWriter_WriteBlock_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		{"mem", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
	}

	for _, d := range data {
		if err := w.Write([]byte(d.key), d.values); err != nil {
			t.Fatalf("unexpected error writing: %v", err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error open file: %v", err)
	}
	defer fd.Close()

	b, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatalf("unexpected error read all: %v", err)
	}

	if got, exp := len(b), 5; got < exp {
		t.Fatalf("file size mismatch: got %v, exp %v", got, exp)
	}
	if got := binary.BigEndian.Uint32(b[0:4]); got != tsm1.MagicNumber {
		t.Fatalf("magic number mismatch: got %v, exp %v", got, tsm1.MagicNumber)
	}

	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("error seeking: %v", err)
	}

	// Create reader for that file
	r, err := tsm1.NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	f = MustTempFile(dir)
	w, err = tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	iter := r.BlockIterator()
	for iter.Next() {
		key, minTime, maxTime, _, _, b, err := iter.Read()
		if err != nil {
			t.Fatalf("unexpected error reading block: %v", err)
		}
		if err := w.WriteBlock([]byte(key), minTime, maxTime, b); err != nil {
			t.Fatalf("unexpected error writing block: %v", err)
		}
	}
	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fd, err = os.Open(f.Name())
	if err != nil {
		t.Fatalf("unexpected error open file: %v", err)
	}

	// Now create a reader to verify the written blocks matches the originally
	// written file using Write
	r, err = tsm1.NewTSMReader(fd)
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}
	defer r.Close()

	for _, d := range data {
		readValues, err := r.ReadAll([]byte(d.key))
		if err != nil {
			t.Fatalf("unexpected error readin: %v", err)
		}

		if exp := len(d.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range d.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}
	}
}

func TestTSMWriter_WriteBlock_MaxKey(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	key := bytes.Repeat([]byte("a"), 100000)
	if err := w.WriteBlock(key, 0, 0, nil); err != tsm1.ErrMaxKeyLengthExceeded {
		t.Fatalf("expected max key length error writing key: %v", err)
	}
}

func TestTSMWriter_Write_MaxKey(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	defer f.Close()

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error created writer: %v", err)
	}

	key := bytes.Repeat([]byte("a"), 100000)
	if err := w.Write(key, []tsm1.Value{tsm1.NewValue(0, 1.0)}); err != tsm1.ErrMaxKeyLengthExceeded {
		t.Fatalf("expected max key length error writing key: %v", err)
	}
}

// Ensures that a writer will properly compute stats for multiple measurements.
func TestTSMWriter_Write_MultipleMeasurements(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Write file with multiple measurements.
	f1 := MustWriteTSM(dir, 1, map[string][]tsm1.Value{
		"cpu,host=A#!~#value":  {tsm1.NewValue(1, 1.1), tsm1.NewValue(2, 1.2)},
		"cpu,host=B#!~#value":  {tsm1.NewValue(1, 1.1)},
		"mem,host=A#!~#value":  {tsm1.NewValue(1, 1.1), tsm1.NewValue(2, 1.2)},
		"disk,host=A#!~#value": {tsm1.NewValue(1, 1.1)},
	})

	stats := tsm1.NewMeasurementStats()
	if f, err := os.Open(tsm1.StatsFilename(f1)); err != nil {
		t.Fatal(err)
	} else if _, err := stats.ReadFrom(bufio.NewReader(f)); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(stats, tsm1.MeasurementStats{
		"cpu":  78,
		"mem":  44,
		"disk": 34,
	}); diff != "" {
		t.Fatal(diff)
	}
}

type fakeSyncer bool

func (f *fakeSyncer) Sync() error {
	*f = true
	return nil
}

func TestTSMWriter_Sync(t *testing.T) {
	f := &struct {
		io.Writer
		fakeSyncer
	}{
		Writer: ioutil.Discard,
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	values := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	if err := w.Write([]byte("cpu"), values); err != nil {
		t.Fatalf("unexpected error writing: %v", err)

	}
	if err := w.WriteIndex(); err != nil {
		t.Fatalf("unexpected error writing index: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	if !f.fakeSyncer {
		t.Fatal("failed to sync")
	}
}
