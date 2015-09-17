package pd1_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/pd1"
)

func TestWAL_TestWriteQueryOpen(t *testing.T) {
	w := NewWAL()
	defer w.Cleanup()

	var vals map[string]pd1.Values
	var fields map[string]*tsdb.MeasurementFields
	var series []*tsdb.SeriesCreate

	w.Index = &MockIndexWriter{
		fn: func(valuesByKey map[string]pd1.Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
			vals = valuesByKey
			fields = measurementFieldsToSave
			series = seriesToCreate
			return nil
		},
	}

	if err := w.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error)
	}

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.2 1000000000")
	p3 := parsePoint("cpu,host=A value=2.1 2000000000")
	p4 := parsePoint("cpu,host=B value=2.2 2000000000")
	fieldsToWrite := map[string]*tsdb.MeasurementFields{"foo": {Fields: map[string]*tsdb.Field{"bar": {Name: "value"}}}}
	seriesToWrite := []*tsdb.SeriesCreate{{Measurement: "asdf"}}

	if err := w.WritePoints([]tsdb.Point{p1, p2}, fieldsToWrite, seriesToWrite); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	c := w.Cursor("cpu,host=A", tsdb.Forward)
	k, v := c.Next()
	if btou64(k) != uint64(p1.UnixNano()) {
		t.Fatalf("p1 time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), btou64(k))
	}
	if 1.1 != btof64(v) {
		t.Fatal("p1 data not equal")
	}
	c = w.Cursor("cpu,host=B", tsdb.Forward)
	k, v = c.Next()
	if btou64(k) != uint64(p2.UnixNano()) {
		t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), btou64(k))
	}
	if 1.2 != btof64(v) {
		t.Fatal("p2 data not equal")
	}

	k, v = c.Next()
	if k != nil {
		t.Fatal("expected nil")
	}

	// ensure we can do another write to the wal and get stuff
	if err := w.WritePoints([]tsdb.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error)
	}

	c = w.Cursor("cpu,host=A", tsdb.Forward)
	k, v = c.Next()
	if btou64(k) != uint64(p1.UnixNano()) {
		t.Fatalf("p1 time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), btou64(k))
	}
	if 1.1 != btof64(v) {
		t.Fatal("p1 data not equal")
	}
	k, v = c.Next()
	if btou64(k) != uint64(p3.UnixNano()) {
		t.Fatalf("p3 time wrong:\n\texp:%d\n\tgot:%d\n", p3.UnixNano(), btou64(k))
	}
	if 2.1 != btof64(v) {
		t.Fatal("p3 data not equal")
	}

	// ensure we can seek
	k, v = c.Seek(u64tob(2000000000))
	if btou64(k) != uint64(p3.UnixNano()) {
		t.Fatalf("p3 time wrong:\n\texp:%d\n\tgot:%d\n", p3.UnixNano(), btou64(k))
	}
	if 2.1 != btof64(v) {
		t.Fatal("p3 data not equal")
	}
	k, v = c.Next()
	if k != nil {
		t.Fatal("expected nil")
	}

	// ensure we close and after open it flushes to the index
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close: %s", err.Error())
	}

	if err := w.Open(); err != nil {
		t.Fatalf("failed to open: %s", err.Error())
	}

	if len(vals["cpu,host=A#value"]) != 2 {
		t.Fatal("expected host A values to flush to index on open")
	}

	if len(vals["cpu,host=B#value"]) != 1 {
		t.Fatal("expected host B values to flush to index on open")
	}

	if err := w.WritePoints([]tsdb.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error)
	}
	c = w.Cursor("cpu,host=B", tsdb.Forward)
	k, v = c.Next()
	if btou64(k) != uint64(p4.UnixNano()) {
		t.Fatalf("p4 time wrong:\n\texp:%d\n\tgot:%d\n", p4.UnixNano(), btou64(k))
	}
	if 2.2 != btof64(v) {
		t.Fatal("p4 data not equal")
	}

	if !reflect.DeepEqual(fields, fieldsToWrite) {
		t.Fatal("fields not flushed")
	}

	if !reflect.DeepEqual(series, seriesToWrite) {
		t.Fatal("series not flushed")
	}
}

type Log struct {
	*pd1.Log
	path string
}

func NewWAL() *Log {
	dir, err := ioutil.TempDir("", "pd1-test")
	if err != nil {
		panic("couldn't get temp dir")
	}

	l := &Log{
		Log:  pd1.NewLog(dir),
		path: dir,
	}
	l.LoggingEnabled = true
	return l
}

func (l *Log) Cleanup() error {
	l.Close()
	os.RemoveAll(l.path)
	return nil
}

type MockIndexWriter struct {
	fn func(valuesByKey map[string]pd1.Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error
}

func (m *MockIndexWriter) WriteAndCompact(valuesByKey map[string]pd1.Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	return m.fn(valuesByKey, measurementFieldsToSave, seriesToCreate)
}
