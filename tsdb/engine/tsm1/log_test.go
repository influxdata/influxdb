package tsm1_test

import (
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func TestLog_TestWriteQueryOpen(t *testing.T) {
	w := NewLog()
	defer w.Close()

	// Mock call to the index.
	var vals map[string]tsm1.Values
	var fields map[string]*tsdb.MeasurementFields
	var series []*tsdb.SeriesCreate
	w.IndexWriter.WriteFn = func(valuesByKey map[string]tsm1.Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
		vals = valuesByKey
		fields = measurementFieldsToSave
		series = seriesToCreate
		return nil
	}

	if err := w.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.2 1000000000")
	p3 := parsePoint("cpu,host=A value=2.1 2000000000")
	p4 := parsePoint("cpu,host=B value=2.2 2000000000")
	fieldsToWrite := map[string]*tsdb.MeasurementFields{"foo": {Fields: map[string]*tsdb.Field{"bar": {Name: "value"}}}}
	seriesToWrite := []*tsdb.SeriesCreate{{Measurement: "asdf"}}

	if err := w.WritePoints([]models.Point{p1, p2}, fieldsToWrite, seriesToWrite); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	fieldNames := []string{"value"}
	var codec *tsdb.FieldCodec

	c := w.Cursor("cpu,host=A", fieldNames, codec, true)
	k, v := c.Next()
	if k != p1.UnixNano() {
		t.Fatalf("p1 time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), k)
	}
	if 1.1 != v {
		t.Fatal("p1 data not equal")
	}
	c = w.Cursor("cpu,host=B", fieldNames, codec, true)
	k, v = c.Next()
	if k != p2.UnixNano() {
		t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), k)
	}
	if 1.2 != v {
		t.Fatal("p2 data not equal")
	}

	k, v = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF", k, v)
	}

	// ensure we can do another write to the wal and get stuff
	if err := w.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	c = w.Cursor("cpu,host=A", fieldNames, codec, true)
	k, v = c.Next()
	if k != p1.UnixNano() {
		t.Fatalf("p1 time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), k)
	}
	if 1.1 != v {
		t.Fatal("p1 data not equal")
	}
	k, v = c.Next()
	if k != p3.UnixNano() {
		t.Fatalf("p3 time wrong:\n\texp:%d\n\tgot:%d\n", p3.UnixNano(), k)
	}
	if 2.1 != v {
		t.Fatal("p3 data not equal")
	}

	// ensure we can seek
	k, v = c.SeekTo(2000000000)
	if k != p3.UnixNano() {
		t.Fatalf("p3 time wrong:\n\texp:%d\n\tgot:%d\n", p3.UnixNano(), k)
	}
	if 2.1 != v {
		t.Fatal("p3 data not equal")
	}
	k, v = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}

	// ensure we close and after open it flushes to the index
	if err := w.Log.Close(); err != nil {
		t.Fatalf("failed to close: %s", err.Error())
	}

	if err := w.Open(); err != nil {
		t.Fatalf("failed to open: %s", err.Error())
	}

	if len(vals[tsm1.SeriesFieldKey("cpu,host=A", "value")]) != 2 {
		t.Fatal("expected host A values to flush to index on open")
	}

	if len(vals[tsm1.SeriesFieldKey("cpu,host=B", "value")]) != 1 {
		t.Fatal("expected host B values to flush to index on open")
	}

	if err := w.WritePoints([]models.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}
	c = w.Cursor("cpu,host=B", fieldNames, codec, true)
	k, v = c.Next()
	if k != p4.UnixNano() {
		t.Fatalf("p4 time wrong:\n\texp:%d\n\tgot:%d\n", p4.UnixNano(), k)
	}
	if 2.2 != v {
		t.Fatal("p4 data not equal")
	}

	if !reflect.DeepEqual(fields, fieldsToWrite) {
		t.Fatal("fields not flushed")
	}

	if !reflect.DeepEqual(series, seriesToWrite) {
		t.Fatal("series not flushed")
	}
}

// Tests that concurrent flushes and writes do not trigger race conditions
func TestLog_WritePoints_FlushConcurrent(t *testing.T) {
	w := NewLog()
	defer w.Close()
	w.FlushMemorySizeThreshold = 1000
	total := 1000

	w.IndexWriter.WriteFn = func(valuesByKey map[string]tsm1.Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
		return nil
	}

	if err := w.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}

			// Force an idle flush
			if err := w.Flush(); err != nil {
				t.Fatalf("failed to run full compaction: %v", err)
			}
			// Allow some time so memory flush can occur due to writes
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			if i > total {
				return
			}
			select {
			case <-done:
				return
			default:
			}

			pt := models.MustNewPoint("cpu",
				map[string]string{"host": "A"},
				map[string]interface{}{"value": i},
				time.Unix(int64(i), 0),
			)

			if err := w.WritePoints([]models.Point{pt}, nil, nil); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			i++
		}
	}()

	// Let the goroutines run for a second
	select {
	case <-time.After(1 * time.Second):
		close(done)
	}

	// Wait for them to exit
	wg.Wait()
}

// Tests that concurrent writes when the WAL closes do not cause race conditions.
func TestLog_WritePoints_CloseConcurrent(t *testing.T) {
	w := NewLog()
	defer w.Close()
	w.FlushMemorySizeThreshold = 1000
	total := 1000

	w.IndexWriter.WriteFn = func(valuesByKey map[string]tsm1.Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
		return nil
	}

	if err := w.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			if i > total {
				return
			}
			select {
			case <-done:
				return
			default:
			}

			pt := models.MustNewPoint("cpu",
				map[string]string{"host": "A"},
				map[string]interface{}{"value": i},
				time.Unix(int64(i), 0),
			)

			if err := w.WritePoints([]models.Point{pt}, nil, nil); err != nil && err != tsm1.ErrWALClosed {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			i++
		}
	}()

	time.Sleep(10 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// Let the goroutines run for a second
	select {
	case <-time.After(1 * time.Second):
		close(done)
	}

	// Wait for them to exit
	wg.Wait()
}

// Ensure the log can handle random data.
func TestLog_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	quick.Check(func(pointsSlice PointsSlice) bool {
		l := NewLog()
		l.FlushMemorySizeThreshold = 4096 // low threshold
		defer l.Close()

		var mu sync.Mutex
		index := make(map[string]tsm1.Values)

		// Ignore flush to the index.
		l.IndexWriter.WriteFn = func(valuesByKey map[string]tsm1.Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
			mu.Lock()
			defer mu.Unlock()
			for key, values := range valuesByKey {
				index[key] = append(index[key], values...)
			}

			// Simulate slow index writes.
			time.Sleep(100 * time.Millisecond)

			return nil
		}

		// Open the log.
		if err := l.Open(); err != nil {
			t.Fatal(err)
		}

		// Generate fields and series to create.
		fieldsToWrite := pointsSlice.MeasurementFields()
		seriesToWrite := pointsSlice.SeriesCreate()

		// Write each set of points separately.
		for _, points := range pointsSlice {
			if err := l.WritePoints(points.Encode(), fieldsToWrite, seriesToWrite); err != nil {
				t.Fatal(err)
			}
		}

		// Iterate over each series and read out cursor.
		for _, series := range pointsSlice.Series() {
			mu.Lock()
			if got := mergeIndexCursor(series, l, index); !reflect.DeepEqual(got, series.Values) {
				t.Fatalf("mismatch:\n\ngot=%v\n\nexp=%v\n\n", len(got), len(series.Values))
			}
			mu.Unlock()
		}

		// Reopen log.
		if err := l.Reopen(); err != nil {
			t.Fatal(err)
		}

		// Iterate over each series and read out cursor again.
		for _, series := range pointsSlice.Series() {
			mu.Lock()
			if got := mergeIndexCursor(series, l, index); !reflect.DeepEqual(got, series.Values) {
				t.Fatalf("mismatch(reopen):\n\ngot=%v\n\nexp=%v\n\n", len(got), len(series.Values))
			}
			mu.Unlock()
		}

		return true
	}, &quick.Config{
		MaxCount: 10,
		Values: func(values []reflect.Value, rand *rand.Rand) {
			values[0] = reflect.ValueOf(GeneratePointsSlice(rand))
		},
	})
}

func mergeIndexCursor(series *Series, l *Log, index map[string]tsm1.Values) tsm1.Values {
	c := l.Cursor(series.Name, series.FieldsSlice(), &tsdb.FieldCodec{}, true)
	a := ReadAllCursor(c)
	a = append(index[series.Name+"#!~#value"], a...)
	a = DedupeValues(a)
	sort.Sort(a)
	return a
}

type Log struct {
	*tsm1.Log
	IndexWriter IndexWriter
}

// NewLog returns a new instance of Log
func NewLog() *Log {
	path, err := ioutil.TempDir("", "tsm1-test")
	if err != nil {
		panic(err)
	}

	l := &Log{Log: tsm1.NewLog(path)}
	l.Log.IndexWriter = &l.IndexWriter
	l.LoggingEnabled = true

	return l
}

// Close closes the log and removes the underlying temporary path.
func (l *Log) Close() error {
	defer os.RemoveAll(l.Path())
	return l.Log.Close()
}

// Reopen closes and reopens the log.
func (l *Log) Reopen() error {
	if err := l.Log.Close(); err != nil {
		return err
	}
	if err := l.Log.Open(); err != nil {
		return err
	}
	return nil
}

// IndexWriter represents a mock implementation of tsm1.IndexWriter.
type IndexWriter struct {
	WriteFn                 func(valuesByKey map[string]tsm1.Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error
	MarkDeletesFn           func(keys []string)
	MarkMeasurementDeleteFn func(name string)
}

func (w *IndexWriter) Write(valuesByKey map[string]tsm1.Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	return w.WriteFn(valuesByKey, measurementFieldsToSave, seriesToCreate)
}

func (w *IndexWriter) MarkDeletes(keys []string) {
	w.MarkDeletesFn(keys)
}

func (w *IndexWriter) MarkMeasurementDelete(name string) {
	w.MarkMeasurementDeleteFn(name)
}

// PointsSlice represents a slice of point slices.
type PointsSlice []Points

// GeneratePointsSlice randomly generates a slice of slice of points.
func GeneratePointsSlice(rand *rand.Rand) PointsSlice {
	var pointsSlice PointsSlice
	for i, pointsN := 0, rand.Intn(100); i < pointsN; i++ {
		var points Points
		for j, pointN := 0, rand.Intn(1000); j < pointN; j++ {
			points = append(points, Point{
				Name:   strconv.Itoa(rand.Intn(10)),
				Fields: models.Fields{"value": rand.Int63n(100000)},
				Time:   time.Unix(0, rand.Int63n(int64(24*time.Hour))).UTC(),
			})
		}
		pointsSlice = append(pointsSlice, points)
	}
	return pointsSlice
}

// MeasurementFields returns a set of fields used across all points.
func (a PointsSlice) MeasurementFields() map[string]*tsdb.MeasurementFields {
	mfs := map[string]*tsdb.MeasurementFields{}
	for _, points := range a {
		for _, p := range points {
			pp := p.Encode()

			// Create measurement field, if not exists.
			mf := mfs[string(pp.Key())]
			if mf == nil {
				mf = &tsdb.MeasurementFields{Fields: make(map[string]*tsdb.Field)}
				mfs[string(pp.Key())] = mf
			}

			// Add all fields on the point.
			for name, value := range p.Fields {
				mf.CreateFieldIfNotExists(name, influxql.InspectDataType(value), false)
			}
		}
	}
	return mfs
}

// SeriesCreate returns a list of series to create across all points.
func (a PointsSlice) SeriesCreate() []*tsdb.SeriesCreate {
	// Create unique set of series.
	m := map[string]*tsdb.SeriesCreate{}
	for _, points := range a {
		for _, p := range points {
			if pp := p.Encode(); m[string(pp.Key())] == nil {
				m[string(pp.Key())] = &tsdb.SeriesCreate{Measurement: pp.Name(), Series: tsdb.NewSeries(string(string(pp.Key())), pp.Tags())}
			}
		}
	}

	// Convert to slice.
	slice := make([]*tsdb.SeriesCreate, 0, len(m))
	for _, v := range m {
		slice = append(slice, v)
	}
	return slice
}

// Series returns a set of per-series data.
func (a PointsSlice) Series() map[string]*Series {
	m := map[string]*Series{}
	for _, points := range a {
		for _, p := range points {
			pp := p.Encode()

			// Create series if not exists.
			s := m[string(pp.Key())]
			if s == nil {
				s = &Series{
					Name:   string(pp.Key()),
					Fields: make(map[string]struct{}),
				}
				m[string(pp.Key())] = s
			}

			// Append point data.
			s.Values = append(s.Values, tsm1.NewValue(p.Time, p.Fields["value"]))

			// Add fields.
			for k := range p.Fields {
				s.Fields[k] = struct{}{}
			}
		}
	}

	// Deduplicate & sort items in each series.
	for _, s := range m {
		s.Values = DedupeValues(s.Values)
		sort.Sort(s.Values)
	}

	return m
}

// Points represents a slice of points.
type Points []Point

func (a Points) Encode() []models.Point {
	other := make([]models.Point, len(a))
	for i := range a {
		other[i] = a[i].Encode()
	}
	return other
}

// Point represents a test point
type Point struct {
	Name   string
	Tags   models.Tags
	Fields models.Fields
	Time   time.Time
}

func (p *Point) Encode() models.Point { return models.MustNewPoint(p.Name, p.Tags, p.Fields, p.Time) }

type Series struct {
	Name   string
	Fields map[string]struct{}
	Values tsm1.Values
}

// FieldsSlice returns a list of field names.
func (s *Series) FieldsSlice() []string {
	a := make([]string, 0, len(s.Fields))
	for k := range s.Fields {
		a = append(a, k)
	}
	return a
}
