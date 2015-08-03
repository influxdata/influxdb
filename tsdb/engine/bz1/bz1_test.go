package bz1_test

import (
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/bz1"
)

// Ensure the engine can write series metadata and reload it.
func TestEngine_LoadMetadataIndex_Series(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Setup nop mock.
	e.PointsWriter.WritePointsFn = func(a []tsdb.Point) error { return nil }

	// Write series metadata.
	if err := e.WritePoints(nil, nil, []*tsdb.SeriesCreate{
		{Series: &tsdb.Series{Key: string(tsdb.MakeKey([]byte("cpu"), map[string]string{"host": "server0"})), Tags: map[string]string{"host": "server0"}}},
		{Series: &tsdb.Series{Key: string(tsdb.MakeKey([]byte("cpu"), map[string]string{"host": "server1"})), Tags: map[string]string{"host": "server1"}}},
		{Series: &tsdb.Series{Key: "series with spaces"}},
	}); err != nil {
		t.Fatal(err)
	}

	// Load metadata index.
	index := tsdb.NewDatabaseIndex()
	if err := e.LoadMetadataIndex(index, make(map[string]*tsdb.MeasurementFields)); err != nil {
		t.Fatal(err)
	}

	// Verify index is correct.
	if m := index.Measurement("cpu"); m == nil {
		t.Fatal("measurement not found")
	} else if s := m.SeriesByID(1); s.Key != "cpu,host=server0" || !reflect.DeepEqual(s.Tags, map[string]string{"host": "server0"}) {
		t.Fatalf("unexpected series: %q / %#v", s.Key, s.Tags)
	} else if s = m.SeriesByID(2); s.Key != "cpu,host=server1" || !reflect.DeepEqual(s.Tags, map[string]string{"host": "server1"}) {
		t.Fatalf("unexpected series: %q / %#v", s.Key, s.Tags)
	}

	if m := index.Measurement("series with spaces"); m == nil {
		t.Fatal("measurement not found")
	} else if s := m.SeriesByID(3); s.Key != "series with spaces" {
		t.Fatalf("unexpected series: %q", s.Key)
	}
}

// Ensure the engine can write field metadata and reload it.
func TestEngine_LoadMetadataIndex_Fields(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Setup nop mock.
	e.PointsWriter.WritePointsFn = func(a []tsdb.Point) error { return nil }

	// Write series metadata.
	if err := e.WritePoints(nil, map[string]*tsdb.MeasurementFields{
		"cpu": &tsdb.MeasurementFields{
			Fields: map[string]*tsdb.Field{
				"value": &tsdb.Field{ID: 0, Name: "value"},
			},
		},
	}, nil); err != nil {
		t.Fatal(err)
	}

	// Load metadata index.
	mfs := make(map[string]*tsdb.MeasurementFields)
	if err := e.LoadMetadataIndex(tsdb.NewDatabaseIndex(), mfs); err != nil {
		t.Fatal(err)
	}

	// Verify measurement field is correct.
	if mf := mfs["cpu"]; mf == nil {
		t.Fatal("measurement fields not found")
	} else if !reflect.DeepEqual(mf.Fields, map[string]*tsdb.Field{"value": &tsdb.Field{ID: 0, Name: "value"}}) {
		t.Fatalf("unexpected fields: %#v", mf.Fields)
	}
}

// Ensure the engine can write points to storage.
func TestEngine_WritePoints_PointsWriter(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Points to be inserted.
	points := []tsdb.Point{
		tsdb.NewPoint("cpu", tsdb.Tags{}, tsdb.Fields{}, time.Unix(0, 1)),
		tsdb.NewPoint("cpu", tsdb.Tags{}, tsdb.Fields{}, time.Unix(0, 0)),
		tsdb.NewPoint("cpu", tsdb.Tags{}, tsdb.Fields{}, time.Unix(1, 0)),

		tsdb.NewPoint("cpu", tsdb.Tags{"host": "serverA"}, tsdb.Fields{}, time.Unix(0, 0)),
	}

	// Mock points writer to ensure points are passed through.
	var invoked bool
	e.PointsWriter.WritePointsFn = func(a []tsdb.Point) error {
		invoked = true
		if !reflect.DeepEqual(points, a) {
			t.Fatalf("unexpected points: %#v", a)
		}
		return nil
	}

	// Write points against two separate series.
	if err := e.WritePoints(points, nil, nil); err != nil {
		t.Fatal(err)
	} else if !invoked {
		t.Fatal("PointsWriter.WritePoints() not called")
	}
}

// Ensure the engine can write points to the index.
func TestEngine_WriteIndex_Append(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Append points to index.
	if err := e.WriteIndex(map[string][][]byte{
		"cpu": [][]byte{
			append(u64tob(1), []byte{0x10}...),
			append(u64tob(2), []byte{0x20}...),
		},
		"mem": [][]byte{
			append(u64tob(0), []byte{0x30}...),
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Start transaction.
	tx := e.MustBegin(false)
	defer tx.Rollback()

	// Iterate over "cpu" series.
	c := tx.Cursor("cpu")
	if k, v := c.Seek(u64tob(0)); !reflect.DeepEqual(k, []byte{0, 0, 0, 0, 0, 0, 0, 1}) || !reflect.DeepEqual(v, []byte{0x10}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	}
}

// Ensure the engine read and write randomized points.
func TestEngine_WritePoints_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	t.Skip("refactoring...")

	quick.Check(func(blockSize uint16, points Points) bool {
		e := OpenDefaultEngine()
		e.BlockSize = int(blockSize)
		defer e.Close()

		// Write points against two separate series.
		if err := e.WritePoints([]tsdb.Point(points), nil, nil); err != nil {
			t.Fatal(err)
		}

		// Validate each series.
		for _, series := range points.Series() {
			if keys, err := e.ReadSeriesPointKeys(series.Name, series.Tags); err != nil {
				t.Fatal(err)
			} else if exp := series.Keys(); !reflect.DeepEqual(exp, keys) {
				t.Fatalf("series point keys:\n\nexp=%v\n\ngot=%v\n\n", exp, keys)
			}
		}

		return true
	}, nil)
}

func BenchmarkEngine_WritePoints_0b_1(b *testing.B)     { benchmarkEngine_WritePoints_0b(b, 1) }
func BenchmarkEngine_WritePoints_0b_10(b *testing.B)    { benchmarkEngine_WritePoints_0b(b, 10) }
func BenchmarkEngine_WritePoints_0b_100(b *testing.B)   { benchmarkEngine_WritePoints_0b(b, 100) }
func BenchmarkEngine_WritePoints_0b_1000(b *testing.B)  { benchmarkEngine_WritePoints_0b(b, 1000) }
func BenchmarkEngine_WritePoints_0b_10000(b *testing.B) { benchmarkEngine_WritePoints_0b(b, 10000) }
func benchmarkEngine_WritePoints_0b(b *testing.B, seriesN int) {
	benchmarkEngine_WritePoints(b, 0, seriesN)
}

func BenchmarkEngine_WritePoints_512b_100(b *testing.B)   { benchmarkEngine_WritePoints_512b(b, 100) }
func BenchmarkEngine_WritePoints_512b_1000(b *testing.B)  { benchmarkEngine_WritePoints_512b(b, 1000) }
func BenchmarkEngine_WritePoints_512b_10000(b *testing.B) { benchmarkEngine_WritePoints_512b(b, 10000) }
func benchmarkEngine_WritePoints_512b(b *testing.B, seriesN int) {
	benchmarkEngine_WritePoints(b, 512, seriesN)
}

func BenchmarkEngine_WritePoints_1KB_100(b *testing.B)   { benchmarkEngine_WritePoints_1KB(b, 100) }
func BenchmarkEngine_WritePoints_1KB_1000(b *testing.B)  { benchmarkEngine_WritePoints_1KB(b, 1000) }
func BenchmarkEngine_WritePoints_1KB_10000(b *testing.B) { benchmarkEngine_WritePoints_1KB(b, 10000) }
func benchmarkEngine_WritePoints_1KB(b *testing.B, seriesN int) {
	benchmarkEngine_WritePoints(b, 1024, seriesN)
}

func BenchmarkEngine_WritePoints_2KB_100(b *testing.B)   { benchmarkEngine_WritePoints_2KB(b, 100) }
func BenchmarkEngine_WritePoints_2KB_1000(b *testing.B)  { benchmarkEngine_WritePoints_2KB(b, 1000) }
func BenchmarkEngine_WritePoints_2KB_10000(b *testing.B) { benchmarkEngine_WritePoints_2KB(b, 10000) }
func benchmarkEngine_WritePoints_2KB(b *testing.B, seriesN int) {
	benchmarkEngine_WritePoints(b, 2*1024, seriesN)
}

func BenchmarkEngine_WritePoints_4KB_100(b *testing.B)   { benchmarkEngine_WritePoints_4KB(b, 100) }
func BenchmarkEngine_WritePoints_4KB_1000(b *testing.B)  { benchmarkEngine_WritePoints_4KB(b, 1000) }
func BenchmarkEngine_WritePoints_4KB_10000(b *testing.B) { benchmarkEngine_WritePoints_4KB(b, 10000) }
func benchmarkEngine_WritePoints_4KB(b *testing.B, seriesN int) {
	benchmarkEngine_WritePoints(b, 4*1024, seriesN)
}

func BenchmarkEngine_WritePoints_8KB_100(b *testing.B)   { benchmarkEngine_WritePoints_8KB(b, 100) }
func BenchmarkEngine_WritePoints_8KB_1000(b *testing.B)  { benchmarkEngine_WritePoints_8KB(b, 1000) }
func BenchmarkEngine_WritePoints_8KB_10000(b *testing.B) { benchmarkEngine_WritePoints_8KB(b, 10000) }
func benchmarkEngine_WritePoints_8KB(b *testing.B, seriesN int) {
	benchmarkEngine_WritePoints(b, 8*1024, seriesN)
}

func BenchmarkEngine_WritePoints_16KB_100(b *testing.B)   { benchmarkEngine_WritePoints_16KB(b, 100) }
func BenchmarkEngine_WritePoints_16KB_1000(b *testing.B)  { benchmarkEngine_WritePoints_16KB(b, 1000) }
func BenchmarkEngine_WritePoints_16KB_10000(b *testing.B) { benchmarkEngine_WritePoints_16KB(b, 10000) }
func benchmarkEngine_WritePoints_16KB(b *testing.B, seriesN int) {
	benchmarkEngine_WritePoints(b, 16*1024, seriesN)
}

func BenchmarkEngine_WritePoints_32KB_100(b *testing.B)   { benchmarkEngine_WritePoints_32KB(b, 100) }
func BenchmarkEngine_WritePoints_32KB_1000(b *testing.B)  { benchmarkEngine_WritePoints_32KB(b, 1000) }
func BenchmarkEngine_WritePoints_32KB_10000(b *testing.B) { benchmarkEngine_WritePoints_32KB(b, 10000) }
func benchmarkEngine_WritePoints_32KB(b *testing.B, seriesN int) {
	benchmarkEngine_WritePoints(b, 32*1024, seriesN)
}

func benchmarkEngine_WritePoints(b *testing.B, blockSize, seriesN int) {
	e := OpenDefaultEngine()
	e.BlockSize = blockSize
	defer e.Close()

	// Generate data.
	var points []tsdb.Point
	for i := 0; i < b.N; i++ {
		points = append(points, tsdb.NewPoint(
			strconv.Itoa(i/seriesN),
			tsdb.Tags{},
			tsdb.Fields{"value": float64(i)},
			time.Unix(0, 0).Add(time.Duration(i))))
	}
	b.ResetTimer()

	// Write points against two separate series.
	if err := e.WritePoints(points, nil, nil); err != nil {
		b.Fatal(err)
	}

	// Stop timer and measure size per point.
	b.StopTimer()
	if stats, err := e.Stats(); err != nil {
		b.Fatal(err)
	} else {
		b.Logf("n=%d, %.1f b/pt", b.N, float64(stats.Size)/float64(b.N))
	}
}

// Engine represents a test wrapper for bz1.Engine.
type Engine struct {
	*bz1.Engine
	PointsWriter EnginePointsWriter
}

// NewEngine returns a new instance of Engine.
func NewEngine(opt tsdb.EngineOptions) *Engine {
	// Generate temporary file.
	f, _ := ioutil.TempFile("", "bz1-")
	f.Close()
	os.Remove(f.Name())

	// Create test wrapper and attach mocks.
	e := &Engine{
		Engine: bz1.NewEngine(f.Name(), opt).(*bz1.Engine),
	}
	e.Engine.PointsWriter = &e.PointsWriter
	return e
}

// OpenEngine returns an opened instance of Engine. Panic on error.
func OpenEngine(opt tsdb.EngineOptions) *Engine {
	e := NewEngine(opt)
	if err := e.Open(); err != nil {
		panic(err)
	}
	return e
}

// OpenDefaultEngine returns an open Engine with default options.
func OpenDefaultEngine() *Engine { return OpenEngine(tsdb.NewEngineOptions()) }

// Close closes the engine and removes all data.
func (e *Engine) Close() error {
	e.Engine.Close()
	os.RemoveAll(e.Path())
	return nil
}

// MustBegin returns a new tranaction. Panic on error.
func (e *Engine) MustBegin(writable bool) tsdb.Tx {
	tx, err := e.Begin(writable)
	if err != nil {
		panic(err)
	}
	return tx
}

// ReadSeriesPointKeys returns the raw keys for all points store for a series.
func (e *Engine) ReadSeriesPointKeys(name string, tags tsdb.Tags) ([][]byte, error) {
	// Open transaction on engine.
	tx, err := e.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Create a cursor for the series.
	c := tx.Cursor(string(tsdb.MakeKey([]byte(name), tags)))

	// Collect all the keys.
	var keys [][]byte
	for k, _ := c.Seek([]byte{0, 0, 0, 0, 0, 0, 0, 0}); k != nil; k, _ = c.Next() {
		keys = append(keys, copyBytes(k))
	}

	return keys, nil
}

// EnginePointsWriter represents a mock that implements Engine.PointsWriter.
type EnginePointsWriter struct {
	WritePointsFn func(points []tsdb.Point) error
}

func (w *EnginePointsWriter) WritePoints(points []tsdb.Point) error {
	return w.WritePointsFn(points)
}

// copyBytes returns a copy of a byte slice.
func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	other := make([]byte, len(b))
	copy(other, b)
	return other
}

// Points represents a list of points that implements quick.Generator.
type Points []tsdb.Point

// Generate generates a random list of point.
func (Points) Generate(rand *rand.Rand, size int) reflect.Value {
	points := make([]tsdb.Point, rand.Intn(1000))

	for i := range points {
		// Generate individual point fields.
		name := strconv.Itoa(rand.Intn(3))
		tags := tsdb.Tags{"host": strconv.Itoa(rand.Intn(3))}
		value, _ := quick.Value(reflect.TypeOf(float64(0)), rand)
		timestamp := time.Unix(int64(rand.Uint32()), int64(rand.Uint32()))

		// Create point with random data.
		points[i] = tsdb.NewPoint(name, tags, tsdb.Fields{"value": value}, timestamp)
	}

	return reflect.ValueOf(points)
}

// Series returns a list of unique series.
func (a Points) Series() []Series {
	m := make(map[string]Series)
	for _, p := range a {
		// Retrieve series.
		key := string(p.Key())
		s, ok := m[key]

		// If not set already, set name and tags.
		if !ok {
			s.Name = p.Name()
			s.Tags = p.Tags()
		}

		// Append point to series.
		s.Points = append(s.Points, p)

		// Update series.
		m[key] = s
	}

	// Convert to slice of series and sort points.
	var other []Series
	for _, s := range m {
		sort.Sort(tsdb.Points(s.Points))
		other = append(other, s)
	}

	return other
}

// Series represents a name + tag series. It also includes a list of points.
type Series struct {
	Name   string
	Tags   tsdb.Tags
	Points []tsdb.Point
}

// Keys returns byte slice keys for each point.
func (s *Series) Keys() [][]byte {
	var a [][]byte
	for _, p := range s.Points {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(p.UnixNano()))
		a = append(a, b)
	}
	return a
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
