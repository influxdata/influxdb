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
func TestEngine_WritePoints(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Write points against two separate series.
	if err := e.WritePoints([]tsdb.Point{
		tsdb.NewPoint("cpu", tsdb.Tags{}, tsdb.Fields{}, time.Unix(0, 1)),
		tsdb.NewPoint("cpu", tsdb.Tags{}, tsdb.Fields{}, time.Unix(0, 0)),
		tsdb.NewPoint("cpu", tsdb.Tags{}, tsdb.Fields{}, time.Unix(1, 0)),

		tsdb.NewPoint("cpu", tsdb.Tags{"host": "serverA"}, tsdb.Fields{}, time.Unix(0, 0)),
	},
		nil, nil,
	); err != nil {
		t.Fatal(err)
	}

	// Iterate over "cpu" series.
	if keys, err := e.ReadSeriesPointKeys(`cpu`, tsdb.Tags{}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(keys, [][]byte{
		{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // Unix(0, 0)
		{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, // Unix(0, 1)
		{0x00, 0x00, 0x00, 0x00, 0x3b, 0x9a, 0xca, 0x00}, // Unix(1, 0)
	}) {
		t.Fatalf("unexpected series point keys: %v", keys)
	}
}

// Ensure the engine read and write randomized points.
func TestEngine_WritePoints_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

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

// Engine represents a test wrapper for bz1.Engine.
type Engine struct {
	*bz1.Engine
}

// NewEngine returns a new instance of Engine.
func NewEngine(opt tsdb.EngineOptions) *Engine {
	// Generate temporary file.
	f, _ := ioutil.TempFile("", "bz1-")
	f.Close()
	os.Remove(f.Name())

	// Return test wrapper.
	return &Engine{Engine: bz1.NewEngine(f.Name(), opt).(*bz1.Engine)}
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
