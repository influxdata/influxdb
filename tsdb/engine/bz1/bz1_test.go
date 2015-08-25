package bz1_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/bz1"
	"github.com/influxdb/influxdb/tsdb/engine/wal"
)

// Ensure the engine can write series metadata and reload it.
func TestEngine_LoadMetadataIndex_Series(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Setup mock that writes the index
	seriesToCreate := []*tsdb.SeriesCreate{
		{Series: tsdb.NewSeries(string(tsdb.MakeKey([]byte("cpu"), map[string]string{"host": "server0"})), map[string]string{"host": "server0"})},
		{Series: tsdb.NewSeries(string(tsdb.MakeKey([]byte("cpu"), map[string]string{"host": "server1"})), map[string]string{"host": "server1"})},
		{Series: tsdb.NewSeries("series with spaces", nil)},
	}
	e.PointsWriter.WritePointsFn = func(a []tsdb.Point) error { return e.WriteIndex(nil, nil, seriesToCreate) }

	// Write series metadata.
	if err := e.WritePoints(nil, nil, seriesToCreate); err != nil {
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

	// Setup mock that writes the index
	fields := map[string]*tsdb.MeasurementFields{
		"cpu": &tsdb.MeasurementFields{
			Fields: map[string]*tsdb.Field{
				"value": &tsdb.Field{ID: 0, Name: "value"},
			},
		},
	}
	e.PointsWriter.WritePointsFn = func(a []tsdb.Point) error { return e.WriteIndex(nil, fields, nil) }

	// Write series metadata.
	if err := e.WritePoints(nil, fields, nil); err != nil {
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

// Ensure the engine can return errors from the points writer.
func TestEngine_WritePoints_ErrPointsWriter(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Ensure points writer returns an error.
	e.PointsWriter.WritePointsFn = func(a []tsdb.Point) error { return errors.New("marker") }

	// Write to engine.
	if err := e.WritePoints(nil, nil, nil); err == nil || err.Error() != `write points: marker` {
		t.Fatal(err)
	}
}

// Ensure the engine can write points to the index.
func TestEngine_WriteIndex_Append(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Append points to index.
	if err := e.WriteIndex(map[string][][]byte{
		"cpu": [][]byte{
			append(u64tob(1), 0x10),
			append(u64tob(2), 0x20),
		},
		"mem": [][]byte{
			append(u64tob(0), 0x30),
		},
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Start transaction.
	tx := e.MustBegin(false)
	defer tx.Rollback()

	// Iterate over "cpu" series.
	c := tx.Cursor("cpu")
	if k, v := c.Seek(u64tob(0)); !reflect.DeepEqual(k, []byte{0, 0, 0, 0, 0, 0, 0, 1}) || !reflect.DeepEqual(v, []byte{0x10}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = c.Next(); !reflect.DeepEqual(k, []byte{0, 0, 0, 0, 0, 0, 0, 2}) || !reflect.DeepEqual(v, []byte{0x20}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, _ = c.Next(); k != nil {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	}

	// Iterate over "mem" series.
	c = tx.Cursor("mem")
	if k, v := c.Seek(u64tob(0)); !reflect.DeepEqual(k, []byte{0, 0, 0, 0, 0, 0, 0, 0}) || !reflect.DeepEqual(v, []byte{0x30}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, _ = c.Next(); k != nil {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	}
}

// Ensure the engine can rewrite blocks that contain the new point range.
func TestEngine_WriteIndex_Insert(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Write initial points to index.
	if err := e.WriteIndex(map[string][][]byte{
		"cpu": [][]byte{
			append(u64tob(10), 0x10),
			append(u64tob(20), 0x20),
			append(u64tob(30), 0x30),
		},
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Write overlapping points to index.
	if err := e.WriteIndex(map[string][][]byte{
		"cpu": [][]byte{
			append(u64tob(9), 0x09),
			append(u64tob(10), 0xFF),
			append(u64tob(25), 0x25),
			append(u64tob(31), 0x31),
		},
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Write overlapping points to index again.
	if err := e.WriteIndex(map[string][][]byte{
		"cpu": [][]byte{
			append(u64tob(31), 0xFF),
		},
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Start transaction.
	tx := e.MustBegin(false)
	defer tx.Rollback()

	// Iterate over "cpu" series.
	c := tx.Cursor("cpu")
	if k, v := c.Seek(u64tob(0)); btou64(k) != 9 || !bytes.Equal(v, []byte{0x09}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = c.Next(); btou64(k) != 10 || !bytes.Equal(v, []byte{0xFF}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = c.Next(); btou64(k) != 20 || !bytes.Equal(v, []byte{0x20}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = c.Next(); btou64(k) != 25 || !bytes.Equal(v, []byte{0x25}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = c.Next(); btou64(k) != 30 || !bytes.Equal(v, []byte{0x30}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = c.Next(); btou64(k) != 31 || !bytes.Equal(v, []byte{0xFF}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	}
}

// Ensure that the engine properly seeks to a block when the seek value is in the middle.
func TestEngine_WriteIndex_SeekAgainstInBlockValue(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// make sure we have data split across two blocks
	dataSize := (bz1.DefaultBlockSize - 16) / 2
	data := make([]byte, dataSize, dataSize)
	// Write initial points to index.
	if err := e.WriteIndex(map[string][][]byte{
		"cpu": [][]byte{
			append(u64tob(10), data...),
			append(u64tob(20), data...),
			append(u64tob(30), data...),
			append(u64tob(40), data...),
		},
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Start transaction.
	tx := e.MustBegin(false)
	defer tx.Rollback()

	// Ensure that we can seek to a block in the middle
	c := tx.Cursor("cpu")
	if k, _ := c.Seek(u64tob(15)); btou64(k) != 20 {
		t.Fatalf("expected to seek to time 20, but got %d", btou64(k))
	}
	// Ensure that we can seek to the block on the end
	if k, _ := c.Seek(u64tob(35)); btou64(k) != 40 {
		t.Fatalf("expected to seek to time 40, but got %d", btou64(k))
	}
}

// Ensure the engine ignores writes without keys.
func TestEngine_WriteIndex_NoKeys(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()
	if err := e.WriteIndex(nil, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// Ensure the engine ignores writes without points in a key.
func TestEngine_WriteIndex_NoPoints(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()
	if err := e.WriteIndex(map[string][][]byte{"cpu": nil}, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// Ensure the engine can accept randomly generated points.
func TestEngine_WriteIndex_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	quick.Check(func(sets []Points, blockSize uint) bool {
		e := OpenDefaultEngine()
		e.BlockSize = int(blockSize % 1024) // 1KB max block size
		defer e.Close()

		// Write points to index in multiple sets.
		for _, set := range sets {
			if err := e.WriteIndex(map[string][][]byte(set), nil, nil); err != nil {
				t.Fatal(err)
			}
		}

		// Merge all points together.
		points := MergePoints(sets)

		// Retrieve a sorted list of keys so results are deterministic.
		keys := points.Keys()

		// Start transaction to read index.
		tx := e.MustBegin(false)
		defer tx.Rollback()

		// Iterate over results to ensure they are correct.
		for _, key := range keys {
			c := tx.Cursor(key)

			// Read list of key/values.
			var got [][]byte
			for k, v := c.Seek(u64tob(0)); k != nil; k, v = c.Next() {
				got = append(got, append(copyBytes(k), v...))
			}

			if !reflect.DeepEqual(got, points[key]) {
				t.Fatalf("points: block size=%d, key=%s:\n\ngot=%x\n\nexp=%x\n\n", e.BlockSize, key, got, points[key])
			}
		}

		return true
	}, nil)
}

// Ensure the engine can accept randomly generated append-only points.
func TestEngine_WriteIndex_Quick_Append(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	quick.Check(func(sets appendPointSets, blockSize uint) bool {
		e := OpenDefaultEngine()
		e.BlockSize = int(blockSize % 1024) // 1KB max block size
		defer e.Close()

		// Write points to index in multiple sets.
		for _, set := range sets {
			if err := e.WriteIndex(map[string][][]byte(set), nil, nil); err != nil {
				t.Fatal(err)
			}
		}

		// Merge all points together.
		points := MergePoints([]Points(sets))

		// Retrieve a sorted list of keys so results are deterministic.
		keys := points.Keys()

		// Start transaction to read index.
		tx := e.MustBegin(false)
		defer tx.Rollback()

		// Iterate over results to ensure they are correct.
		for _, key := range keys {
			c := tx.Cursor(key)

			// Read list of key/values.
			var got [][]byte
			for k, v := c.Seek(u64tob(0)); k != nil; k, v = c.Next() {
				got = append(got, append(copyBytes(k), v...))
			}

			if !reflect.DeepEqual(got, points[key]) {
				t.Fatalf("points: block size=%d, key=%s:\n\ngot=%x\n\nexp=%x\n\n", e.BlockSize, key, got, points[key])
			}
		}

		return true
	}, nil)
}

func BenchmarkEngine_WriteIndex_512b(b *testing.B)  { benchmarkEngine_WriteIndex(b, 512) }
func BenchmarkEngine_WriteIndex_1KB(b *testing.B)   { benchmarkEngine_WriteIndex(b, 1*1024) }
func BenchmarkEngine_WriteIndex_4KB(b *testing.B)   { benchmarkEngine_WriteIndex(b, 4*1024) }
func BenchmarkEngine_WriteIndex_16KB(b *testing.B)  { benchmarkEngine_WriteIndex(b, 16*1024) }
func BenchmarkEngine_WriteIndex_32KB(b *testing.B)  { benchmarkEngine_WriteIndex(b, 32*1024) }
func BenchmarkEngine_WriteIndex_64KB(b *testing.B)  { benchmarkEngine_WriteIndex(b, 64*1024) }
func BenchmarkEngine_WriteIndex_128KB(b *testing.B) { benchmarkEngine_WriteIndex(b, 128*1024) }
func BenchmarkEngine_WriteIndex_256KB(b *testing.B) { benchmarkEngine_WriteIndex(b, 256*1024) }

func benchmarkEngine_WriteIndex(b *testing.B, blockSize int) {
	// Skip small iterations.
	if b.N < 1000000 {
		return
	}

	// Create a simple engine.
	e := OpenDefaultEngine()
	e.BlockSize = blockSize
	defer e.Close()

	// Create codec.
	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	// Generate points.
	a := make(map[string][][]byte)
	a["cpu"] = make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		a["cpu"][i] = wal.MarshalEntry(int64(i), MustEncodeFields(codec, tsdb.Fields{"value": float64(i)}))
	}

	b.ResetTimer()

	// Insert into engine.
	if err := e.WriteIndex(a, nil, nil); err != nil {
		b.Fatal(err)
	}

	// Calculate on-disk size per point.
	bs, _ := e.SeriesBucketStats("cpu")
	stats, err := e.Stats()
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("pts=%9d  bytes/pt=%4.01f  leaf-util=%3.0f%%",
		b.N,
		float64(stats.Size)/float64(b.N),
		(float64(bs.LeafInuse)/float64(bs.LeafAlloc))*100.0,
	)
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
	walPath := filepath.Join(f.Name(), "wal")

	// Create test wrapper and attach mocks.
	e := &Engine{
		Engine: bz1.NewEngine(f.Name(), walPath, opt).(*bz1.Engine),
	}
	e.Engine.WAL = &e.PointsWriter
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

// EnginePointsWriter represents a mock that implements Engine.PointsWriter.
type EnginePointsWriter struct {
	WritePointsFn func(points []tsdb.Point) error
}

func (w *EnginePointsWriter) WritePoints(points []tsdb.Point, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	return w.WritePointsFn(points)
}

func (w *EnginePointsWriter) LoadMetadataIndex(index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
	return nil
}

func (w *EnginePointsWriter) DeleteSeries(keys []string) error { return nil }

func (w *EnginePointsWriter) Open() error { return nil }

func (w *EnginePointsWriter) Close() error { return nil }

func (w *EnginePointsWriter) Cursor(key string) tsdb.Cursor { return &Cursor{} }

func (w *EnginePointsWriter) Flush() error { return nil }

// Cursor represents a mock that implements tsdb.Curosr.
type Cursor struct {
}

func (c *Cursor) Seek(key []byte) ([]byte, []byte) { return nil, nil }

func (c *Cursor) Next() ([]byte, []byte) { return nil, nil }

// Points represents a set of encoded points by key. Implements quick.Generator.
type Points map[string][][]byte

// Keys returns a sorted list of keys.
func (m Points) Keys() []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (Points) Generate(rand *rand.Rand, size int) reflect.Value {
	return reflect.ValueOf(Points(GeneratePoints(rand, size,
		rand.Intn(size),
		func(_ int) time.Time { return time.Unix(0, 0).Add(time.Duration(rand.Intn(100))) },
	)))
}

// appendPointSets represents sets of sequential points. Implements quick.Generator.
type appendPointSets []Points

func (appendPointSets) Generate(rand *rand.Rand, size int) reflect.Value {
	sets := make([]Points, 0)
	for i, n := 0, rand.Intn(size); i < n; i++ {
		sets = append(sets, GeneratePoints(rand, size,
			rand.Intn(size),
			func(j int) time.Time {
				return time.Unix(0, 0).Add((time.Duration(i) * time.Second) + (time.Duration(j) * time.Nanosecond))
			},
		))
	}
	return reflect.ValueOf(appendPointSets(sets))
}

func GeneratePoints(rand *rand.Rand, size, seriesN int, timestampFn func(int) time.Time) Points {
	// Generate series with a random number of points in each.
	m := make(Points)
	for i := 0; i < seriesN; i++ {
		key := strconv.Itoa(i)

		// Generate points for the series.
		for j, pointN := 0, rand.Intn(size); j < pointN; j++ {
			timestamp := timestampFn(j)
			data, ok := quick.Value(reflect.TypeOf([]byte(nil)), rand)
			if !ok {
				panic("cannot generate data")
			}
			m[key] = append(m[key], bz1.MarshalEntry(timestamp.UnixNano(), data.Interface().([]byte)))
		}
	}
	return m
}

// MergePoints returns a map of all points merged together by key.
// Later points will overwrite earlier ones.
func MergePoints(a []Points) Points {
	// Combine all points into one set.
	m := make(Points)
	for _, set := range a {
		for key, values := range set {
			m[key] = append(m[key], values...)
		}
	}

	// Dedupe points.
	for key, values := range m {
		m[key] = tsdb.DedupeEntries(values)
	}

	return m
}

// MustEncodeFields encodes fields with codec. Panic on error.
func MustEncodeFields(codec *tsdb.FieldCodec, fields tsdb.Fields) []byte {
	b, err := codec.EncodeFields(fields)
	if err != nil {
		panic(err)
	}
	return b
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

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 converts an 8-byte slice into an uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
