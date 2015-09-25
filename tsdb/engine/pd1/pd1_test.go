package pd1_test

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/pd1"
)

func TestEngine_WriteAndReadFloats(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Cleanup()

	e.Shard = newFieldCodecMock(map[string]influxql.DataType{"value": influxql.Float})

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.2 1000000000")
	p3 := parsePoint("cpu,host=A value=2.1 2000000000")
	p4 := parsePoint("cpu,host=B value=2.2 2000000000")

	if err := e.WritePoints([]models.Point{p1, p2, p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	fields := []string{"value"}
	var codec *tsdb.FieldCodec

	verify := func(checkSingleBVal bool) {
		c := e.Cursor("cpu,host=A", fields, codec, true)
		k, v := c.Next()
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
		k, v = c.Next()
		if k != tsdb.EOF {
			t.Fatal("expected EOF")
		}

		c = e.Cursor("cpu,host=B", fields, codec, true)
		k, v = c.Next()
		if k != p2.UnixNano() {
			t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), k)
		}
		if 1.2 != v {
			t.Fatal("p2 data not equal")
		}

		if checkSingleBVal {
			k, v = c.Next()
			if k != tsdb.EOF {
				t.Fatal("expected EOF")
			}
		}
	}
	verify(true)

	if err := e.WritePoints([]models.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	verify(false)

	c := e.Cursor("cpu,host=B", fields, codec, true)
	k, v := c.Next()
	if k != p2.UnixNano() {
		t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), k)
	}
	if 1.2 != v {
		t.Fatal("p2 data not equal")
	}
	k, v = c.Next()
	if k != p4.UnixNano() {
		t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), k)
	}
	if 2.2 != v {
		t.Fatal("p2 data not equal")
	}

	// verify we can seek
	k, v = c.SeekTo(2000000000)
	if k != p4.UnixNano() {
		t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), k)
	}
	if 2.2 != v {
		t.Fatal("p2 data not equal")
	}

	c = e.Cursor("cpu,host=A", fields, codec, true)
	k, v = c.SeekTo(0)
	if k != p1.UnixNano() {
		t.Fatalf("p1 time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), k)
	}
	if 1.1 != v {
		t.Fatal("p1 data not equal")
	}

	if err := e.Close(); err != nil {
		t.Fatalf("error closing: %s", err.Error())
	}

	if err := e.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}

	verify(false)
}

func TestEngine_WriteIndexWithCollision(t *testing.T) {
}

func TestEngine_WriteIndexQueryAcrossDataFiles(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Cleanup()

	e.Shard = newFieldCodecMock(map[string]influxql.DataType{"value": influxql.Float})
	e.RotateFileSize = 10

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.1 1000000000")
	p3 := parsePoint("cpu,host=A value=2.4 4000000000")
	p4 := parsePoint("cpu,host=B value=2.4 4000000000")

	if err := e.WritePoints([]models.Point{p1, p2, p3, p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	p5 := parsePoint("cpu,host=A value=1.5 5000000000")
	p6 := parsePoint("cpu,host=B value=2.5 5000000000")
	p7 := parsePoint("cpu,host=A value=1.3 3000000000")
	p8 := parsePoint("cpu,host=B value=2.3 3000000000")

	if err := e.WritePoints([]models.Point{p5, p6, p7, p8}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if count := e.DataFileCount(); count != 2 {
		t.Fatalf("expected 2 data files to exist but got %d", count)
	}

	fields := []string{"value"}
	var codec *tsdb.FieldCodec

	verify := func(series string, points []models.Point, seek int64) {
		c := e.Cursor(series, fields, codec, true)

		// we we want to seek, do it and verify the first point matches
		if seek != 0 {
			k, v := c.SeekTo(seek)
			p := points[0]
			val := p.Fields()["value"]
			if p.UnixNano() != k || val != v {
				t.Fatalf("expected to seek to first point\n\texp: %d %f\n\tgot: %d %f", p.UnixNano(), val, k, v)
			}
			points = points[1:]
		}

		for _, p := range points {
			k, v := c.Next()
			val := p.Fields()["value"]
			if p.UnixNano() != k || val != v {
				t.Fatalf("expected to seek to first point\n\texp: %d %f\n\tgot: %d %f", p.UnixNano(), val, k, v.(float64))
			}
		}
	}

	verify("cpu,host=A", []models.Point{p1, p7, p3, p5}, 0)
	verify("cpu,host=B", []models.Point{p2, p8, p4, p6}, 0)
}

func TestEngine_WriteIndexBenchmarkNames(t *testing.T) {
	t.Skip("whatevs")

	e := OpenDefaultEngine()
	defer e.Cleanup()

	var points []models.Point
	for i := 0; i < 100000; i++ {
		points = append(points, parsePoint(fmt.Sprintf("cpu%d value=22.1", i)))
	}

	st := time.Now()
	if err := e.WritePoints(points, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	fmt.Println("took: ", time.Since(st))

	st = time.Now()
	if err := e.WritePoints(points, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	fmt.Println("took: ", time.Since(st))
}

// Engine represents a test wrapper for pd1.Engine.
type Engine struct {
	*pd1.Engine
}

// NewEngine returns a new instance of Engine.
func NewEngine(opt tsdb.EngineOptions) *Engine {
	dir, err := ioutil.TempDir("", "pd1-test")
	if err != nil {
		panic("couldn't get temp dir")
	}

	// Create test wrapper and attach mocks.
	e := &Engine{
		Engine: pd1.NewEngine(dir, dir, opt).(*pd1.Engine),
	}

	return e
}

// OpenEngine returns an opened instance of Engine. Panic on error.
func OpenEngine(opt tsdb.EngineOptions) *Engine {
	e := NewEngine(opt)
	if err := e.Open(); err != nil {
		panic(err)
	}
	e.WAL.SkipCache = true
	return e
}

// OpenDefaultEngine returns an open Engine with default options.
func OpenDefaultEngine() *Engine { return OpenEngine(tsdb.NewEngineOptions()) }

// Cleanup closes the engine and removes all data.
func (e *Engine) Cleanup() error {
	e.Engine.Close()
	os.RemoveAll(e.Path())
	return nil
}

func newFieldCodecMock(fields map[string]influxql.DataType) *FieldCodeMock {
	m := make(map[string]*tsdb.Field)

	for n, t := range fields {
		m[n] = &tsdb.Field{Name: n, Type: t}
	}
	codec := tsdb.NewFieldCodec(m)

	return &FieldCodeMock{codec: codec}
}

type FieldCodeMock struct {
	codec *tsdb.FieldCodec
}

func (f *FieldCodeMock) FieldCodec(m string) *tsdb.FieldCodec {
	return f.codec
}

func parsePoints(buf string) []models.Point {
	points, err := models.ParsePointsString(buf)
	if err != nil {
		panic(fmt.Sprintf("couldn't parse points: %s", err.Error()))
	}
	return points
}

func parsePoint(buf string) models.Point {
	return parsePoints(buf)[0]
}

func inttob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btof64(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}
