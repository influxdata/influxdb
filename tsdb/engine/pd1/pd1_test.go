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
	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/pd1"
)

func TestEngine_WriteAndReadFloats(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	e.Shard = newFieldCodecMock(map[string]influxql.DataType{"value": influxql.Float})

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.2 1000000000")
	p3 := parsePoint("cpu,host=A value=2.1 2000000000")
	p4 := parsePoint("cpu,host=B value=2.2 2000000000")

	if err := e.WritePoints([]tsdb.Point{p1, p2, p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func(checkSingleBVal bool) {
		c := e.Cursor("cpu,host=A", tsdb.Forward)
		k, v := c.Next()
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
		k, v = c.Next()
		if k != nil {
			t.Fatal("expected nil")
		}

		c = e.Cursor("cpu,host=B", tsdb.Forward)
		k, v = c.Next()
		if btou64(k) != uint64(p2.UnixNano()) {
			t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), btou64(k))
		}
		if 1.2 != btof64(v) {
			t.Fatal("p2 data not equal")
		}

		if checkSingleBVal {
			k, v = c.Next()
			if k != nil {
				t.Fatal("expected nil")
			}
		}
	}
	verify(true)

	if err := e.WritePoints([]tsdb.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	verify(false)

	c := e.Cursor("cpu,host=B", tsdb.Forward)
	k, v := c.Next()
	if btou64(k) != uint64(p2.UnixNano()) {
		t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), btou64(k))
	}
	if 1.2 != btof64(v) {
		t.Fatal("p2 data not equal")
	}
	k, v = c.Next()
	if btou64(k) != uint64(p4.UnixNano()) {
		t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), btou64(k))
	}
	if 2.2 != btof64(v) {
		t.Fatal("p2 data not equal")
	}

	// verify we can seek
	k, v = c.Seek(u64tob(2000000000))
	if btou64(k) != uint64(p4.UnixNano()) {
		t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), btou64(k))
	}
	if 2.2 != btof64(v) {
		t.Fatal("p2 data not equal")
	}

	c = e.Cursor("cpu,host=A", tsdb.Forward)
	k, v = c.Seek(u64tob(0))
	if btou64(k) != uint64(p1.UnixNano()) {
		t.Fatalf("p1 time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), btou64(k))
	}
	if 1.1 != btof64(v) {
		t.Fatal("p1 data not equal")
	}
}

func TestEngine_WriteIndexWithCollision(t *testing.T) {
}

func TestEngine_WriteIndexBenchmarkNames(t *testing.T) {
	t.Skip("whatevs")

	e := OpenDefaultEngine()
	defer e.Close()

	var points []tsdb.Point
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

// Close closes the engine and removes all data.
func (e *Engine) Close() error {
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

func parsePoints(buf string) []tsdb.Point {
	points, err := tsdb.ParsePointsString(buf)
	if err != nil {
		panic(fmt.Sprintf("couldn't parse points: %s", err.Error()))
	}
	return points
}

func parsePoint(buf string) tsdb.Point {
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
