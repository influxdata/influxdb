package pd1_test

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/pd1"
)

func TestEngine_WriteAndReadFloats(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	p1 := parsePoint("cpu,host=A value=1.1 1000000000", codec)
	p2 := parsePoint("cpu,host=B value=1.2 1000000000", codec)
	p3 := parsePoint("cpu,host=A value=2.1 2000000000", codec)
	p4 := parsePoint("cpu,host=B value=2.2 2000000000", codec)

	if err := e.WriteAndCompact([]tsdb.Point{p1, p2, p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func() {
		c := e.Cursor("cpu,host=A", tsdb.Forward)
		k, v := c.Next()
		if btou64(k) != uint64(p1.UnixNano()) {
			t.Fatalf("p1 time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), btou64(k))
		}
		if !reflect.DeepEqual(v, p1.Data()) {
			t.Fatal("p1 data not equal")
		}
		k, v = c.Next()
		if btou64(k) != uint64(p3.UnixNano()) {
			t.Fatalf("p3 time wrong:\n\texp:%d\n\tgot:%d\n", p3.UnixNano(), btou64(k))
		}
		if !reflect.DeepEqual(v, p3.Data()) {
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
		if !reflect.DeepEqual(v, p2.Data()) {
			t.Fatal("p2 data not equal")
		}
		k, v = c.Next()
		if k != nil {
			t.Fatal("expected nil")
		}
	}
	verify()

	if err := e.WriteAndCompact([]tsdb.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	verify()

	c := e.Cursor("cpu,host=B", tsdb.Forward)
	k, v := c.Seek(u64tob(2000000000))
	if btou64(k) != uint64(p4.UnixNano()) {
		t.Fatalf("p4 time wrong:\n\texp:%d\n\tgot:%d\n", p4.UnixNano(), btou64(k))
	}
	if !reflect.DeepEqual(v, p4.Data()) {
		t.Fatal("p4 data not equal")
	}
}

func TestEngine_WriteIndexWithCollision(t *testing.T) {
}

func TestEngine_WriteIndexBenchmarkNames(t *testing.T) {
	t.Skip("whatevs")

	e := OpenDefaultEngine()
	defer e.Close()

	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	var points []tsdb.Point
	for i := 0; i < 100000; i++ {
		points = append(points, parsePoint(fmt.Sprintf("cpu%d value=22.1", i), codec))
	}

	st := time.Now()
	if err := e.WriteAndCompact(points, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	fmt.Println("took: ", time.Since(st))

	st = time.Now()
	if err := e.WriteAndCompact(points, nil, nil); err != nil {
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

func parsePoints(buf string, codec *tsdb.FieldCodec) []tsdb.Point {
	points, err := tsdb.ParsePointsString(buf)
	if err != nil {
		panic(fmt.Sprintf("couldn't parse points: %s", err.Error()))
	}
	for _, p := range points {
		b, err := codec.EncodeFields(p.Fields())
		if err != nil {
			panic(fmt.Sprintf("couldn't encode fields: %s", err.Error()))
		}
		p.SetData(b)
	}
	return points
}

func parsePoint(buf string, codec *tsdb.FieldCodec) tsdb.Point {
	return parsePoints(buf, codec)[0]
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
