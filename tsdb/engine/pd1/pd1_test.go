package pd1_test

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"reflect"
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

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.2 1000000000")
	p3 := parsePoint("cpu,host=A value=2.1 2000000000")
	p4 := parsePoint("cpu,host=B value=2.2 2000000000")

	if err := e.WritePoints([]models.Point{p1, p2, p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	fields := []string{"value"}
	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	verify := func(checkSingleBVal bool) {
		c := e.Cursor("cpu,host=A", fields, codec, true)
		k, v := c.SeekTo(0)
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
		k, v = c.SeekTo(0)
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
	k, v := c.SeekTo(0)
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
	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	verify := func(series string, points []models.Point, seek int64) {
		c := e.Cursor(series, fields, codec, true)

		k, v := c.SeekTo(seek)
		p := points[0]
		val := p.Fields()["value"]
		if p.UnixNano() != k || val != v {
			t.Fatalf("expected to seek to first point\n\texp: %d %f\n\tgot: %d %f", p.UnixNano(), val, k, v)
		}
		points = points[1:]

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
	verify("cpu,host=A", []models.Point{p5}, 5000000000)
	verify("cpu,host=B", []models.Point{p6}, 5000000000)
}

func TestEngine_WriteOverwritePreviousPoint(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Cleanup()

	fields := []string{"value"}
	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 1000000000")
	p3 := parsePoint("cpu,host=A value=1.3 1000000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	c := e.Cursor("cpu,host=A", fields, codec, true)
	k, v := c.SeekTo(0)
	if k != p2.UnixNano() {
		t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), k)
	}
	if 1.2 != v {
		t.Fatalf("data wrong:\n\texp:%f\n\tgot:%f", 1.2, v.(float64))
	}
	k, v = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}

	if err := e.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	c = e.Cursor("cpu,host=A", fields, codec, true)
	k, v = c.SeekTo(0)
	if k != p3.UnixNano() {
		t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p3.UnixNano(), k)
	}
	if 1.3 != v {
		t.Fatalf("data wrong:\n\texp:%f\n\tgot:%f", 1.3, v.(float64))
	}
	k, v = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}
}

func TestEngine_CursorCombinesWALAndIndex(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Cleanup()

	fields := []string{"value"}
	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")

	if err := e.WritePoints([]models.Point{p1}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	e.WAL.SkipCache = false
	if err := e.WritePoints([]models.Point{p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	c := e.Cursor("cpu,host=A", fields, codec, true)
	k, v := c.SeekTo(0)
	if k != p1.UnixNano() {
		t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), k)
	}
	if 1.1 != v {
		t.Fatalf("data wrong:\n\texp:%f\n\tgot:%f", 1.1, v.(float64))
	}
	k, v = c.Next()
	if k != p2.UnixNano() {
		t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), k)
	}
	if 1.2 != v {
		t.Fatalf("data wrong:\n\texp:%f\n\tgot:%f", 1.2, v.(float64))
	}
	k, v = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}
}

func TestEngine_Compaction(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Cleanup()

	e.RotateFileSize = 10

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.1 1000000000")
	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	p3 := parsePoint("cpu,host=A value=2.4 4000000000")
	p4 := parsePoint("cpu,host=B value=2.4 4000000000")
	if err := e.WritePoints([]models.Point{p3, p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	p5 := parsePoint("cpu,host=A value=1.5 5000000000")
	p6 := parsePoint("cpu,host=B value=2.5 5000000000")
	if err := e.WritePoints([]models.Point{p5, p6}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	p7 := parsePoint("cpu,host=A value=1.5 6000000000")
	p8 := parsePoint("cpu,host=B value=2.5 6000000000")
	if err := e.WritePoints([]models.Point{p7, p8}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if count := e.DataFileCount(); count != 4 {
		t.Fatalf("expected 3 data files to exist but got %d", count)
	}

	fields := []string{"value"}
	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	e.CompactionAge = time.Duration(0)

	if err := e.Compact(); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}

	if count := e.DataFileCount(); count != 1 {
		t.Fatalf("expected compaction to reduce data file count to 1 but got %d", count)
	}

	verify := func(series string, points []models.Point, seek int64) {
		c := e.Cursor(series, fields, codec, true)

		k, v := c.SeekTo(seek)
		p := points[0]
		val := p.Fields()["value"]
		if p.UnixNano() != k || val != v {
			t.Fatalf("expected to seek to first point\n\texp: %d %f\n\tgot: %d %f", p.UnixNano(), val, k, v)
		}
		points = points[1:]

		for _, p := range points {
			k, v := c.Next()
			val := p.Fields()["value"]
			if p.UnixNano() != k || val != v {
				t.Fatalf("expected to seek to first point\n\texp: %d %f\n\tgot: %d %f", p.UnixNano(), val, k, v.(float64))
			}
		}
	}

	if err := e.Close(); err != nil {
		t.Fatalf("error closing: %s", err.Error())
	}
	if err := e.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}
	verify("cpu,host=A", []models.Point{p1, p3, p5, p7}, 0)
	verify("cpu,host=B", []models.Point{p2, p4, p6, p8}, 0)
}

// Ensure that if two keys have the same fnv64-a id, we handle it
func TestEngine_KeyCollisionsAreHandled(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Cleanup()

	fields := []string{"value"}
	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	// make sure two of these keys collide
	e.HashSeriesField = func(key string) uint64 {
		return 1
	}
	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.2 1000000000")
	p3 := parsePoint("cpu,host=C value=1.3 1000000000")

	if err := e.WritePoints([]models.Point{p1, p2, p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func(series string, points []models.Point, seek int64) {
		c := e.Cursor(series, fields, codec, true)

		k, v := c.SeekTo(seek)
		p := points[0]
		val := p.Fields()["value"]
		if p.UnixNano() != k || val != v {
			t.Fatalf("expected to seek to first point\n\texp: %d %f\n\tgot: %d %f", p.UnixNano(), val, k, v)
		}
		points = points[1:]

		for _, p := range points {
			k, v := c.Next()
			val := p.Fields()["value"]
			if p.UnixNano() != k || val != v {
				t.Fatalf("expected to seek to first point\n\texp: %d %f\n\tgot: %d %f", p.UnixNano(), val, k, v.(float64))
			}
		}
	}

	verify("cpu,host=A", []models.Point{p1}, 0)
	verify("cpu,host=B", []models.Point{p2}, 0)
	verify("cpu,host=C", []models.Point{p3}, 0)

	p4 := parsePoint("cpu,host=A value=2.1 2000000000")
	p5 := parsePoint("cpu,host=B value=2.2 2000000000")
	p6 := parsePoint("cpu,host=C value=2.3 2000000000")

	if err := e.WritePoints([]models.Point{p4, p5, p6}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify("cpu,host=A", []models.Point{p1, p4}, 0)
	verify("cpu,host=B", []models.Point{p2, p5}, 0)
	verify("cpu,host=C", []models.Point{p3, p6}, 0)

	// verify collisions are handled after closing and reopening
	if err := e.Close(); err != nil {
		t.Fatalf("error closing: %s", err.Error())
	}
	if err := e.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}

	verify("cpu,host=A", []models.Point{p1, p4}, 0)
	verify("cpu,host=B", []models.Point{p2, p5}, 0)
	verify("cpu,host=C", []models.Point{p3, p6}, 0)

	p7 := parsePoint("cpu,host=A value=3.1 3000000000")
	p8 := parsePoint("cpu,host=B value=3.2 3000000000")
	p9 := parsePoint("cpu,host=C value=3.3 3000000000")

	if err := e.WritePoints([]models.Point{p7, p8, p9}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify("cpu,host=A", []models.Point{p1, p4, p7}, 0)
	verify("cpu,host=B", []models.Point{p2, p5, p8}, 0)
	verify("cpu,host=C", []models.Point{p3, p6, p9}, 0)
}

func TestEngine_SupportMultipleFields(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Cleanup()

	fields := []string{"value", "foo"}

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2,foo=2.2 2000000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	c := e.Cursor("cpu,host=A", fields, nil, true)
	k, v := c.SeekTo(0)
	if k != p1.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p1.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.1}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, v = c.Next()
	if k != p2.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p2.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.2, "foo": 2.2}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, _ = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}

	// verify we can update a field and it's still all good
	p11 := parsePoint("cpu,host=A foo=2.1 1000000000")
	if err := e.WritePoints([]models.Point{p11}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	c = e.Cursor("cpu,host=A", fields, nil, true)
	k, v = c.SeekTo(0)
	if k != p1.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p1.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.1, "foo": 2.1}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, v = c.Next()
	if k != p2.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p2.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.2, "foo": 2.2}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, _ = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}

	// verify it's all good with the wal in the picture
	e.WAL.SkipCache = false

	p3 := parsePoint("cpu,host=A value=1.3 3000000000")
	p4 := parsePoint("cpu,host=A value=1.4,foo=2.4 4000000000")
	if err := e.WritePoints([]models.Point{p3, p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	c = e.Cursor("cpu,host=A", fields, nil, true)
	k, v = c.SeekTo(0)
	if k != p1.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p1.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.1, "foo": 2.1}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, v = c.Next()
	if k != p2.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p2.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.2, "foo": 2.2}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, v = c.Next()
	if k != p3.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p3.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.3}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, v = c.Next()
	if k != p4.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p2.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.4, "foo": 2.4}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, _ = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}

	p33 := parsePoint("cpu,host=A foo=2.3 3000000000")
	if err := e.WritePoints([]models.Point{p33}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	c = e.Cursor("cpu,host=A", fields, nil, true)
	k, v = c.SeekTo(0)
	if k != p1.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p1.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.1, "foo": 2.1}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, v = c.Next()
	if k != p2.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p2.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.2, "foo": 2.2}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, v = c.Next()
	if k != p3.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p3.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.3, "foo": 2.3}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, v = c.Next()
	if k != p4.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p2.UnixNano(), k)
	}
	if !reflect.DeepEqual(v, map[string]interface{}{"value": 1.4, "foo": 2.4}) {
		t.Fatalf("value wrong: %v", v)
	}
	k, _ = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}

	// and ensure we can grab one of the fields
	c = e.Cursor("cpu,host=A", []string{"value"}, nil, true)
	k, v = c.SeekTo(4000000000)
	if k != p4.UnixNano() {
		t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", p4.UnixNano(), k)
	}
	if v != 1.4 {
		t.Fatalf("value wrong: %v", v)
	}
	k, _ = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}
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
	e.SkipCompaction = true
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
