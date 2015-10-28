package tsm1_test

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func TestEngine_WriteAndReadFloats(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.2 1000000000")
	p3 := parsePoint("cpu,host=A value=2.1 2000000000")
	p4 := parsePoint("cpu,host=B value=2.2 2000000000")

	if err := e.WritePoints([]models.Point{p1, p2, p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	fields := []string{"value"}

	verify := func(checkSingleBVal bool) {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=A", fields, nil, true)
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

		c = tx.Cursor("cpu,host=B", fields, nil, true)
		k, v = c.SeekTo(0)
		if k != p2.UnixNano() {
			t.Fatalf("p2 time wrong:\n\texp:%d\n\tgot:%d\n", p2.UnixNano(), k)
		}
		if 1.2 != v {
			t.Fatal("p2 data not equal")
		}

		if checkSingleBVal {
			k, _ = c.Next()
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

	tx, _ := e.Begin(false)
	c := tx.Cursor("cpu,host=B", fields, nil, true)
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

	c = tx.Cursor("cpu,host=A", fields, nil, true)
	k, v = c.SeekTo(0)
	if k != p1.UnixNano() {
		t.Fatalf("p1 time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), k)
	}
	if 1.1 != v {
		t.Fatal("p1 data not equal")
	}
	tx.Rollback()

	if err := e.Engine.Close(); err != nil {
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
	defer e.Close()

	e.RotateFileSize = 10

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.2 1000000000")
	p3 := parsePoint("cpu,host=A value=2.1 4000000000")
	p4 := parsePoint("cpu,host=B value=2.2 4000000000")

	if err := e.WritePoints([]models.Point{p1, p2, p3, p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	p5 := parsePoint("cpu,host=A value=3.1 5000000000")
	p6 := parsePoint("cpu,host=B value=3.2 5000000000")
	p7 := parsePoint("cpu,host=A value=4.1 3000000000")
	p8 := parsePoint("cpu,host=B value=4.2 3000000000")

	if err := e.WritePoints([]models.Point{p5, p6, p7, p8}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if count := e.DataFileCount(); count != 2 {
		t.Fatalf("expected 2 data files to exist but got %d", count)
	}

	fields := []string{"value"}

	verify := func(series string, points []models.Point, seek int64) {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor(series, fields, nil, true)

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

	fmt.Println("v1")
	verify("cpu,host=A", []models.Point{p1, p7, p3, p5}, 0)
	fmt.Println("v2")
	verify("cpu,host=B", []models.Point{p2, p8, p4, p6}, 0)
	fmt.Println("v3")
	verify("cpu,host=A", []models.Point{p5}, 5000000000)
	fmt.Println("v4")
	verify("cpu,host=B", []models.Point{p6}, 5000000000)
}

func TestEngine_WriteOverwritePreviousPoint(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 1000000000")
	p3 := parsePoint("cpu,host=A value=1.3 1000000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	tx, _ := e.Begin(false)
	defer tx.Rollback()
	c := tx.Cursor("cpu,host=A", fields, nil, true)
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

	tx2, _ := e.Begin(false)
	defer tx2.Rollback()
	c = tx2.Cursor("cpu,host=A", fields, nil, true)
	k, v = c.SeekTo(0)
	if k != p3.UnixNano() {
		t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p3.UnixNano(), k)
	}
	if 1.3 != v {
		t.Fatalf("data wrong:\n\texp:%f\n\tgot:%f", 1.3, v.(float64))
	}
	k, _ = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}
}

// Tests that writing a point that before the earliest point
// is queryable before and after a full compaction
func TestEngine_Write_BeforeFirstPoint(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	e.RotateFileSize = 1

	fields := []string{"value"}

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")
	p4 := parsePoint("cpu,host=A value=1.4 0000000000") // earlier than first point

	verify := func(points []models.Point) {
		tx2, _ := e.Begin(false)
		defer tx2.Rollback()
		c := tx2.Cursor("cpu,host=A", fields, nil, true)
		k, v := c.SeekTo(0)

		for _, p := range points {
			if k != p.UnixNano() {
				t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p.UnixNano(), k)
			}
			if v != p.Fields()["value"] {
				t.Fatalf("data wrong:\n\texp:%f\n\tgot:%f", p.Fields()["value"], v.(float64))
			}
			k, v = c.Next()
		}
	}

	// Write each point individually to force file rotation
	for _, p := range []models.Point{p1, p2} {
		if err := e.WritePoints([]models.Point{p}, nil, nil); err != nil {
			t.Fatalf("failed to write points: %s", err.Error())
		}
	}

	verify([]models.Point{p1, p2})

	// Force a full compaction
	e.CompactionAge = time.Duration(0)
	if err := e.Compact(true); err != nil {
		t.Fatalf("failed to run full compaction: %v", err)
	}

	// Write a point before the earliest data file
	if err := e.WritePoints([]models.Point{p3, p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Verify earlier point is returned in the correct order before compaction
	verify([]models.Point{p4, p1, p2, p3})

	if err := e.Compact(true); err != nil {
		t.Fatalf("failed to run full compaction: %v", err)
	}
	// Verify earlier point is returned in the correct order after compaction
	verify([]models.Point{p4, p1, p2, p3})
}

// Tests that writing a series with different fields is queryable before and
// after a full compaction
func TestEngine_Write_MixedFields(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	e.RotateFileSize = 1

	fields := []string{"value", "value2"}

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2,value2=2.1 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")
	p4 := parsePoint("cpu,host=A value=1.4 4000000000")

	verify := func(points []models.Point) {
		tx2, _ := e.Begin(false)
		defer tx2.Rollback()
		c := tx2.Cursor("cpu,host=A", fields, nil, true)
		k, v := c.SeekTo(0)

		for _, p := range points {
			if k != p.UnixNano() {
				t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p.UnixNano(), k)
			}

			pv := v.(map[string]interface{})
			for _, key := range fields {
				if pv[key] != p.Fields()[key] {
					t.Fatalf("data wrong:\n\texp:%v\n\tgot:%v", p.Fields()[key], pv[key])
				}
			}
			k, v = c.Next()
		}
	}

	// Write each point individually to force file rotation
	for _, p := range []models.Point{p1, p2} {
		if err := e.WritePoints([]models.Point{p}, nil, nil); err != nil {
			t.Fatalf("failed to write points: %s", err.Error())
		}
	}

	verify([]models.Point{p1, p2})

	// Force a full compaction
	e.CompactionAge = time.Duration(0)
	if err := e.Compact(true); err != nil {
		t.Fatalf("failed to run full compaction: %v", err)
	}

	// Write a point before the earliest data file
	if err := e.WritePoints([]models.Point{p3, p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Verify points returned in the correct order before compaction
	verify([]models.Point{p1, p2, p3, p4})

	if err := e.Compact(true); err != nil {
		t.Fatalf("failed to run full compaction: %v", err)
	}
	// Verify points returned in the correct order after compaction
	verify([]models.Point{p1, p2, p3, p4})
}

// Tests that writing and compactions running concurrently does not
// fail.
func TestEngine_WriteCompaction_Concurrent(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	e.RotateFileSize = 1

	done := make(chan struct{})
	total := 1000

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			if i > total {
				return
			}

			pt := models.MustNewPoint("cpu",
				map[string]string{"host": "A"},
				map[string]interface{}{"value": i},
				time.Unix(int64(i), 0),
			)
			if err := e.WritePoints([]models.Point{pt}, nil, nil); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			i++
		}
	}()

	// Force a compactions to happen
	e.CompactionAge = time.Duration(0)

	// Run compactions concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}

			if err := e.Compact(false); err != nil {
				t.Fatalf("failed to run full compaction: %v", err)
			}
		}
	}()

	// Let the goroutines run for a second
	select {
	case <-time.After(1 * time.Second):
		close(done)
	}

	// Wait for them to exit
	wg.Wait()

	tx2, _ := e.Begin(false)
	defer tx2.Rollback()
	c := tx2.Cursor("cpu,host=A", []string{"value"}, nil, true)
	k, v := c.SeekTo(0)

	// Verify we wrote and can read all the points
	i := 0
	for {
		if exp := time.Unix(int64(i), 0).UnixNano(); k != exp {
			t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", exp, k)
		}

		if exp := int64(i); v != exp {
			t.Fatalf("value wrong:\n\texp:%v\n\tgot:%v", exp, v)
		}

		k, v = c.Next()
		if k == tsdb.EOF {
			break
		}
		i += 1
	}

	if i != total {
		t.Fatalf("point count mismatch: got %v, exp %v", i, total)
	}

}

func TestEngine_CursorCombinesWALAndIndex(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")

	if err := e.WritePoints([]models.Point{p1}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	e.WAL.SkipCache = false
	if err := e.WritePoints([]models.Point{p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	tx, _ := e.Begin(false)
	defer tx.Rollback()
	c := tx.Cursor("cpu,host=A", fields, nil, true)
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
	k, _ = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}
}

func TestEngine_Compaction(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

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

	e.CompactionAge = time.Duration(0)

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}

	if count := e.DataFileCount(); count != 1 {
		t.Fatalf("expected compaction to reduce data file count to 1 but got %d", count)
	}

	verify := func(series string, points []models.Point, seek int64) {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor(series, fields, nil, true)

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

	verify("cpu,host=A", []models.Point{p1, p3, p5, p7}, 0)
	verify("cpu,host=B", []models.Point{p2, p4, p6, p8}, 0)
	if err := e.Engine.Close(); err != nil {
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
	defer e.Close()

	fields := []string{"value"}

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
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor(series, fields, nil, true)

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
	if err := e.Engine.Close(); err != nil {
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
	defer e.Close()

	fields := []string{"value", "foo"}

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2,foo=2.2 2000000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	tx, _ := e.Begin(false)
	defer tx.Rollback()
	c := tx.Cursor("cpu,host=A", fields, nil, true)
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

	tx2, _ := e.Begin(false)
	defer tx2.Rollback()
	c = tx2.Cursor("cpu,host=A", fields, nil, true)
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

	tx3, _ := e.Begin(false)
	defer tx3.Rollback()
	c = tx3.Cursor("cpu,host=A", fields, nil, true)
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

	tx4, _ := e.Begin(false)
	defer tx4.Rollback()
	c = tx4.Cursor("cpu,host=A", fields, nil, true)
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
	c = tx4.Cursor("cpu,host=A", []string{"value"}, nil, true)
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

func TestEngine_WriteManyPointsToSingleSeries(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	var points []models.Point
	for i := 1; i <= 10000; i++ {
		points = append(points, parsePoint(fmt.Sprintf("cpu,host=A value=%d %d000000000", i, i)))
		if i%500 == 0 {
			if err := e.WritePoints(points, nil, nil); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			points = nil
		}
	}

	tx, _ := e.Begin(false)
	defer tx.Rollback()
	c := tx.Cursor("cpu,host=A", fields, nil, true)
	k, v := c.SeekTo(0)
	for i := 2; i <= 10000; i++ {
		k, v = c.Next()
		if k != int64(i)*1000000000 {
			t.Fatalf("time wrong:\n\texp: %d\n\tgot: %d", i*1000000000, k)
		}
		if v != float64(i) {
			t.Fatalf("value wrong:\n\texp:%v\n\tgot:%v", float64(i), v)
		}
	}
	k, _ = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF")
	}
}

func TestEngine_WritePointsInMultipleRequestsWithSameTime(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	e.WAL.SkipCache = false

	if err := e.WritePoints([]models.Point{parsePoint("foo value=1 0")}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{parsePoint("foo value=2 0")}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{parsePoint("foo value=3 0")}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("foo", fields, nil, true)
		k, v := c.SeekTo(0)
		if k != 0 {
			t.Fatalf("expected 0 time but got %d", k)
		}
		if v != float64(3) {
			t.Fatalf("expected 3 for value but got %f", v.(float64))
		}
		k, _ = c.Next()
		if k != tsdb.EOF {
			t.Fatal("expected EOF")
		}
	}

	verify()

	if err := e.Engine.Close(); err != nil {
		t.Fatalf("error closing: %s", err.Error())
	}
	if err := e.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}

	verify()
}

func TestEngine_CursorDescendingOrder(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	p1 := parsePoint("foo value=1 1")
	p2 := parsePoint("foo value=2 2")

	e.WAL.SkipCache = false

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("foo", fields, nil, false)
		fmt.Println("seek")
		k, v := c.SeekTo(5000000)
		if k != 2 {
			t.Fatalf("expected 2 time but got %d", k)
		}
		if v != float64(2) {
			t.Fatalf("expected 2 for value but got %f", v.(float64))
		}
		fmt.Println("next1")
		k, v = c.Next()
		if k != 1 {
			t.Fatalf("expected 1 time but got %d", k)
		}
		fmt.Println("next2")
		if v != float64(1) {
			t.Fatalf("expected 1 for value but got %f", v.(float64))
		}
		k, _ = c.Next()
		if k != tsdb.EOF {
			t.Fatal("expected EOF", k)
		}
	}
	fmt.Println("verify 1")
	verify()

	if err := e.WAL.Flush(); err != nil {
		t.Fatalf("error flushing WAL %s", err.Error())
	}

	fmt.Println("verify 2")
	verify()

	p3 := parsePoint("foo value=3 3")

	if err := e.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("foo", fields, nil, false)
		k, v := c.SeekTo(234232)
		if k != 3 {
			t.Fatalf("expected 3 time but got %d", k)
		}
		if v != float64(3) {
			t.Fatalf("expected 3 for value but got %f", v.(float64))
		}
		k, _ = c.Next()
		if k != 2 {
			t.Fatalf("expected 2 time but got %d", k)
		}
	}()
}

func TestEngine_CompactWithSeriesInOneFile(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	e.RotateFileSize = 10
	e.MaxPointsPerBlock = 1

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")

	if err := e.WritePoints([]models.Point{p1}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if count := e.DataFileCount(); count != 3 {
		t.Fatalf("expected 3 data files but got %d", count)
	}

	verify := func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=A", fields, nil, true)
		k, v := c.SeekTo(0)
		if k != 1000000000 {
			t.Fatalf("expected time 1000000000 but got %d", k)
		}
		if v != 1.1 {
			t.Fatalf("expected value 1.1 but got %f", v.(float64))
		}
		k, v = c.Next()
		if k != 3000000000 {
			t.Fatalf("expected time 3000000000 but got %d", k)
		}
		c = tx.Cursor("cpu,host=B", fields, nil, true)
		k, v = c.SeekTo(0)
		if k != 2000000000 {
			t.Fatalf("expected time 2000000000 but got %d", k)
		}
		if v != 1.2 {
			t.Fatalf("expected value 1.2 but got %f", v.(float64))
		}
	}

	fmt.Println("verify 1")
	verify()

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}
	fmt.Println("verify 2")
	verify()

	p4 := parsePoint("cpu,host=A value=1.4 4000000000")
	if err := e.WritePoints([]models.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}
	tx1, _ := e.Begin(false)
	defer tx1.Rollback()
	c := tx1.Cursor("cpu,host=A", fields, nil, true)
	k, v := c.SeekTo(0)
	if k != 1000000000 {
		t.Fatalf("expected time 1000000000 but got %d", k)
	}
	if v != 1.1 {
		t.Fatalf("expected value 1.1 but got %f", v.(float64))
	}
	k, v = c.Next()
	if k != 3000000000 {
		t.Fatalf("expected time 3000000000 but got %d", k)
	}
	k, _ = c.Next()
	if k != 4000000000 {
		t.Fatalf("expected time 3000000000 but got %d", k)
	}
}

// Ensure that compactions that happen where blocks from old data files
// skip decoding and just get copied over to the new data file works.
func TestEngine_CompactionWithCopiedBlocks(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	e.RotateFileSize = 10
	e.MaxPointsPerBlock = 1

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=A", fields, nil, true)
		k, _ := c.SeekTo(0)
		if k != 1000000000 {
			t.Fatalf("expected time 1000000000 but got %d", k)
		}
		k, _ = c.Next()
		if k != 2000000000 {
			t.Fatalf("expected time 2000000000 but got %d", k)
		}
		k, _ = c.Next()
		if k != 3000000000 {
			t.Fatalf("expected time 3000000000 but got %d", k)
		}
	}

	verify()
	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}
	fmt.Println("verify 2")
	verify()

	p4 := parsePoint("cpu,host=B value=1.4 4000000000")
	if err := e.WritePoints([]models.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}
	fmt.Println("verify 3")
	verify()

	p5 := parsePoint("cpu,host=A value=1.5 5000000000")
	p6 := parsePoint("cpu,host=A value=1.6 6000000000")
	p7 := parsePoint("cpu,host=B value=2.1 7000000000")
	if err := e.WritePoints([]models.Point{p5, p6, p7}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	p8 := parsePoint("cpu,host=A value=1.5 7000000000")
	p9 := parsePoint("cpu,host=A value=1.6 8000000000")
	p10 := parsePoint("cpu,host=B value=2.1 8000000000")
	if err := e.WritePoints([]models.Point{p8, p9, p10}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}
	verify()

}

func TestEngine_RewritingOldBlocks(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	e.MaxPointsPerBlock = 2

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")
	p4 := parsePoint("cpu,host=A value=1.5 1500000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	tx, _ := e.Begin(false)
	defer tx.Rollback()
	c := tx.Cursor("cpu,host=A", fields, nil, true)
	k, _ := c.SeekTo(0)
	if k != 1000000000 {
		t.Fatalf("expected time 1000000000 but got %d", k)
	}
	k, _ = c.Next()
	if k != 1500000000 {
		t.Fatalf("expected time 1500000000 but got %d", k)
	}
	k, _ = c.Next()
	if k != 2000000000 {
		t.Fatalf("expected time 2000000000 but got %d", k)
	}
	k, _ = c.Next()
	if k != 3000000000 {
		t.Fatalf("expected time 3000000000 but got %d", k)
	}
}

func TestEngine_WriteIntoCompactedFile(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	e.MaxPointsPerBlock = 3
	e.RotateFileSize = 10

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")
	p4 := parsePoint("cpu,host=A value=1.5 4000000000")
	p5 := parsePoint("cpu,host=A value=1.6 2500000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p5}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if count := e.DataFileCount(); count != 1 {
		t.Fatalf("expected 1 data file but got %d", count)
	}

	tx, _ := e.Begin(false)
	defer tx.Rollback()
	c := tx.Cursor("cpu,host=A", fields, nil, true)
	k, _ := c.SeekTo(0)
	if k != 1000000000 {
		t.Fatalf("wrong time: %d", k)
	}
	k, _ = c.Next()
	if k != 2000000000 {
		t.Fatalf("wrong time: %d", k)
	}
	k, _ = c.Next()
	if k != 2500000000 {
		t.Fatalf("wrong time: %d", k)
	}
	k, _ = c.Next()
	if k != 3000000000 {
		t.Fatalf("wrong time: %d", k)
	}
	k, _ = c.Next()
	if k != 4000000000 {
		t.Fatalf("wrong time: %d", k)
	}
}

func TestEngine_WriteIntoCompactedFile_MaxPointsPerBlockZero(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	e.MaxPointsPerBlock = 4
	e.RotateFileSize = 10

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")
	p4 := parsePoint("cpu,host=A value=1.5 4000000000")
	p5 := parsePoint("cpu,host=A value=1.6 2500000000")
	p6 := parsePoint("cpu,host=A value=1.7 5000000000")
	p7 := parsePoint("cpu,host=A value=1.8 6000000000")
	p8 := parsePoint("cpu,host=A value=1.9 7000000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p6, p7, p8}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p5}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if count := e.DataFileCount(); count != 1 {
		t.Fatalf("expected 1 data file but got %d", count)
	}

	tx, _ := e.Begin(false)
	defer tx.Rollback()
	c := tx.Cursor("cpu,host=A", fields, nil, true)
	k, _ := c.SeekTo(0)
	if k != 1000000000 {
		t.Fatalf("wrong time: %d", k)
	}
	k, _ = c.Next()
	if k != 2000000000 {
		t.Fatalf("wrong time: %d", k)
	}
	k, _ = c.Next()
	if k != 2500000000 {
		t.Fatalf("wrong time: %d", k)
	}
	k, _ = c.Next()
	if k != 3000000000 {
		t.Fatalf("wrong time: %d", k)
	}
	k, _ = c.Next()
	if k != 4000000000 {
		t.Fatalf("wrong time: %d", k)
	}
}

func TestEngine_DuplicatePointsInWalAndIndex(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}
	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 1000000000")
	if err := e.WritePoints([]models.Point{p1}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	e.WAL.SkipCache = false
	if err := e.WritePoints([]models.Point{p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	tx, _ := e.Begin(false)
	defer tx.Rollback()
	c := tx.Cursor("cpu,host=A", fields, nil, true)
	k, v := c.SeekTo(0)
	if k != 1000000000 {
		t.Fatalf("wrong time: %d", k)
	}
	if v != 1.2 {
		t.Fatalf("wrong value: %f", v.(float64))
	}
	k, _ = c.Next()
	if k != tsdb.EOF {
		t.Fatal("expected EOF", k)
	}
}

func TestEngine_Deletes(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}
	// Create metadata.
	mf := &tsdb.MeasurementFields{Fields: make(map[string]*tsdb.Field)}
	mf.CreateFieldIfNotExists("value", influxql.Float, false)
	atag := map[string]string{"host": "A"}
	btag := map[string]string{"host": "B"}
	seriesToCreate := []*tsdb.SeriesCreate{
		{Series: tsdb.NewSeries(string(models.MakeKey([]byte("cpu"), atag)), atag)},
		{Series: tsdb.NewSeries(string(models.MakeKey([]byte("cpu"), btag)), btag)},
	}

	p1 := parsePoint("cpu,host=A value=1.1 1000000001")
	p2 := parsePoint("cpu,host=A value=1.2 2000000001")
	p3 := parsePoint("cpu,host=B value=2.1 1000000000")
	p4 := parsePoint("cpu,host=B value=2.1 2000000000")

	e.SkipCompaction = true
	e.WAL.SkipCache = false

	if err := e.WritePoints([]models.Point{p1, p3}, map[string]*tsdb.MeasurementFields{"cpu": mf}, seriesToCreate); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=A", fields, nil, true)
		k, _ := c.SeekTo(0)
		if k != p1.UnixNano() {
			t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), k)
		}
	}()

	if err := e.DeleteSeries([]string{"cpu,host=A"}); err != nil {
		t.Fatalf("failed to delete series: %s", err.Error())
	}

	func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=B", fields, nil, true)
		k, _ := c.SeekTo(0)
		if k != p3.UnixNano() {
			t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), k)
		}
		c = tx.Cursor("cpu,host=A", fields, nil, true)
		k, _ = c.SeekTo(0)
		if k != tsdb.EOF {
			t.Fatal("expected EOF", k)
		}
	}()

	if err := e.WritePoints([]models.Point{p2, p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.WAL.Flush(); err != nil {
		t.Fatalf("error flushing wal: %s", err.Error())
	}

	func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=A", fields, nil, true)
		k, _ := c.SeekTo(0)
		if k != p2.UnixNano() {
			t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), k)
		}
	}()

	if err := e.DeleteSeries([]string{"cpu,host=A"}); err != nil {
		t.Fatalf("failed to delete series: %s", err.Error())
	}

	// we already know the delete on the wal works. open and close so
	// the wal flushes to the index. To verify that the delete gets
	// persisted and will go all the way through the index

	if err := e.Engine.Close(); err != nil {
		t.Fatalf("error closing: %s", err.Error())
	}
	if err := e.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}

	verify := func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=B", fields, nil, true)
		k, _ := c.SeekTo(0)
		if k != p3.UnixNano() {
			t.Fatalf("time wrong:\n\texp:%d\n\tgot:%d\n", p1.UnixNano(), k)
		}
		c = tx.Cursor("cpu,host=A", fields, nil, true)
		k, _ = c.SeekTo(0)
		if k != tsdb.EOF {
			t.Fatal("expected EOF")
		}
	}

	fmt.Println("verify 1")
	verify()

	// open and close to verify thd delete was persisted
	if err := e.Engine.Close(); err != nil {
		t.Fatalf("error closing: %s", err.Error())
	}
	if err := e.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}

	fmt.Println("verify 2")
	verify()

	if err := e.DeleteSeries([]string{"cpu,host=B"}); err != nil {
		t.Fatalf("failed to delete series: %s", err.Error())
	}

	func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=B", fields, nil, true)
		k, _ := c.SeekTo(0)
		if k != tsdb.EOF {
			t.Fatal("expected EOF")
		}
	}()

	if err := e.WAL.Flush(); err != nil {
		t.Fatalf("error flushing: %s", err.Error())
	}

	func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=B", fields, nil, true)
		k, _ := c.SeekTo(0)
		if k != tsdb.EOF {
			t.Fatal("expected EOF")
		}
	}()

	// open and close to verify thd delete was persisted
	if err := e.Engine.Close(); err != nil {
		t.Fatalf("error closing: %s", err.Error())
	}
	if err := e.Open(); err != nil {
		t.Fatalf("error opening: %s", err.Error())
	}

	func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=B", fields, nil, true)
		k, _ := c.SeekTo(0)
		if k != tsdb.EOF {
			t.Fatal("expected EOF")
		}
	}()
}

func TestEngine_IndexGoodAfterFlush(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	fields := []string{"value"}

	p1 := parsePoint("test,tag=a value=2.5 1443916800000000000")
	p2 := parsePoint("test value=3.5 1443916810000000000")
	p3 := parsePoint("test,tag=b value=6.5 1443916860000000000")
	p4 := parsePoint("test value=8.5 1443916861000000000")

	e.SkipCompaction = true
	e.WAL.SkipCache = false

	for _, p := range []models.Point{p1, p2, p3, p4} {
		if err := e.WritePoints([]models.Point{p}, nil, nil); err != nil {
			t.Fatalf("failed to write points: %s", err.Error())
		}
	}

	verify := func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c1 := tx.Cursor("test", fields, nil, true)
		c2 := tx.Cursor("test,tag=a", fields, nil, true)
		c3 := tx.Cursor("test,tag=b", fields, nil, true)
		k, v := c1.SeekTo(1443916800000000001)
		if k != p2.UnixNano() {
			t.Fatalf("time wrong: %d", k)
		}
		if v != 3.5 {
			t.Fatalf("value wrong: %f", v.(float64))
		}
		k, v = c1.Next()
		if k != p4.UnixNano() {
			t.Fatalf("time wrong: %d", k)
		}
		if v != 8.5 {
			t.Fatalf("value wrong: %f", v.(float64))
		}
		if k, _ := c1.Next(); k != tsdb.EOF {
			t.Fatalf("expected EOF: %d", k)
		}
		k, _ = c2.SeekTo(1443916800000000001)
		if k != tsdb.EOF {
			t.Fatalf("time wrong: %d", k)
		}
		k, v = c3.SeekTo(1443916800000000001)
		if k != p3.UnixNano() {
			t.Fatalf("time wrong: %d", k)
		}
		if v != 6.5 {
			t.Fatalf("value wrong: %f", v.(float64))
		}
		if k, _ := c3.Next(); k != tsdb.EOF {
			t.Fatalf("expected EOF: %d", k)
		}
	}

	fmt.Println("verify1")
	verify()
	fmt.Println("flush")
	if err := e.WAL.Flush(); err != nil {
		t.Fatalf("error flushing: %s", err.Error())
	}
	fmt.Println("verify2")
	verify()
}

// Ensure that when rewriting an index file with values in a
// series not in the file doesn't cause corruption on compaction
func TestEngine_RewriteFileAndCompact(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Engine.Close()

	fields := []string{"value"}

	e.RotateFileSize = 10

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")
	p4 := parsePoint("cpu,host=A value=1.5 4000000000")
	p5 := parsePoint("cpu,host=A value=1.6 5000000000")
	p6 := parsePoint("cpu,host=B value=2.1 2000000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p4, p5, p6}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}

	func() {
		tx, _ := e.Begin(false)
		defer tx.Rollback()
		c := tx.Cursor("cpu,host=A", fields, nil, true)
		k, _ := c.SeekTo(0)
		if k != p1.UnixNano() {
			t.Fatalf("wrong time %d", k)
		}
		c = tx.Cursor("cpu,host=B", fields, nil, true)
		k, _ = c.SeekTo(0)
		if k != p6.UnixNano() {
			t.Fatalf("wrong time %d", k)
		}
	}()
}

func TestEngine_DecodeAndCombine_NoNewValues(t *testing.T) {
	var newValues tsm1.Values
	e := OpenDefaultEngine()
	defer e.Engine.Close()

	values := make(tsm1.Values, 1)
	values[0] = tsm1.NewValue(time.Unix(0, 0), float64(1))

	block, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	remaining, encoded, err := e.DecodeAndCombine(newValues, block, nil, time.Unix(1, 0).UnixNano(), false)
	if len(remaining) != 0 {
		t.Fatalf("unexpected remaining values: exp %v, got %v", 0, len(remaining))
	}

	if len(encoded) != len(block) {
		t.Fatalf("unexpected encoded block length: exp %v, got %v", len(block), len(encoded))
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEngine_Write_Concurrent(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Engine.Close()

	values1 := make(tsm1.Values, 1)
	values1[0] = tsm1.NewValue(time.Unix(0, 0), float64(1))

	pointsByKey1 := map[string]tsm1.Values{
		"foo": values1,
	}

	values2 := make(tsm1.Values, 1)
	values2[0] = tsm1.NewValue(time.Unix(10, 0), float64(1))

	pointsByKey2 := map[string]tsm1.Values{
		"foo": values2,
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			e.Write(pointsByKey1, nil, nil)
			wg.Done()
		}()
		wg.Add(1)
		go func() {
			e.Write(pointsByKey2, nil, nil)
			wg.Done()
		}()
	}
	wg.Wait()
}

// Ensure the index won't compact files that would cause the
// resulting file to be larger than the max file size
func TestEngine_IndexFileSizeLimitedDuringCompaction(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	e.RotateFileSize = 10
	e.MaxFileSize = 100
	e.MaxPointsPerBlock = 3

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")
	p4 := parsePoint("cpu,host=A value=1.5 4000000000")
	p5 := parsePoint("cpu,host=A value=1.6 5000000000")
	p6 := parsePoint("cpu,host=B value=2.1 2000000000")

	if err := e.WritePoints([]models.Point{p1, p2}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p4, p5, p6}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if count := e.DataFileCount(); count != 3 {
		t.Fatalf("execpted 3 data file but got %d", count)
	}

	if err := checkPoints(e, "cpu,host=A", []models.Point{p1, p2, p3, p4, p5}); err != nil {
		t.Fatal(err.Error())
	}
	if err := checkPoints(e, "cpu,host=B", []models.Point{p6}); err != nil {
		t.Fatal(err.Error())
	}

	if err := e.Compact(true); err != nil {
		t.Fatalf("error compacting: %s", err.Error())
	}

	if err := checkPoints(e, "cpu,host=A", []models.Point{p1, p2, p3, p4, p5}); err != nil {
		t.Fatal(err.Error())
	}
	if err := checkPoints(e, "cpu,host=B", []models.Point{p6}); err != nil {
		t.Fatal(err.Error())
	}

	if count := e.DataFileCount(); count != 2 {
		t.Fatalf("expected 1 data file but got %d", count)
	}
}

// Ensure the index will split a large data file in two if a write
// will cause the resulting data file to be larger than the max file
// size limit
func TestEngine_IndexFilesSplitOnWriteToLargeFile(t *testing.T) {
}

// checkPoints will ensure that the engine has the points passed in the block
// along with checking that seeks in the middle work and an EOF is hit at the end
func checkPoints(e *Engine, key string, points []models.Point) error {
	tx, _ := e.Begin(false)
	defer tx.Rollback()
	c := tx.Cursor(key, []string{"value"}, nil, true)
	k, v := c.SeekTo(0)
	if k != points[0].UnixNano() {
		return fmt.Errorf("wrong time:\n\texp: %d\n\tgot: %d", points[0].UnixNano(), k)
	}
	if got := points[0].Fields()["value"]; v != got {
		return fmt.Errorf("wrong value:\n\texp: %v\n\tgot: %v", v, got)
	}
	points = points[1:]
	for _, p := range points {
		k, v = c.Next()
		if k != p.UnixNano() {
			return fmt.Errorf("wrong time:\n\texp: %d\n\tgot: %d", p.UnixNano(), k)
		}
		if got := p.Fields()["value"]; v != got {
			return fmt.Errorf("wrong value:\n\texp: %v\n\tgot: %v", v, got)
		}
	}
	k, _ = c.Next()
	if k != tsdb.EOF {
		return fmt.Errorf("expected EOF but got: %d", k)
	}

	// TODO: seek tests

	return nil
}

// Engine represents a test wrapper for tsm1.Engine.
type Engine struct {
	*tsm1.Engine
}

// NewEngine returns a new instance of Engine.
func NewEngine(opt tsdb.EngineOptions) *Engine {
	dir, err := ioutil.TempDir("", "tsm1-test")
	if err != nil {
		panic("couldn't get temp dir")
	}

	// Create test wrapper and attach mocks.
	e := &Engine{
		Engine: tsm1.NewEngine(dir, dir, opt).(*tsm1.Engine),
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

// Close closes the engine and removes all data.
func (e *Engine) Close() error {
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
