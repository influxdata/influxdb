package tsm1

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
)

// Ensure an engine containing cached values responds correctly to queries.
func TestDevEngine_CacheQuery_Ascending(t *testing.T) {
	// Generate temporary file.
	f, _ := ioutil.TempFile("", "tsm1dev")
	f.Close()
	os.Remove(f.Name())
	walPath := filepath.Join(f.Name(), "wal")
	os.MkdirAll(walPath, 0777)
	defer os.RemoveAll(f.Name())

	// Create a few points.
	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")

	// Write those points to the engine.
	e := NewDevEngine(f.Name(), walPath, tsdb.NewEngineOptions())
	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1dev engine: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p1, p2, p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Start a query transactions and get a cursor.
	tx := devTx{engine: e.(*DevEngine)}
	ascCursor := tx.Cursor("cpu,host=A", []string{"value"}, nil, true)

	k, v := ascCursor.SeekTo(1)
	if k != 1000000000 {
		t.Fatalf("failed to seek to before first key: %v %v", k, v)
	}

	k, v = ascCursor.SeekTo(1000000000)
	if k != 1000000000 {
		t.Fatalf("failed to seek to first key: %v %v", k, v)
	}

	k, v = ascCursor.Next()
	if k != 2000000000 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = ascCursor.Next()
	if k != 3000000000 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = ascCursor.Next()
	if k != -1 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = ascCursor.SeekTo(4000000000)
	if k != -1 {
		t.Fatalf("failed to seek to past last key: %v %v", k, v)
	}
}

// Ensure an engine containing cached values responds correctly to queries.
func TestDevEngine_TSMQuery_Ascending(t *testing.T) {
	fs := NewFileStore("")

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu,host=A#!~#value", []Value{NewValue(time.Unix(1, 0), 1.0)}},
		keyValues{"cpu,host=A#!~#value", []Value{NewValue(time.Unix(2, 0), 2.0)}},
		keyValues{"cpu,host=A#!~#value", []Value{NewValue(time.Unix(3, 0), 3.0)}},
	}

	files, err := newFiles(data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Add(files...)

	// Start a query transactions and get a cursor.
	ascCursor := devCursor{
		tsm:       fs,
		series:    "cpu,host=A",
		fields:    []string{"value"},
		ascending: true,
	}

	k, v := ascCursor.SeekTo(1)
	if k != 1000000000 {
		t.Fatalf("failed to seek to before first key: %v %v", k, v)
	}

	k, v = ascCursor.SeekTo(1000000000)
	if k != 1000000000 {
		t.Fatalf("failed to seek to first key: %v %v", k, v)
	}

	k, v = ascCursor.Next()
	if k != 2000000000 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = ascCursor.Next()
	if k != 3000000000 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = ascCursor.Next()
	if k != -1 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = ascCursor.SeekTo(4000000000)
	if k != -1 {
		t.Fatalf("failed to seek to past last key: %v %v", k, v)
	}
}

// Ensure an engine containing cached values responds correctly to queries.
func TestDevEngine_CacheQuery_Descending(t *testing.T) {
	t.Skip("fixme")
	// Generate temporary file.
	f, _ := ioutil.TempFile("", "tsm1dev")
	f.Close()
	os.Remove(f.Name())
	walPath := filepath.Join(f.Name(), "wal")
	os.MkdirAll(walPath, 0777)
	defer os.RemoveAll(f.Name())

	// Create a few points.
	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")

	// Write those points to the engine.
	e := NewDevEngine(f.Name(), walPath, tsdb.NewEngineOptions())
	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1dev engine: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p1, p2, p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Start a query transactions and get a cursor.
	tx := devTx{engine: e.(*DevEngine)}
	descCursor := tx.Cursor("cpu,host=A", []string{"value"}, nil, false)

	k, v := descCursor.SeekTo(4000000000)
	if k != 3000000000 {
		t.Fatalf("failed to seek to before last key: %v %v", k, v)
	}

	k, v = descCursor.Next()
	if k != 2000000000 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = descCursor.SeekTo(1)
	if k != -1 {
		t.Fatalf("failed to seek to after first key: %v %v", k, v)
	}
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

func newFiles(values ...keyValues) ([]TSMFile, error) {
	var files []TSMFile

	for _, v := range values {
		var b bytes.Buffer
		w, err := NewTSMWriter(&b)
		if err != nil {
			return nil, err
		}

		if err := w.Write(v.key, v.values); err != nil {
			return nil, err
		}

		if err := w.WriteIndex(); err != nil {
			return nil, err
		}

		r, err := NewTSMReader(bytes.NewReader(b.Bytes()))
		if err != nil {
			return nil, err
		}
		files = append(files, r)
	}
	return files, nil
}

type keyValues struct {
	key    string
	values []Value
}
