package v1_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/v1"
)

// Ensure the engine can write duplicate points to the WAL and cache and retrieve them correctly.
func TestEngine_Cursor_Duplicate(t *testing.T) {
	e := OpenDefaultEngine()
	defer e.Close()

	// Write point.
	if err := e.WritePoints([]tsdb.Point{
		tsdb.NewPoint("cpu", tsdb.Tags{}, tsdb.Fields{"value": 100}, time.Unix(0, 1)),
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Flush to disk.
	if err := e.Flush(0); err != nil {
		t.Fatal(err)
	}

	// Write point again.
	if err := e.WritePoints([]tsdb.Point{
		tsdb.NewPoint("cpu", tsdb.Tags{}, tsdb.Fields{"value": 100}, time.Unix(0, 1)),
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Iterate over "cpu" series.
	if keys, err := e.ReadSeriesPointKeys(`cpu`, tsdb.Tags{}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(keys, [][]byte{
		{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, // Unix(0, 1)
	}) {
		t.Fatalf("unexpected series point keys: %v", keys)
	}
}

// Engine represents a test wrapper for v1.Engine.
type Engine struct {
	*v1.Engine
}

// NewEngine returns a new instance of Engine.
func NewEngine(opt tsdb.EngineOptions) *Engine {
	// Generate temporary file.
	f, _ := ioutil.TempFile("", "v1-")
	f.Close()
	os.Remove(f.Name())

	// Return test wrapper.
	return &Engine{Engine: v1.NewEngine(f.Name(), opt).(*v1.Engine)}
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
