package storage_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/storage"
	"github.com/influxdata/platform/tsdb"
)

func TestEngine_WriteAndIndex(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()

	// Calling WritePoints when the engine is not open will return
	// ErrEngineClosed.
	if got, exp := engine.Write1xPoints(nil), storage.ErrEngineClosed; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.Tags{{Key: []byte("host"), Value: []byte("server")}},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Write1xPoints([]models.Point{pt}); err != nil {
		t.Fatal(err)
	}

	pt.SetTime(time.Unix(2, 3))
	if err := engine.Write1xPoints([]models.Point{pt}); err != nil {
		t.Fatal(err)
	}

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %v series, exp %v series in index", got, exp)
	}

	// ensure the index gets loaded after closing and opening the shard
	engine.Engine.Close() // Don't remove the data
	engine.MustOpen()

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %v series, exp %v series in index", got, exp)
	}

	// and ensure that we can still write data
	pt.SetTime(time.Unix(2, 6))
	if err := engine.Write1xPoints([]models.Point{pt}); err != nil {
		t.Fatal(err)
	}
}

func TestEngine_TimeTag(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"time": "value"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Write1xPoints([]models.Point{pt}); err == nil {
		t.Fatal("expected error: got nil")
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"foo": "bar", "time": "value"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Write1xPoints([]models.Point{pt}); err == nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWrite_TimeField(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{}),
		map[string]interface{}{"time": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Write1xPoints([]models.Point{pt}); err == nil {
		t.Fatal("expected error: got nil")
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{}),
		map[string]interface{}{"value": 1.1, "time": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Write1xPoints([]models.Point{pt}); err == nil {
		t.Fatal("expected error: got nil")
	}
}

func TestEngine_WriteAddNewField(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := engine.Write1xPoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0, "value2": 2.0},
		time.Unix(1, 2),
	)

	err = engine.Write1xPoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := engine.SeriesCardinality(), int64(2); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

// Ensures that when a shard is closed, it removes any series meta-data
// from the index.
func TestEngineClose_RemoveIndex(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := engine.Write1xPoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

	// ensure the index gets loaded after closing and opening the shard
	engine.Engine.Close() // Don't destroy temporary data.
	engine.Open()

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

type Engine struct {
	path string
	*storage.Engine
}

// NewEngine create a new wrapper around a storage engine.
func NewEngine(c storage.Config) *Engine {
	path, _ := ioutil.TempDir("", "storage_engine_test")

	// TODO(edd) clean this up...
	c.EngineOptions.Config = c.Config

	engine := storage.NewEngine(path, c)
	return &Engine{
		path:   path,
		Engine: engine,
	}
}

// NewDefaultEngine returns a new Engine with a default configuration.
func NewDefaultEngine() *Engine {
	return NewEngine(storage.NewConfig())
}

// MustOpen opens the engine or panicks.
func (e *Engine) MustOpen() {
	if err := e.Engine.Open(); err != nil {
		panic(err)
	}
}

// Write1xPoints converts old style points into the new 2.0 engine format.
// This allows us to use the old `models` package helper functions and still write
// the points in the correct format.
func (e *Engine) Write1xPoints(pts []models.Point) error {
	org, _ := platform.IDFromString("3131313131313131")
	bucket, _ := platform.IDFromString("3232323232323232")
	points, err := tsdb.ExplodePoints(*org, *bucket, pts)
	if err != nil {
		return err
	}
	return e.Engine.WritePoints(points)
}

// Close closes the engine and removes all temporary data.
func (e *Engine) Close() error {
	defer os.RemoveAll(e.path)
	return e.Engine.Close()
}
