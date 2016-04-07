package tsm1_test

import (
	"archive/tar"
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Ensure engine can load the metadata index after reopening.
func TestEngine_LoadMetadataIndex(t *testing.T) {
	e := MustOpenEngine()
	defer e.Close()

	if err := e.WritePointsString(`cpu,host=A value=1.1 1000000000`); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Ensure we can close and load index from the WAL
	if err := e.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Load metadata index.
	index := tsdb.NewDatabaseIndex("db")
	if err := e.LoadMetadataIndex(nil, index); err != nil {
		t.Fatal(err)
	}

	// Verify index is correct.
	if m := index.Measurement("cpu"); m == nil {
		t.Fatal("measurement not found")
	} else if s := m.SeriesByID(1); s.Key != "cpu,host=A" || !reflect.DeepEqual(s.Tags, map[string]string{"host": "A"}) {
		t.Fatalf("unexpected series: %q / %#v", s.Key, s.Tags)
	}

	// write the snapshot, ensure we can close and load index from TSM
	if err := e.WriteSnapshot(); err != nil {
		t.Fatalf("error writing snapshot: %s", err.Error())
	}

	// Ensure we can close and load index from the WAL
	if err := e.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Load metadata index.
	index = tsdb.NewDatabaseIndex("db")
	if err := e.LoadMetadataIndex(nil, index); err != nil {
		t.Fatal(err)
	}

	// Verify index is correct.
	if m := index.Measurement("cpu"); m == nil {
		t.Fatal("measurement not found")
	} else if s := m.SeriesByID(1); s.Key != "cpu,host=A" || !reflect.DeepEqual(s.Tags, map[string]string{"host": "A"}) {
		t.Fatalf("unexpected series: %q / %#v", s.Key, s.Tags)
	}

	// Write a new point and ensure we can close and load index from TSM and WAL
	if err := e.WritePoints([]models.Point{
		MustParsePointString("cpu,host=B value=1.2 2000000000"),
	}); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Ensure we can close and load index from the TSM & WAL
	if err := e.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Load metadata index.
	index = tsdb.NewDatabaseIndex("db")
	if err := e.LoadMetadataIndex(nil, index); err != nil {
		t.Fatal(err)
	}

	// Verify index is correct.
	if m := index.Measurement("cpu"); m == nil {
		t.Fatal("measurement not found")
	} else if s := m.SeriesByID(1); s.Key != "cpu,host=A" || !reflect.DeepEqual(s.Tags, map[string]string{"host": "A"}) {
		t.Fatalf("unexpected series: %q / %#v", s.Key, s.Tags)
	} else if s := m.SeriesByID(2); s.Key != "cpu,host=B" || !reflect.DeepEqual(s.Tags, map[string]string{"host": "B"}) {
		t.Fatalf("unexpected series: %q / %#v", s.Key, s.Tags)
	}
}

// Ensure that deletes only sent to the WAL will clear out the data from the cache on restart
func TestEngine_DeleteWALLoadMetadata(t *testing.T) {
	e := MustOpenEngine()
	defer e.Close()

	if err := e.WritePointsString(
		`cpu,host=A value=1.1 1000000000`,
		`cpu,host=B value=1.2 2000000000`,
	); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Remove series.
	if err := e.DeleteSeries([]string{"cpu,host=A"}); err != nil {
		t.Fatalf("failed to delete series: %s", err.Error())
	}

	// Ensure we can close and load index from the WAL
	if err := e.Reopen(); err != nil {
		t.Fatal(err)
	}

	if exp, got := 0, len(e.Cache.Values(tsm1.SeriesFieldKey("cpu,host=A", "value"))); exp != got {
		t.Fatalf("unexpected number of values: got: %d. exp: %d", got, exp)
	}

	if exp, got := 1, len(e.Cache.Values(tsm1.SeriesFieldKey("cpu,host=B", "value"))); exp != got {
		t.Fatalf("unexpected number of values: got: %d. exp: %d", got, exp)
	}
}

// Ensure that the engine will backup any TSM files created since the passed in time
func TestEngine_Backup(t *testing.T) {
	// Generate temporary file.
	f, _ := ioutil.TempFile("", "tsm")
	f.Close()
	os.Remove(f.Name())
	walPath := filepath.Join(f.Name(), "wal")
	os.MkdirAll(walPath, 0777)
	defer os.RemoveAll(f.Name())

	// Create a few points.
	p1 := MustParsePointString("cpu,host=A value=1.1 1000000000")
	p2 := MustParsePointString("cpu,host=B value=1.2 2000000000")
	p3 := MustParsePointString("cpu,host=C value=1.3 3000000000")

	// Write those points to the engine.
	e := tsm1.NewEngine(f.Name(), walPath, tsdb.NewEngineOptions()).(*tsm1.Engine)

	// mock the planner so compactions don't run during the test
	e.CompactionPlan = &mockPlanner{}

	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1 engine: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p1}); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WriteSnapshot(); err != nil {
		t.Fatalf("failed to snapshot: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p2}); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	b := bytes.NewBuffer(nil)
	if err := e.Backup(b, "", time.Unix(0, 0)); err != nil {
		t.Fatalf("failed to backup: %s", err.Error())
	}

	tr := tar.NewReader(b)
	if len(e.FileStore.Files()) != 2 {
		t.Fatalf("file count wrong: exp: %d, got: %d", 2, len(e.FileStore.Files()))
	}

	for _, f := range e.FileStore.Files() {
		th, err := tr.Next()
		if err != nil {
			t.Fatalf("failed reading header: %s", err.Error())
		}
		if !strings.Contains(f.Path(), th.Name) || th.Name == "" {
			t.Fatalf("file name doesn't match:\n\tgot: %s\n\texp: %s", th.Name, f.Path())
		}
	}

	lastBackup := time.Now()

	// we have to sleep for a second because last modified times only have second level precision.
	// so this test won't work properly unless the file is at least a second past the last one
	time.Sleep(time.Second)

	if err := e.WritePoints([]models.Point{p3}); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	b = bytes.NewBuffer(nil)
	if err := e.Backup(b, "", lastBackup); err != nil {
		t.Fatalf("failed to backup: %s", err.Error())
	}

	tr = tar.NewReader(b)
	th, err := tr.Next()
	if err != nil {
		t.Fatalf("error getting next tar header: %s", err.Error())
	}

	mostRecentFile := e.FileStore.Files()[e.FileStore.Count()-1].Path()
	if !strings.Contains(mostRecentFile, th.Name) || th.Name == "" {
		t.Fatalf("file name doesn't match:\n\tgot: %s\n\texp: %s", th.Name, mostRecentFile)
	}
}

// Ensure engine can create an ascending iterator for cached values.
func TestEngine_CreateIterator_Cache_Ascending(t *testing.T) {
	t.Parallel()

	e := MustOpenEngine()
	defer e.Close()

	e.Index().CreateMeasurementIndexIfNotExists("cpu")
	e.MeasurementFields("cpu").CreateFieldIfNotExists("value", influxql.Float, false)
	e.Index().CreateSeriesIndexIfNotExists("cpu", tsdb.NewSeries("cpu,host=A", map[string]string{"host": "A"}))
	if err := e.WritePointsString(
		`cpu,host=A value=1.1 1000000000`,
		`cpu,host=A value=1.2 2000000000`,
		`cpu,host=A value=1.3 3000000000`,
	); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	itr, err := e.CreateIterator(influxql.IteratorOptions{
		Expr:       influxql.MustParseExpr(`value`),
		Dimensions: []string{"host"},
		Sources:    []influxql.Source{&influxql.Measurement{Name: "cpu"}},
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	fitr := itr.(influxql.FloatIterator)

	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
		t.Fatalf("unexpected point(0): %v", p)
	}
	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2}) {
		t.Fatalf("unexpected point(1): %v", p)
	}
	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
		t.Fatalf("unexpected point(2): %v", p)
	}
	if p := fitr.Next(); p != nil {
		t.Fatalf("expected eof: %v", p)
	}
}

// Ensure engine can create an descending iterator for cached values.
func TestEngine_CreateIterator_Cache_Descending(t *testing.T) {
	t.Parallel()

	e := MustOpenEngine()
	defer e.Close()

	e.Index().CreateMeasurementIndexIfNotExists("cpu")
	e.MeasurementFields("cpu").CreateFieldIfNotExists("value", influxql.Float, false)
	e.Index().CreateSeriesIndexIfNotExists("cpu", tsdb.NewSeries("cpu,host=A", map[string]string{"host": "A"}))
	if err := e.WritePointsString(
		`cpu,host=A value=1.1 1000000000`,
		`cpu,host=A value=1.2 2000000000`,
		`cpu,host=A value=1.3 3000000000`,
	); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	itr, err := e.CreateIterator(influxql.IteratorOptions{
		Expr:       influxql.MustParseExpr(`value`),
		Dimensions: []string{"host"},
		Sources:    []influxql.Source{&influxql.Measurement{Name: "cpu"}},
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  false,
	})
	if err != nil {
		t.Fatal(err)
	}
	fitr := itr.(influxql.FloatIterator)

	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
		t.Fatalf("unexpected point(0): %v", p)
	}
	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2}) {
		t.Fatalf("unexpected point(1): %v", p)
	}
	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
		t.Fatalf("unexpected point(2): %v", p)
	}
	if p := fitr.Next(); p != nil {
		t.Fatalf("expected eof: %v", p)
	}
}

// Ensure engine can create an ascending iterator for tsm values.
func TestEngine_CreateIterator_TSM_Ascending(t *testing.T) {
	t.Parallel()

	e := MustOpenEngine()
	defer e.Close()

	e.Index().CreateMeasurementIndexIfNotExists("cpu")
	e.MeasurementFields("cpu").CreateFieldIfNotExists("value", influxql.Float, false)
	e.Index().CreateSeriesIndexIfNotExists("cpu", tsdb.NewSeries("cpu,host=A", map[string]string{"host": "A"}))
	if err := e.WritePointsString(
		`cpu,host=A value=1.1 1000000000`,
		`cpu,host=A value=1.2 2000000000`,
		`cpu,host=A value=1.3 3000000000`,
	); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	e.MustWriteSnapshot()

	itr, err := e.CreateIterator(influxql.IteratorOptions{
		Expr:       influxql.MustParseExpr(`value`),
		Dimensions: []string{"host"},
		Sources:    []influxql.Source{&influxql.Measurement{Name: "cpu"}},
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	fitr := itr.(influxql.FloatIterator)

	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
		t.Fatalf("unexpected point(0): %v", p)
	}
	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2}) {
		t.Fatalf("unexpected point(1): %v", p)
	}
	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
		t.Fatalf("unexpected point(2): %v", p)
	}
	if p := fitr.Next(); p != nil {
		t.Fatalf("expected eof: %v", p)
	}
}

// Ensure engine can create an descending iterator for cached values.
func TestEngine_CreateIterator_TSM_Descending(t *testing.T) {
	t.Parallel()

	e := MustOpenEngine()
	defer e.Close()

	e.Index().CreateMeasurementIndexIfNotExists("cpu")
	e.MeasurementFields("cpu").CreateFieldIfNotExists("value", influxql.Float, false)
	e.Index().CreateSeriesIndexIfNotExists("cpu", tsdb.NewSeries("cpu,host=A", map[string]string{"host": "A"}))
	if err := e.WritePointsString(
		`cpu,host=A value=1.1 1000000000`,
		`cpu,host=A value=1.2 2000000000`,
		`cpu,host=A value=1.3 3000000000`,
	); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	e.MustWriteSnapshot()

	itr, err := e.CreateIterator(influxql.IteratorOptions{
		Expr:       influxql.MustParseExpr(`value`),
		Dimensions: []string{"host"},
		Sources:    []influxql.Source{&influxql.Measurement{Name: "cpu"}},
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  false,
	})
	if err != nil {
		t.Fatal(err)
	}
	fitr := itr.(influxql.FloatIterator)

	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
		t.Fatalf("unexpected point(0): %v", p)
	}
	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2}) {
		t.Fatalf("unexpected point(1): %v", p)
	}
	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
		t.Fatalf("unexpected point(2): %v", p)
	}
	if p := fitr.Next(); p != nil {
		t.Fatalf("expected eof: %v", p)
	}
}

// Ensure engine can create an iterator with auxilary fields.
func TestEngine_CreateIterator_Aux(t *testing.T) {
	t.Parallel()

	e := MustOpenEngine()
	defer e.Close()

	e.Index().CreateMeasurementIndexIfNotExists("cpu")
	e.MeasurementFields("cpu").CreateFieldIfNotExists("value", influxql.Float, false)
	e.MeasurementFields("cpu").CreateFieldIfNotExists("F", influxql.Float, false)
	e.Index().CreateSeriesIndexIfNotExists("cpu", tsdb.NewSeries("cpu,host=A", map[string]string{"host": "A"}))
	if err := e.WritePointsString(
		`cpu,host=A value=1.1 1000000000`,
		`cpu,host=A F=100 1000000000`,
		`cpu,host=A value=1.2 2000000000`,
		`cpu,host=A value=1.3 3000000000`,
		`cpu,host=A F=200 3000000000`,
	); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	itr, err := e.CreateIterator(influxql.IteratorOptions{
		Expr:       influxql.MustParseExpr(`value`),
		Aux:        []string{"F"},
		Dimensions: []string{"host"},
		Sources:    []influxql.Source{&influxql.Measurement{Name: "cpu"}},
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	fitr := itr.(influxql.FloatIterator)

	if p := fitr.Next(); !deep.Equal(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1, Aux: []interface{}{float64(100)}}) {
		t.Fatalf("unexpected point(0): %v", p)
	}
	if p := fitr.Next(); !deep.Equal(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2, Aux: []interface{}{(*float64)(nil)}}) {
		t.Fatalf("unexpected point(1): %v", p)
	}
	if p := fitr.Next(); !deep.Equal(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3, Aux: []interface{}{float64(200)}}) {
		t.Fatalf("unexpected point(2): %v", p)
	}
	if p := fitr.Next(); p != nil {
		t.Fatalf("expected eof: %v", p)
	}
}

// Ensure engine can create an iterator with a condition.
func TestEngine_CreateIterator_Condition(t *testing.T) {
	t.Parallel()

	e := MustOpenEngine()
	defer e.Close()

	e.Index().CreateMeasurementIndexIfNotExists("cpu")
	e.Index().Measurement("cpu").SetFieldName("X")
	e.Index().Measurement("cpu").SetFieldName("Y")
	e.MeasurementFields("cpu").CreateFieldIfNotExists("value", influxql.Float, false)
	e.MeasurementFields("cpu").CreateFieldIfNotExists("X", influxql.Float, false)
	e.MeasurementFields("cpu").CreateFieldIfNotExists("Y", influxql.Float, false)
	e.Index().CreateSeriesIndexIfNotExists("cpu", tsdb.NewSeries("cpu,host=A", map[string]string{"host": "A"}))
	if err := e.WritePointsString(
		`cpu,host=A value=1.1 1000000000`,
		`cpu,host=A X=10 1000000000`,
		`cpu,host=A Y=100 1000000000`,

		`cpu,host=A value=1.2 2000000000`,

		`cpu,host=A value=1.3 3000000000`,
		`cpu,host=A X=20 3000000000`,
		`cpu,host=A Y=200 3000000000`,
	); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	itr, err := e.CreateIterator(influxql.IteratorOptions{
		Expr:       influxql.MustParseExpr(`value`),
		Dimensions: []string{"host"},
		Condition:  influxql.MustParseExpr(`X = 10 OR Y > 150`),
		Sources:    []influxql.Source{&influxql.Measurement{Name: "cpu"}},
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	fitr := itr.(influxql.FloatIterator)

	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
		t.Fatalf("unexpected point(0): %v", p)
	}
	if p := fitr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
		t.Fatalf("unexpected point(2): %v", p)
	}
	if p := fitr.Next(); p != nil {
		t.Fatalf("expected eof: %v", p)
	}
}

// Engine is a test wrapper for tsm1.Engine.
type Engine struct {
	*tsm1.Engine
	root string
}

// NewEngine returns a new instance of Engine at a temporary location.
func NewEngine() *Engine {
	root, err := ioutil.TempDir("", "tsm1-")
	if err != nil {
		panic(err)
	}
	return &Engine{
		Engine: tsm1.NewEngine(
			filepath.Join(root, "data"),
			filepath.Join(root, "wal"),
			tsdb.NewEngineOptions()).(*tsm1.Engine),
		root: root,
	}
}

// MustOpenEngine returns a new, open instance of Engine.
func MustOpenEngine() *Engine {
	e := NewEngine()
	if err := e.Open(); err != nil {
		panic(err)
	}
	if err := e.LoadMetadataIndex(nil, tsdb.NewDatabaseIndex("db")); err != nil {
		panic(err)
	}
	return e
}

// Close closes the engine and removes all underlying data.
func (e *Engine) Close() error {
	defer os.RemoveAll(e.root)
	return e.Engine.Close()
}

// Reopen closes and reopens the engine.
func (e *Engine) Reopen() error {
	if err := e.Engine.Close(); err != nil {
		return err
	}

	e.Engine = tsm1.NewEngine(
		filepath.Join(e.root, "data"),
		filepath.Join(e.root, "wal"),
		tsdb.NewEngineOptions()).(*tsm1.Engine)

	if err := e.Engine.Open(); err != nil {
		return err
	}
	return nil
}

// MustWriteSnapshot forces a snapshot of the engine. Panic on error.
func (e *Engine) MustWriteSnapshot() {
	if err := e.WriteSnapshot(); err != nil {
		panic(err)
	}
}

// WritePointsString parses a string buffer and writes the points.
func (e *Engine) WritePointsString(buf ...string) error {
	return e.WritePoints(MustParsePointsString(strings.Join(buf, "\n")))
}

// MustParsePointsString parses points from a string. Panic on error.
func MustParsePointsString(buf string) []models.Point {
	a, err := models.ParsePointsString(buf)
	if err != nil {
		panic(err)
	}
	return a
}

// MustParsePointString parses the first point from a string. Panic on error.
func MustParsePointString(buf string) models.Point { return MustParsePointsString(buf)[0] }

type mockPlanner struct{}

func (m *mockPlanner) Plan(lastWrite time.Time) []tsm1.CompactionGroup { return nil }
func (m *mockPlanner) PlanLevel(level int) []tsm1.CompactionGroup      { return nil }

// ParseTags returns an instance of Tags for a comma-delimited list of key/values.
func ParseTags(s string) influxql.Tags {
	m := make(map[string]string)
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m[a[0]] = a[1]
	}
	return influxql.NewTags(m)
}
