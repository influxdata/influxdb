package tsm1

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/lifecycle"
	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
)

// Test that series id set gets updated and returned appropriately.
func TestIndex_SeriesIDSet(t *testing.T) {
	engine := MustOpenEngine()
	defer engine.Close()

	// Add some series.
	engine.MustAddSeries("cpu", map[string]string{"host": "a", "region": "west"})
	engine.MustAddSeries("cpu", map[string]string{"host": "b", "region": "west"})
	engine.MustAddSeries("cpu", map[string]string{"host": "b"})
	engine.MustAddSeries("gpu", nil)
	engine.MustAddSeries("gpu", map[string]string{"host": "b"})
	engine.MustAddSeries("mem", map[string]string{"host": "z"})

	// Collect series IDs.
	seriesIDMap := map[string]tsdb.SeriesID{}
	var e tsdb.SeriesIDElem
	var err error

	itr := engine.sfile.SeriesIDIterator()
	for e, err = itr.Next(); ; e, err = itr.Next() {
		if err != nil {
			t.Fatal(err)
		} else if e.SeriesID.IsZero() {
			break
		}

		name, tags := tsdb.ParseSeriesKey(engine.sfile.SeriesKey(e.SeriesID))
		key := fmt.Sprintf("%s%s", name, tags.HashKey())
		seriesIDMap[key] = e.SeriesID
	}

	for _, id := range seriesIDMap {
		if !engine.SeriesIDSet().Contains(id) {
			t.Fatalf("bitmap does not contain ID: %d", id)
		}
	}

	// Drop all the series for the gpu measurement and they should no longer
	// be in the series ID set. This relies on the fact that DeleteBucketRange is really
	// operating on prefixes.
	if err := engine.DeleteBucketRange([]byte("gpu"), math.MinInt64, math.MaxInt64); err != nil {
		t.Fatal(err)
	}

	if engine.SeriesIDSet().Contains(seriesIDMap["gpu"]) {
		t.Fatalf("bitmap does not contain ID: %d for key %s, but should", seriesIDMap["gpu"], "gpu")
	} else if engine.SeriesIDSet().Contains(seriesIDMap["gpu,host=b"]) {
		t.Fatalf("bitmap does not contain ID: %d for key %s, but should", seriesIDMap["gpu,host=b"], "gpu,host=b")
	}
	delete(seriesIDMap, "gpu")
	delete(seriesIDMap, "gpu,host=b")

	// The rest of the keys should still be in the set.
	for key, id := range seriesIDMap {
		if !engine.SeriesIDSet().Contains(id) {
			t.Fatalf("bitmap does not contain ID: %d for key %s, but should", id, key)
		}
	}

	// Reopen the engine, and the series should be re-added to the bitmap.
	if err := engine.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Check bitset is expected.
	expected := tsdb.NewSeriesIDSet()
	for _, id := range seriesIDMap {
		expected.Add(id)
	}

	if !engine.SeriesIDSet().Equals(expected) {
		t.Fatalf("got bitset %s, expected %s", engine.SeriesIDSet().String(), expected.String())
	}
}

// TestEngine_ShouldCompactCache makes sure that shouldCompactCache acts according to
// the configuration.
func TestEngine_ShouldCompactCache(t *testing.T) {
	nowTime := time.Now()

	e := MustOpenEngine()
	defer e.Close()

	e.config.Cache.SnapshotMemorySize = 1024
	e.config.Cache.SnapshotWriteColdDuration = toml.Duration(time.Minute)

	if e.shouldCompactCache(nowTime) {
		t.Fatal("nothing written to cache, so should not compact")
	}

	if err := e.WritePointsString("m,k=v f=3i"); err != nil {
		t.Fatal(err)
	}

	if e.shouldCompactCache(nowTime) {
		t.Fatal("cache size < flush threshold and nothing written to FileStore, so should not compact")
	}

	if !e.shouldCompactCache(nowTime.Add(time.Hour)) {
		t.Fatal("last compaction was longer than flush write cold threshold, so should compact")
	}

	e.config.Cache.SnapshotMemorySize = 1
	if !e.shouldCompactCache(nowTime) {
		t.Fatal("cache size > flush threshold, so should compact")
	}
}

// BenchmarkEngine_WritePoints reports how quickly we can write points for different
// batch sizes.
func BenchmarkEngine_WritePoints(b *testing.B) {
	batchSizes := []int{10, 100, 1000, 5000, 10000}
	for _, sz := range batchSizes {
		e := MustOpenEngine()
		pp := make([]models.Point, 0, sz)
		for i := 0; i < sz; i++ {
			p := MustParsePointString(fmt.Sprintf("cpu,host=%d value=1.2", i))
			pp = append(pp, p)
		}

		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				err := e.WritePoints(pp)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		e.Close()
	}
}

// BenchmarkEngine_WritePoints_Parallel reports how quickly we can write points
// for different batch sizes and under different concurrent loads.
func BenchmarkEngine_WritePoints_Parallel(b *testing.B) {
	batchSizes := []int{1000, 5000, 10000, 25000, 50000, 75000, 100000, 200000}
	for _, sz := range batchSizes {
		e := MustOpenEngine()

		cpus := runtime.GOMAXPROCS(0)
		pp := make([]models.Point, 0, sz*cpus)
		for i := 0; i < sz*cpus; i++ {
			p := MustParsePointString(fmt.Sprintf("cpu,host=%d value=1.2,other=%di", i, i))
			pp = append(pp, p)
		}

		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				errC := make(chan error)
				for i := 0; i < cpus; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						from, to := i*sz, (i+1)*sz
						err := e.WritePoints(pp[from:to])
						if err != nil {
							errC <- err
							return
						}
					}(i)
				}

				go func() {
					wg.Wait()
					close(errC)
				}()

				for err := range errC {
					if err != nil {
						b.Error(err)
					}
				}
			}
		})
		e.Close()
	}
}

//
// helpers
//

// tempDir wraps a temporary directory to make it easy to clean up
// and join paths to it.
type tempDir struct {
	dir string
}

// newTempDir makes a new temporary directory.
func newTempDir() (*tempDir, error) {
	dir, err := ioutil.TempDir("", "tsm1-")
	if err != nil {
		return nil, err
	}
	return &tempDir{dir: dir}, nil
}

// Dir returns the current temporary directory.
func (t *tempDir) Dir() string { return t.dir }

// Join is a helper to join a directory to the temporary directory.
func (t *tempDir) Join(dir string) string { return filepath.Join(t.dir, dir) }

// Close removes the temporary directory.
func (t *tempDir) Close() (err error) {
	if t.dir == "" {
		err = os.RemoveAll(t.dir)
		t.dir = ""
	}
	return err
}

// testEngine is a test wrapper for an Engine.
type testEngine struct {
	dir     *tempDir
	options []EngineOption

	*Engine
	index *tsi1.Index
	sfile *tsdb.SeriesFile
}

// newTestEngine returns a new instance of Engine at a temporary location.
func newTestEngine(options ...EngineOption) (*testEngine, error) {
	dir, err := newTempDir()
	if err != nil {
		return nil, err
	}

	baseOptions := []EngineOption{
		WithCompactionPlanner(newMockPlanner()),
	}

	e := new(testEngine)
	if err := e.init(dir, append(baseOptions, options...)); err != nil {
		dir.Close()
		return nil, err
	}

	return e, nil
}

// init sets the fields on the testEngine based on the
func (e *testEngine) init(dir *tempDir, options []EngineOption) error {
	var opener lifecycle.Opener

	sfile := tsdb.NewSeriesFile(dir.Join("_series"))
	opener.Open(sfile)

	index := tsi1.NewIndex(sfile, tsi1.NewConfig(), tsi1.WithPath(dir.Join("index")))
	opener.Open(index)

	if err := opener.Done(); err != nil {
		return err
	}

	e.dir = dir
	e.options = options
	e.sfile = sfile
	e.index = index
	e.Engine = NewEngine(dir.Join("data"), index, NewConfig(), options...)
	return nil
}

// MustOpenEngine returns a new, open instance of Engine.
func MustOpenEngine(options ...EngineOption) *testEngine {
	e, err := newTestEngine(options...)
	if err != nil {
		panic(err)
	}

	if err := e.Engine.Open(); err != nil {
		e.Close()
		panic(err)
	}

	return e
}

// Close closes the engine and removes all underlying data.
func (e *testEngine) Close() error {
	return e.close(true)
}

// close closes the engine and removes all of the underlying data if
// cleanup is true.
func (e *testEngine) close(cleanup bool) error {
	var closer lifecycle.Closer

	if e.index != nil {
		closer.Close(e.index)
		e.index = nil
	}

	if e.sfile != nil {
		closer.Close(e.sfile)
		e.sfile = nil
	}

	if e.Engine != nil {
		closer.Close(e.Engine)
		e.Engine = nil
	}

	if cleanup && e.dir != nil {
		closer.Close(e.dir)
		e.dir = nil
	}

	return closer.Done()
}

// Reopen closes and reopens the engine.
func (e *testEngine) Reopen() error {
	// Close engine without removing underlying engine data.
	if err := e.close(false); err != nil {
		return err
	}

	// Initialize the fields on the testEngine again.
	if err := e.init(e.dir, e.options); err != nil {
		return err
	}

	// Open the engine and clean up if that fails.
	if err := e.Engine.Open(); err != nil {
		e.close(true)
		return err
	}

	return nil
}

// SeriesIDSet provides access to the underlying series id bitset in the engine's
// index. It will panic if the underlying index does not have a SeriesIDSet
// method.
func (e *testEngine) SeriesIDSet() *tsdb.SeriesIDSet {
	return e.index.SeriesIDSet()
}

// AddSeries adds the provided series data to the index and writes a point to
// the engine with default values for a field and a time of now.
func (e *testEngine) AddSeries(name string, tags map[string]string) error {
	point, err := models.NewPoint(name, models.NewTags(tags), models.Fields{"v": 1.0}, time.Now())
	if err != nil {
		return err
	}
	return e.writePoints(point)
}

// WritePointsString calls WritePointsString on the underlying engine, but also
// adds the associated series to the index.
func (e *testEngine) WritePointsString(ptstr ...string) error {
	points, err := models.ParsePointsString(strings.Join(ptstr, "\n"))
	if err != nil {
		return err
	}
	return e.writePoints(points...)
}

// writePoints adds the series for the provided points to the index, and writes
// the point data to the engine.
func (e *testEngine) writePoints(points ...models.Point) error {
	// Write into the index.
	collection := tsdb.NewSeriesCollection(points)
	if err := e.index.CreateSeriesListIfNotExists(collection); err != nil {
		return err
	}
	// Write the points into the cache/wal.
	return e.WritePoints(points)
}

// MustAddSeries calls AddSeries, panicking if there is an error.
func (e *testEngine) MustAddSeries(name string, tags map[string]string) {
	if err := e.AddSeries(name, tags); err != nil {
		panic(err)
	}
}

// MustWriteSnapshot forces a snapshot of the engine. Panic on error.
func (e *testEngine) MustWriteSnapshot() {
	if err := e.WriteSnapshot(); err != nil {
		panic(err)
	}
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

// mockPlanner is a CompactionPlanner that never compacts anything.
type mockPlanner struct{}

func newMockPlanner() CompactionPlanner { return &mockPlanner{} }

func (m *mockPlanner) Plan(lastWrite time.Time) []CompactionGroup { return nil }
func (m *mockPlanner) PlanLevel(level int) []CompactionGroup      { return nil }
func (m *mockPlanner) PlanOptimize() []CompactionGroup            { return nil }
func (m *mockPlanner) Release(groups []CompactionGroup)           {}
func (m *mockPlanner) FullyCompacted() bool                       { return false }
func (m *mockPlanner) ForceFull()                                 {}
func (m *mockPlanner) SetFileStore(fs *FileStore)                 {}
