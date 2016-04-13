package tsdb_test

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/influxdata/influxdb/tsdb"
	_ "github.com/influxdata/influxdb/tsdb/engine"
)

// DefaultPrecision is the precision used by the MustWritePointsString() function.
const DefaultPrecision = "s"

func TestShardWriteAndIndex(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	index := tsdb.NewDatabaseIndex("db")
	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")

	sh := tsdb.NewShard(1, index, tmpShard, tmpWal, opts)

	// Calling WritePoints when the engine is not open will return
	// ErrEngineClosed.
	if got, exp := sh.WritePoints(nil), tsdb.ErrEngineClosed; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	pt := models.MustNewPoint(
		"cpu",
		map[string]string{"host": "server"},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := sh.WritePoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	pt.SetTime(time.Unix(2, 3))
	err = sh.WritePoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	validateIndex := func() {
		if index.SeriesN() != 1 {
			t.Fatalf("series wasn't in index")
		}

		seriesTags := index.Series(string(pt.Key())).Tags
		if len(seriesTags) != len(pt.Tags()) || pt.Tags()["host"] != seriesTags["host"] {
			t.Fatalf("tags weren't properly saved to series index: %v, %v", pt.Tags(), seriesTags)
		}
		if !reflect.DeepEqual(index.Measurement("cpu").TagKeys(), []string{"host"}) {
			t.Fatalf("tag key wasn't saved to measurement index")
		}
	}

	validateIndex()

	// ensure the index gets loaded after closing and opening the shard
	sh.Close()

	index = tsdb.NewDatabaseIndex("db")
	sh = tsdb.NewShard(1, index, tmpShard, tmpWal, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	validateIndex()

	// and ensure that we can still write data
	pt.SetTime(time.Unix(2, 6))
	err = sh.WritePoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestShardWriteAddNewField(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	index := tsdb.NewDatabaseIndex("db")
	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")

	sh := tsdb.NewShard(1, index, tmpShard, tmpWal, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	pt := models.MustNewPoint(
		"cpu",
		map[string]string{"host": "server"},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := sh.WritePoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	pt = models.MustNewPoint(
		"cpu",
		map[string]string{"host": "server"},
		map[string]interface{}{"value": 1.0, "value2": 2.0},
		time.Unix(1, 2),
	)

	err = sh.WritePoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	if index.SeriesN() != 1 {
		t.Fatalf("series wasn't in index")
	}
	seriesTags := index.Series(string(pt.Key())).Tags
	if len(seriesTags) != len(pt.Tags()) || pt.Tags()["host"] != seriesTags["host"] {
		t.Fatalf("tags weren't properly saved to series index: %v, %v", pt.Tags(), seriesTags)
	}
	if !reflect.DeepEqual(index.Measurement("cpu").TagKeys(), []string{"host"}) {
		t.Fatalf("tag key wasn't saved to measurement index")
	}

	if len(index.Measurement("cpu").FieldNames()) != 2 {
		t.Fatalf("field names wasn't saved to measurement index")
	}
}

// Ensure a shard can create iterators for its underlying data.
func TestShard_CreateIterator_Ascending(t *testing.T) {
	sh := NewShard()

	// Calling CreateIterator when the engine is not open will return
	// ErrEngineClosed.
	_, got := sh.CreateIterator(influxql.IteratorOptions{})
	if exp := tsdb.ErrEngineClosed; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	if err := sh.Open(); err != nil {
		t.Fatal(err)
	}
	defer sh.Close()

	sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

	// Create iterator.
	itr, err := sh.CreateIterator(influxql.IteratorOptions{
		Expr:       influxql.MustParseExpr(`value`),
		Aux:        []string{"val2"},
		Dimensions: []string{"host"},
		Sources:    []influxql.Source{&influxql.Measurement{Name: "cpu"}},
		Ascending:  true,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer itr.Close()
	fitr := itr.(influxql.FloatIterator)

	// Read values from iterator.
	if p := fitr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "cpu",
		Tags:  influxql.NewTags(map[string]string{"host": "serverA"}),
		Time:  time.Unix(0, 0).UnixNano(),
		Value: 100,
		Aux:   []interface{}{(*float64)(nil)},
	}) {
		t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
	}

	if p := fitr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "cpu",
		Tags:  influxql.NewTags(map[string]string{"host": "serverA"}),
		Time:  time.Unix(10, 0).UnixNano(),
		Value: 50,
		Aux:   []interface{}{float64(5)},
	}) {
		t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
	}

	if p := fitr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "cpu",
		Tags:  influxql.NewTags(map[string]string{"host": "serverB"}),
		Time:  time.Unix(0, 0).UnixNano(),
		Value: 25,
		Aux:   []interface{}{(*float64)(nil)},
	}) {
		t.Fatalf("unexpected point(2): %s", spew.Sdump(p))
	}
}

// Ensure a shard can create iterators for its underlying data.
func TestShard_CreateIterator_Descending(t *testing.T) {
	sh := NewShard()

	// Calling CreateIterator when the engine is not open will return
	// ErrEngineClosed.
	_, got := sh.CreateIterator(influxql.IteratorOptions{})
	if exp := tsdb.ErrEngineClosed; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	if err := sh.Open(); err != nil {
		t.Fatal(err)
	}
	defer sh.Close()

	sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

	// Create iterator.
	itr, err := sh.CreateIterator(influxql.IteratorOptions{
		Expr:       influxql.MustParseExpr(`value`),
		Aux:        []string{"val2"},
		Dimensions: []string{"host"},
		Sources:    []influxql.Source{&influxql.Measurement{Name: "cpu"}},
		Ascending:  false,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer itr.Close()
	fitr := itr.(influxql.FloatIterator)

	// Read values from iterator.
	if p := fitr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "cpu",
		Tags:  influxql.NewTags(map[string]string{"host": "serverB"}),
		Time:  time.Unix(0, 0).UnixNano(),
		Value: 25,
		Aux:   []interface{}{(*float64)(nil)},
	}) {
		t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
	}

	if p := fitr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "cpu",
		Tags:  influxql.NewTags(map[string]string{"host": "serverA"}),
		Time:  time.Unix(10, 0).UnixNano(),
		Value: 50,
		Aux:   []interface{}{float64(5)},
	}) {
		t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
	}

	if p := fitr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "cpu",
		Tags:  influxql.NewTags(map[string]string{"host": "serverA"}),
		Time:  time.Unix(0, 0).UnixNano(),
		Value: 100,
		Aux:   []interface{}{(*float64)(nil)},
	}) {
		t.Fatalf("unexpected point(2): %s", spew.Sdump(p))
	}
}

func BenchmarkWritePoints_NewSeries_1K(b *testing.B)   { benchmarkWritePoints(b, 38, 3, 3, 1) }
func BenchmarkWritePoints_NewSeries_100K(b *testing.B) { benchmarkWritePoints(b, 32, 5, 5, 1) }
func BenchmarkWritePoints_NewSeries_250K(b *testing.B) { benchmarkWritePoints(b, 80, 5, 5, 1) }
func BenchmarkWritePoints_NewSeries_500K(b *testing.B) { benchmarkWritePoints(b, 160, 5, 5, 1) }
func BenchmarkWritePoints_NewSeries_1M(b *testing.B)   { benchmarkWritePoints(b, 320, 5, 5, 1) }

func BenchmarkWritePoints_ExistingSeries_1K(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 38, 3, 3, 1)
}
func BenchmarkWritePoints_ExistingSeries_100K(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 32, 5, 5, 1)
}
func BenchmarkWritePoints_ExistingSeries_250K(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 80, 5, 5, 1)
}
func BenchmarkWritePoints_ExistingSeries_500K(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 160, 5, 5, 1)
}
func BenchmarkWritePoints_ExistingSeries_1M(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 320, 5, 5, 1)
}

// benchmarkWritePoints benchmarks writing new series to a shard.
// mCnt - measurement count
// tkCnt - tag key count
// tvCnt - tag value count (values per tag)
// pntCnt - points per series.  # of series = mCnt * (tvCnt ^ tkCnt)
func benchmarkWritePoints(b *testing.B, mCnt, tkCnt, tvCnt, pntCnt int) {
	// Generate test series (measurements + unique tag sets).
	series := genTestSeries(mCnt, tkCnt, tvCnt)
	// Create index for the shard to use.
	index := tsdb.NewDatabaseIndex("db")
	// Generate point data to write to the shard.
	points := []models.Point{}
	for _, s := range series {
		for val := 0.0; val < float64(pntCnt); val++ {
			p := models.MustNewPoint(s.Measurement, s.Series.Tags, map[string]interface{}{"value": val}, time.Now())
			points = append(points, p)
		}
	}

	// Stop & reset timers and mem-stats before the main benchmark loop.
	b.StopTimer()
	b.ResetTimer()

	// Run the benchmark loop.
	for n := 0; n < b.N; n++ {
		tmpDir, _ := ioutil.TempDir("", "shard_test")
		tmpShard := path.Join(tmpDir, "shard")
		tmpWal := path.Join(tmpDir, "wal")
		shard := tsdb.NewShard(1, index, tmpShard, tmpWal, tsdb.NewEngineOptions())
		shard.Open()

		b.StartTimer()
		// Call the function being benchmarked.
		chunkedWrite(shard, points)

		b.StopTimer()
		shard.Close()
		os.RemoveAll(tmpDir)
	}
}

// benchmarkWritePointsExistingSeries benchmarks writing to existing series in a shard.
// mCnt - measurement count
// tkCnt - tag key count
// tvCnt - tag value count (values per tag)
// pntCnt - points per series.  # of series = mCnt * (tvCnt ^ tkCnt)
func benchmarkWritePointsExistingSeries(b *testing.B, mCnt, tkCnt, tvCnt, pntCnt int) {
	// Generate test series (measurements + unique tag sets).
	series := genTestSeries(mCnt, tkCnt, tvCnt)
	// Create index for the shard to use.
	index := tsdb.NewDatabaseIndex("db")
	// Generate point data to write to the shard.
	points := []models.Point{}
	for _, s := range series {
		for val := 0.0; val < float64(pntCnt); val++ {
			p := models.MustNewPoint(s.Measurement, s.Series.Tags, map[string]interface{}{"value": val}, time.Now())
			points = append(points, p)
		}
	}

	tmpDir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")
	shard := tsdb.NewShard(1, index, tmpShard, tmpWal, tsdb.NewEngineOptions())
	shard.Open()
	defer shard.Close()
	chunkedWrite(shard, points)

	// Reset timers and mem-stats before the main benchmark loop.
	b.ResetTimer()

	// Run the benchmark loop.
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		for _, p := range points {
			p.SetTime(p.Time().Add(time.Second))
		}

		b.StartTimer()
		// Call the function being benchmarked.
		chunkedWrite(shard, points)
	}
}

func chunkedWrite(shard *tsdb.Shard, points []models.Point) {
	nPts := len(points)
	chunkSz := 10000
	start := 0
	end := chunkSz

	for {
		if end > nPts {
			end = nPts
		}
		if end-start == 0 {
			break
		}

		shard.WritePoints(points[start:end])
		start = end
		end += chunkSz
	}
}

// Shard represents a test wrapper for tsdb.Shard.
type Shard struct {
	*tsdb.Shard
	path string
}

// NewShard returns a new instance of Shard with temp paths.
func NewShard() *Shard {
	// Create temporary path for data and WAL.
	path, err := ioutil.TempDir("", "influxdb-tsdb-")
	if err != nil {
		panic(err)
	}

	// Build engine options.
	opt := tsdb.NewEngineOptions()
	opt.Config.WALDir = filepath.Join(path, "wal")

	return &Shard{
		Shard: tsdb.NewShard(0,
			tsdb.NewDatabaseIndex("db"),
			filepath.Join(path, "data"),
			filepath.Join(path, "wal"),
			opt,
		),
		path: path,
	}
}

// MustOpenShard returns a new open shard. Panic on error.
func MustOpenShard() *Shard {
	sh := NewShard()
	if err := sh.Open(); err != nil {
		panic(err)
	}
	return sh
}

// Close closes the shard and removes all underlying data.
func (sh *Shard) Close() error {
	defer os.RemoveAll(sh.path)
	return sh.Shard.Close()
}

// MustWritePointsString parses the line protocol (with second precision) and
// inserts the resulting points into the shard. Panic on error.
func (sh *Shard) MustWritePointsString(s string) {
	a, err := models.ParsePointsWithPrecision([]byte(strings.TrimSpace(s)), time.Time{}, "s")
	if err != nil {
		panic(err)
	}

	if err := sh.WritePoints(a); err != nil {
		panic(err)
	}
}
