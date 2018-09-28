package tsdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/tsdb"
	_ "github.com/influxdata/platform/tsdb/tsm1"
)

func TestShardWriteAndIndex(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()

	sh := tsdb.NewShard(1, tmpShard, sfile.SeriesFile, opts)

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
		models.Tags{{Key: []byte("host"), Value: []byte("server")}},
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
		cnt := sh.SeriesN()
		if got, exp := cnt, int64(1); got != exp {
			t.Fatalf("got %v series, exp %v series in index", got, exp)
		}
	}

	validateIndex()

	// ensure the index gets loaded after closing and opening the shard
	sh.Close()

	sh = tsdb.NewShard(1, tmpShard, sfile.SeriesFile, opts)
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

func TestWriteTimeTag(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()

	sh := tsdb.NewShard(1, tmpShard, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{}),
		map[string]interface{}{"time": 1.0},
		time.Unix(1, 2),
	)

	if err := sh.WritePoints([]models.Point{pt}); err == nil {
		t.Fatal("expected error: got nil")
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{}),
		map[string]interface{}{"value": 1.0, "time": 1.0},
		time.Unix(1, 2),
	)

	if err := sh.WritePoints([]models.Point{pt}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

}

func TestWriteTimeField(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()

	sh := tsdb.NewShard(1, tmpShard, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"time": "now"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := sh.WritePoints([]models.Point{pt}); err == nil {
		t.Fatal("expected error: got nil")
	}

}

func TestShardWriteAddNewField(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()

	sh := tsdb.NewShard(1, tmpShard, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := sh.WritePoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0, "value2": 2.0},
		time.Unix(1, 2),
	)

	err = sh.WritePoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := sh.SeriesN(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

// Ensures that when a shard is closed, it removes any series meta-data
// from the index.
func TestShard_Close_RemoveIndex(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()

	sh := tsdb.NewShard(1, tmpShard, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := sh.WritePoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := sh.SeriesN(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

	// ensure the index gets loaded after closing and opening the shard
	sh.Close()
	sh.Open()

	if got, exp := sh.SeriesN(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

func BenchmarkWritePoints_NewSeries_1K(b *testing.B)   { benchmarkWritePoints(b, 38, 3, 3, 1) }
func BenchmarkWritePoints_NewSeries_100K(b *testing.B) { benchmarkWritePoints(b, 32, 5, 5, 1) }
func BenchmarkWritePoints_NewSeries_250K(b *testing.B) { benchmarkWritePoints(b, 80, 5, 5, 1) }
func BenchmarkWritePoints_NewSeries_500K(b *testing.B) { benchmarkWritePoints(b, 160, 5, 5, 1) }
func BenchmarkWritePoints_NewSeries_1M(b *testing.B)   { benchmarkWritePoints(b, 320, 5, 5, 1) }

// Fix measurement and tag key cardinalities and vary tag value cardinality
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_100_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 100, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_500_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 500, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_1000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 1000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_5000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 5000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_10000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 10000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_50000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 50000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_100000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 100000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_500000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 500000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_1000000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 1000000, 1)
}

// Fix tag key and tag values cardinalities and vary measurement cardinality
func BenchmarkWritePoints_NewSeries_100_Measurements_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 100, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_500_Measurements_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 500, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1000, 1, 1, 1)
}

func BenchmarkWritePoints_NewSeries_5000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 5000, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_10000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 10000, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_50000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 50000, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_100000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 100000, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_500000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 500000, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1000000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1000000, 1, 1, 1)
}

// Fix measurement and tag values cardinalities and vary tag key cardinality
func BenchmarkWritePoints_NewSeries_1_Measurement_2_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurements_4_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<2, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurements_8_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<3, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_16_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<4, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_32_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<5, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_64_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<6, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_128_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<7, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_256_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<8, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_512_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<9, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1024_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<10, 1, 1)
}

// Fix series cardinality and vary tag keys and value cardinalities
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_65536_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 1<<16, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_2_TagKeys_256_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 2, 1<<8, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_4_TagKeys_16_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 4, 1<<4, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_8_TagKeys_4_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 8, 1<<2, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_16_TagKeys_2_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 16, 1<<1, 1)
}

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
	// Generate point data to write to the shard.
	points := []models.Point{}
	for _, s := range series {
		for val := 0.0; val < float64(pntCnt); val++ {
			p := models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": val}, time.Now())
			points = append(points, p)
		}
	}

	// Stop & reset timers and mem-stats before the main benchmark loop.
	b.StopTimer()
	b.ResetTimer()

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Run the benchmark loop.
	for n := 0; n < b.N; n++ {
		tmpDir, _ := ioutil.TempDir("", "shard_test")
		tmpShard := filepath.Join(tmpDir, "shard")

		opts := tsdb.NewEngineOptions()

		shard := tsdb.NewShard(1, tmpShard, sfile.SeriesFile, opts)
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
	// Generate point data to write to the shard.
	points := []models.Point{}
	for _, s := range series {
		for val := 0.0; val < float64(pntCnt); val++ {
			p := models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": val}, time.Now())
			points = append(points, p)
		}
	}

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	tmpDir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")

	opts := tsdb.NewEngineOptions()

	shard := tsdb.NewShard(1, tmpShard, sfile.SeriesFile, opts)
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
	sfile *SeriesFile
	path  string
}

type Shards []*Shard

// NewShard returns a new instance of Shard with temp paths.
func NewShard() *Shard {
	return NewShards(1)[0]
}

// MustNewOpenShard creates and opens a shard with the provided index.
func MustNewOpenShard() *Shard {
	sh := NewShard()
	if err := sh.Open(); err != nil {
		panic(err)
	}
	return sh
}

// Close closes the shard and removes all underlying data.
func (sh *Shard) Close() error {
	// Will remove temp series file data.
	if err := sh.sfile.Close(); err != nil {
		return err
	}

	defer os.RemoveAll(sh.path)
	return sh.Shard.Close()
}

// NewShards create several shards all sharing the same
func NewShards(n int) Shards {
	// Create temporary path for data and WAL.
	dir, err := ioutil.TempDir("", "influxdb-tsdb-")
	if err != nil {
		panic(err)
	}

	sfile := MustOpenSeriesFile()

	var shards []*Shard
	var idSets []*tsdb.SeriesIDSet
	for i := 0; i < n; i++ {
		idSets = append(idSets, tsdb.NewSeriesIDSet())
	}

	for i := 0; i < n; i++ {
		// Build engine options.
		opt := tsdb.NewEngineOptions()

		// Initialise series id sets. Need to do this as it's normally done at the
		// store level.
		opt.SeriesIDSets = seriesIDSets(idSets)

		sh := &Shard{
			Shard: tsdb.NewShard(uint64(i),
				filepath.Join(dir, "data", "db0", "rp0", fmt.Sprint(i)),
				sfile.SeriesFile,
				opt,
			),
			sfile: sfile,
			path:  dir,
		}

		shards = append(shards, sh)
	}
	return Shards(shards)
}

// Open opens all the underlying shards.
func (a Shards) Open() error {
	for _, sh := range a {
		if err := sh.Open(); err != nil {
			return err
		}
	}
	return nil
}

// MustOpen opens all the shards, panicking if an error is encountered.
func (a Shards) MustOpen() {
	if err := a.Open(); err != nil {
		panic(err)
	}
}

// Close closes all shards and removes all underlying data.
func (a Shards) Close() error {
	if len(a) == 1 {
		return a[0].Close()
	}

	// Will remove temp series file data.
	if err := a[0].sfile.Close(); err != nil {
		return err
	}

	defer os.RemoveAll(a[0].path)
	for _, sh := range a {
		if err := sh.Shard.Close(); err != nil {
			return err
		}
	}
	return nil
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

func MustTempDir() (string, func()) {
	dir, err := ioutil.TempDir("", "shard-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir, func() { os.RemoveAll(dir) }
}

type seriesIterator struct {
	keys [][]byte
}

type series struct {
	name    []byte
	tags    models.Tags
	deleted bool
}

func (s series) Name() []byte        { return s.name }
func (s series) Tags() models.Tags   { return s.tags }
func (s series) Deleted() bool       { return s.deleted }
func (s series) Expr() influxql.Expr { return nil }

func (itr *seriesIterator) Close() error { return nil }

func (itr *seriesIterator) Next() (tsdb.SeriesElem, error) {
	if len(itr.keys) == 0 {
		return nil, nil
	}
	name, tags := models.ParseKeyBytes(itr.keys[0])
	s := series{name: name, tags: tags}
	itr.keys = itr.keys[1:]
	return s, nil
}

type seriesIDSets []*tsdb.SeriesIDSet

func (a seriesIDSets) ForEach(f func(ids *tsdb.SeriesIDSet)) error {
	for _, v := range a {
		f(v)
	}
	return nil
}
