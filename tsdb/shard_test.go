package tsdb_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/internal"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	_ "github.com/influxdata/influxdb/tsdb/engine"
	_ "github.com/influxdata/influxdb/tsdb/index"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
	"github.com/influxdata/influxql"
)

func TestShardWriteAndIndex(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)

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

	sh = tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
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

func TestShard_Open_CorruptFieldsIndex(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)

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

	if err := sh.Close(); err != nil {
		t.Fatalf("close shard error: %v", err)
	}

	path := filepath.Join(tmpShard, "fields.idx")
	if err := os.Truncate(path, 6); err != nil {
		t.Fatalf("truncate shard error: %v", err)
	}

	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
}

func TestMaxSeriesLimit(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "db", "rp", "1")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.Config.MaxSeriesPerDatabase = 1000
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)

	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	// Writing 1K series should succeed.
	points := []models.Point{}

	for i := 0; i < 1000; i++ {
		pt := models.MustNewPoint(
			"cpu",
			models.Tags{{Key: []byte("host"), Value: []byte(fmt.Sprintf("server%d", i))}},
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		)
		points = append(points, pt)
	}

	err := sh.WritePoints(points)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Writing one more series should exceed the series limit.
	pt := models.MustNewPoint(
		"cpu",
		models.Tags{{Key: []byte("host"), Value: []byte("server9999")}},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err = sh.WritePoints([]models.Point{pt})
	if err == nil {
		t.Fatal("expected error")
	} else if exp, got := `partial write: max-series-per-database limit exceeded: (1000) dropped=1`, err.Error(); exp != got {
		t.Fatalf("unexpected error message:\n\texp = %s\n\tgot = %s", exp, got)
	}

	sh.Close()
}

func TestShard_MaxTagValuesLimit(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "db", "rp", "1")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.Config.MaxValuesPerTag = 1000
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)

	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	// Writing 1K series should succeed.
	points := []models.Point{}

	for i := 0; i < 1000; i++ {
		pt := models.MustNewPoint(
			"cpu",
			models.Tags{{Key: []byte("host"), Value: []byte(fmt.Sprintf("server%d", i))}},
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		)
		points = append(points, pt)
	}

	err := sh.WritePoints(points)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Writing one more series should exceed the series limit.
	pt := models.MustNewPoint(
		"cpu",
		models.Tags{{Key: []byte("host"), Value: []byte("server9999")}},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err = sh.WritePoints([]models.Point{pt})
	if err == nil {
		t.Fatal("expected error")
	} else if exp, got := `partial write: max-values-per-tag limit exceeded (1000/1000): measurement="cpu" tag="host" value="server9999" dropped=1`, err.Error(); exp != got {
		t.Fatalf("unexpected error message:\n\texp = %s\n\tgot = %s", exp, got)
	}

	sh.Close()
}

func TestWriteTimeTag(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
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

	mf := sh.MeasurementFields([]byte("cpu"))
	if mf == nil {
		t.Fatal("expected cpu measurement fields")
	}

	if got, exp := mf.FieldN(), 1; got != exp {
		t.Fatalf("invalid number of field names: got=%v exp=%v", got, exp)
	}
}

func TestWriteTimeField(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
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

	key := models.MakeKey([]byte("cpu"), nil)
	if ok, err := sh.MeasurementExists(key); ok && err == nil {
		t.Fatal("unexpected series")
	}
}

func TestShardWriteAddNewField(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
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

// Tests concurrently writing to the same shard with different field types which
// can trigger a panic when the shard is snapshotted to TSM files.
func TestShard_WritePoints_FieldConflictConcurrent(t *testing.T) {
	if testing.Short() || runtime.GOOS == "windows" {
		t.Skip("Skipping on short and windows")
	}
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)
	opts.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{})

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	points := make([]models.Point, 0, 1000)
	for i := 0; i < cap(points); i++ {
		if i < 500 {
			points = append(points, models.MustNewPoint(
				"cpu",
				models.NewTags(map[string]string{"host": "server"}),
				map[string]interface{}{"value": 1.0},
				time.Unix(int64(i), 0),
			))
		} else {
			points = append(points, models.MustNewPoint(
				"cpu",
				models.NewTags(map[string]string{"host": "server"}),
				map[string]interface{}{"value": int64(1)},
				time.Unix(int64(i), 0),
			))
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errC := make(chan error)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if err := sh.DeleteMeasurement([]byte("cpu")); err != nil {
				errC <- err
				return
			}

			_ = sh.WritePoints(points[:500])
			if f, err := sh.CreateSnapshot(); err == nil {
				os.RemoveAll(f)
			}

		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if err := sh.DeleteMeasurement([]byte("cpu")); err != nil {
				errC <- err
				return
			}

			_ = sh.WritePoints(points[500:])
			if f, err := sh.CreateSnapshot(); err == nil {
				os.RemoveAll(f)
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errC)
	}()

	for err := range errC {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestShard_WritePoints_FieldConflictConcurrentQuery(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)
	opts.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{})

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	// Spin up two goroutines that write points with different field types in reverse
	// order concurrently.  After writing them, query them back.
	errC := make(chan error, 2)
	go func() {
		// Write 250 floats and then ints to the same field
		points := make([]models.Point, 0, 500)
		for i := 0; i < cap(points); i++ {
			if i < 250 {
				points = append(points, models.MustNewPoint(
					"cpu",
					models.NewTags(map[string]string{"host": "server"}),
					map[string]interface{}{"value": 1.0},
					time.Unix(int64(i), 0),
				))
			} else {
				points = append(points, models.MustNewPoint(
					"cpu",
					models.NewTags(map[string]string{"host": "server"}),
					map[string]interface{}{"value": int64(1)},
					time.Unix(int64(i), 0),
				))
			}
		}

		for i := 0; i < 500; i++ {
			if err := sh.DeleteMeasurement([]byte("cpu")); err != nil {
				errC <- err
			}

			sh.WritePoints(points)
			m := &influxql.Measurement{Name: "cpu"}
			iter, err := sh.CreateIterator(context.Background(), m, query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Aux:        []influxql.VarRef{{Val: "value"}},
				Dimensions: []string{},
				Ascending:  true,
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
			})
			if err != nil {
				errC <- err
			}

			switch itr := iter.(type) {
			case query.IntegerIterator:
				p, err := itr.Next()
				for p != nil && err == nil {
					p, err = itr.Next()
				}
				iter.Close()

			case query.FloatIterator:
				p, err := itr.Next()
				for p != nil && err == nil {
					p, err = itr.Next()
				}
				iter.Close()

			}

		}
		errC <- nil
	}()

	go func() {
		// Write 250 ints and then floats to the same field
		points := make([]models.Point, 0, 500)
		for i := 0; i < cap(points); i++ {
			if i < 250 {
				points = append(points, models.MustNewPoint(
					"cpu",
					models.NewTags(map[string]string{"host": "server"}),
					map[string]interface{}{"value": int64(1)},
					time.Unix(int64(i), 0),
				))
			} else {
				points = append(points, models.MustNewPoint(
					"cpu",
					models.NewTags(map[string]string{"host": "server"}),
					map[string]interface{}{"value": 1.0},
					time.Unix(int64(i), 0),
				))
			}
		}
		for i := 0; i < 500; i++ {
			if err := sh.DeleteMeasurement([]byte("cpu")); err != nil {
				errC <- err
			}

			sh.WritePoints(points)
			m := &influxql.Measurement{Name: "cpu"}
			iter, err := sh.CreateIterator(context.Background(), m, query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Aux:        []influxql.VarRef{{Val: "value"}},
				Dimensions: []string{},
				Ascending:  true,
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
			})
			if err != nil {
				errC <- err
			}

			switch itr := iter.(type) {
			case query.IntegerIterator:
				p, err := itr.Next()
				for p != nil && err == nil {
					p, err = itr.Next()
				}
				iter.Close()
			case query.FloatIterator:
				p, err := itr.Next()
				for p != nil && err == nil {
					p, err = itr.Next()
				}
				iter.Close()
			}
		}
		errC <- nil
	}()

	// Check results
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			t.Fatal(err)
		}
	}
}

// Ensures that when a shard is closed, it removes any series meta-data
// from the index.
func TestShard_Close_RemoveIndex(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(path.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
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

// Ensure a shard can create iterators for its underlying data.
func TestShard_CreateIterator_Ascending(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			sh := NewShard(index)
			defer sh.Close()

			// Calling CreateIterator when the engine is not open will return
			// ErrEngineClosed.
			m := &influxql.Measurement{Name: "cpu"}
			_, got := sh.CreateIterator(context.Background(), m, query.IteratorOptions{})
			if exp := tsdb.ErrEngineClosed; got != exp {
				t.Fatalf("got %v, expected %v", got, exp)
			}

			if err := sh.Open(); err != nil {
				t.Fatal(err)
			}

			sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

			// Create iterator.
			var err error
			m = &influxql.Measurement{Name: "cpu"}
			itr, err := sh.CreateIterator(context.Background(), m, query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Aux:        []influxql.VarRef{{Val: "val2"}},
				Dimensions: []string{"host"},
				Ascending:  true,
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer itr.Close()
			fitr := itr.(query.FloatIterator)

			// Read values from iterator.
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(0): %s", err)
			} else if !deep.Equal(p, &query.FloatPoint{
				Name:  "cpu",
				Tags:  query.NewTags(map[string]string{"host": "serverA"}),
				Time:  time.Unix(0, 0).UnixNano(),
				Value: 100,
				Aux:   []interface{}{(*float64)(nil)},
			}) {
				t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
			}

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(1): %s", err)
			} else if !deep.Equal(p, &query.FloatPoint{
				Name:  "cpu",
				Tags:  query.NewTags(map[string]string{"host": "serverA"}),
				Time:  time.Unix(10, 0).UnixNano(),
				Value: 50,
				Aux:   []interface{}{float64(5)},
			}) {
				t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
			}

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(2): %s", err)
			} else if !deep.Equal(p, &query.FloatPoint{
				Name:  "cpu",
				Tags:  query.NewTags(map[string]string{"host": "serverB"}),
				Time:  time.Unix(0, 0).UnixNano(),
				Value: 25,
				Aux:   []interface{}{(*float64)(nil)},
			}) {
				t.Fatalf("unexpected point(2): %s", spew.Sdump(p))
			}
		})
	}
}

// Ensure a shard can create iterators for its underlying data.
func TestShard_CreateIterator_Descending(t *testing.T) {
	var sh *Shard
	var itr query.Iterator

	test := func(index string) {
		sh = NewShard(index)

		// Calling CreateIterator when the engine is not open will return
		// ErrEngineClosed.
		m := &influxql.Measurement{Name: "cpu"}
		_, got := sh.CreateIterator(context.Background(), m, query.IteratorOptions{})
		if exp := tsdb.ErrEngineClosed; got != exp {
			t.Fatalf("got %v, expected %v", got, exp)
		}

		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

		// Create iterator.
		var err error
		m = &influxql.Measurement{Name: "cpu"}
		itr, err = sh.CreateIterator(context.Background(), m, query.IteratorOptions{
			Expr:       influxql.MustParseExpr(`value`),
			Aux:        []influxql.VarRef{{Val: "val2"}},
			Dimensions: []string{"host"},
			Ascending:  false,
			StartTime:  influxql.MinTime,
			EndTime:    influxql.MaxTime,
		})
		if err != nil {
			t.Fatal(err)
		}
		fitr := itr.(query.FloatIterator)

		// Read values from iterator.
		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(0): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{
			Name:  "cpu",
			Tags:  query.NewTags(map[string]string{"host": "serverB"}),
			Time:  time.Unix(0, 0).UnixNano(),
			Value: 25,
			Aux:   []interface{}{(*float64)(nil)},
		}) {
			t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
		}

		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(1): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{
			Name:  "cpu",
			Tags:  query.NewTags(map[string]string{"host": "serverA"}),
			Time:  time.Unix(10, 0).UnixNano(),
			Value: 50,
			Aux:   []interface{}{float64(5)},
		}) {
			t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
		}

		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(2): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{
			Name:  "cpu",
			Tags:  query.NewTags(map[string]string{"host": "serverA"}),
			Time:  time.Unix(0, 0).UnixNano(),
			Value: 100,
			Aux:   []interface{}{(*float64)(nil)},
		}) {
			t.Fatalf("unexpected point(2): %s", spew.Sdump(p))
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
		sh.Close()
		itr.Close()
	}
}

func TestShard_CreateIterator_Series_Auth(t *testing.T) {
	type variant struct {
		name string
		m    *influxql.Measurement
		aux  []influxql.VarRef
	}

	examples := []variant{
		{
			name: "use_index",
			m:    &influxql.Measurement{Name: "cpu"},
			aux:  []influxql.VarRef{{Val: "_seriesKey", Type: influxql.String}},
		},
		{
			name: "use_cursors",
			m:    &influxql.Measurement{Name: "cpu", SystemIterator: "_series"},
			aux:  []influxql.VarRef{{Val: "key", Type: influxql.String}},
		},
	}

	test := func(index string, v variant) error {
		sh := MustNewOpenShard(index)
		defer sh.Close()
		sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
cpu,secret=foo value=100 0
`)

		seriesAuthorizer := &internal.AuthorizerMock{
			AuthorizeSeriesReadFn: func(database string, measurement []byte, tags models.Tags) bool {
				if database == "" || !bytes.Equal(measurement, []byte("cpu")) || tags.GetString("secret") != "" {
					t.Logf("Rejecting series db=%s, m=%s, tags=%v", database, measurement, tags)
					return false
				}
				return true
			},
		}

		// Create iterator for case where we use cursors (e.g., where time
		// included in a SHOW SERIES query).
		itr, err := sh.CreateIterator(context.Background(), v.m, query.IteratorOptions{
			Aux:        v.aux,
			Ascending:  true,
			StartTime:  influxql.MinTime,
			EndTime:    influxql.MaxTime,
			Authorizer: seriesAuthorizer,
		})
		if err != nil {
			return err
		}

		if itr == nil {
			return fmt.Errorf("iterator is nil")
		}
		defer itr.Close()

		fitr := itr.(query.FloatIterator)
		defer fitr.Close()
		var expCount = 2
		var gotCount int
		for {
			f, err := fitr.Next()
			if err != nil {
				return err
			}

			if f == nil {
				break
			}

			if got := f.Aux[0].(string); strings.Contains(got, "secret") {
				return fmt.Errorf("got a series %q that should be filtered", got)
			}
			gotCount++
		}

		if gotCount != expCount {
			return fmt.Errorf("got %d series, expected %d", gotCount, expCount)
		}

		// Delete series cpu,host=serverA,region=uswest
		//
		// We can't call directly on the index as we need to ensure the series
		// file is updated appropriately.
		sitr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=serverA,region=uswest")}}
		if err := sh.DeleteSeriesRange(sitr, math.MinInt64, math.MaxInt64); err != nil {
			t.Fatalf("failed to drop series: %s", err.Error())
		}

		if itr, err = sh.CreateIterator(context.Background(), v.m, query.IteratorOptions{
			Aux:        v.aux,
			Ascending:  true,
			StartTime:  influxql.MinTime,
			EndTime:    influxql.MaxTime,
			Authorizer: seriesAuthorizer,
		}); err != nil {
			return err
		}

		if itr == nil {
			return fmt.Errorf("iterator is nil")
		}
		defer itr.Close()

		fitr = itr.(query.FloatIterator)
		defer fitr.Close()
		expCount = 1
		gotCount = 0
		for {
			f, err := fitr.Next()
			if err != nil {
				return err
			}

			if f == nil {
				break
			}

			if got := f.Aux[0].(string); strings.Contains(got, "secret") {
				return fmt.Errorf("got a series %q that should be filtered", got)
			} else if got := f.Aux[0].(string); strings.Contains(got, "serverA") {
				return fmt.Errorf("got a series %q that should be filtered", got)
			}
			gotCount++
		}

		if gotCount != expCount {
			return fmt.Errorf("got %d series, expected %d", gotCount, expCount)
		}

		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		for _, example := range examples {
			t.Run(index+"_"+example.name, func(t *testing.T) {
				if err := test(index, example); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestShard_Disabled_WriteQuery(t *testing.T) {
	var sh *Shard

	test := func(index string) {
		sh = NewShard(index)
		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		sh.SetEnabled(false)

		pt := models.MustNewPoint(
			"cpu",
			models.NewTags(map[string]string{"host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		)

		err := sh.WritePoints([]models.Point{pt})
		if err == nil {
			t.Fatalf("expected shard disabled error")
		}
		if err != tsdb.ErrShardDisabled {
			t.Fatalf(err.Error())
		}
		m := &influxql.Measurement{Name: "cpu"}
		_, got := sh.CreateIterator(context.Background(), m, query.IteratorOptions{})
		if err == nil {
			t.Fatalf("expected shard disabled error")
		}
		if exp := tsdb.ErrShardDisabled; got != exp {
			t.Fatalf("got %v, expected %v", got, exp)
		}

		sh.SetEnabled(true)

		err = sh.WritePoints([]models.Point{pt})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m = &influxql.Measurement{Name: "cpu"}
		if _, err = sh.CreateIterator(context.Background(), m, query.IteratorOptions{}); err != nil {
			t.Fatalf("unexpected error: %v", got)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
		sh.Close()
	}
}

func TestShard_Closed_Functions(t *testing.T) {
	var sh *Shard
	test := func(index string) {
		sh = NewShard(index)
		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		pt := models.MustNewPoint(
			"cpu",
			models.NewTags(map[string]string{"host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		)

		if err := sh.WritePoints([]models.Point{pt}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sh.Close()

		// Should not panic.
		if exp, got := 0, sh.TagKeyCardinality([]byte("cpu"), []byte("host")); exp != got {
			t.Fatalf("got %d, expected %d", got, exp)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

func TestShard_FieldDimensions(t *testing.T) {
	var sh *Shard

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	setup := func(index string) {
		sh = NewShard(index)

		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
_reserved,region=uswest value="foo" 0
`)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		setup(index)
		for _, tt := range []struct {
			sources []string
			f       map[string]influxql.DataType
			d       map[string]struct{}
		}{
			{
				sources: []string{"cpu"},
				f: map[string]influxql.DataType{
					"value": influxql.Float,
					"val2":  influxql.Float,
				},
				d: map[string]struct{}{
					"host":   {},
					"region": {},
				},
			},
			{
				sources: []string{"mem"},
				f: map[string]influxql.DataType{
					"value": influxql.Integer,
					"val3":  influxql.Boolean,
				},
				d: map[string]struct{}{
					"host": {},
				},
			},
			{
				sources: []string{"cpu", "mem"},
				f: map[string]influxql.DataType{
					"value": influxql.Float,
					"val2":  influxql.Float,
					"val3":  influxql.Boolean,
				},
				d: map[string]struct{}{
					"host":   {},
					"region": {},
				},
			},
			{
				sources: []string{"_fieldKeys"},
				f: map[string]influxql.DataType{
					"fieldKey":  influxql.String,
					"fieldType": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_series"},
				f: map[string]influxql.DataType{
					"key": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_tagKeys"},
				f: map[string]influxql.DataType{
					"tagKey": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_reserved"},
				f: map[string]influxql.DataType{
					"value": influxql.String,
				},
				d: map[string]struct{}{
					"region": {},
				},
			},
			{
				sources: []string{"unknown"},
				f:       map[string]influxql.DataType{},
				d:       map[string]struct{}{},
			},
		} {
			name := fmt.Sprintf("%s_%s", strings.Join(tt.sources, ","), index)
			t.Run(name, func(t *testing.T) {
				f, d, err := sh.FieldDimensions(tt.sources)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if diff := cmp.Diff(tt.f, f, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected fields:\n%s", diff)
				}
				if diff := cmp.Diff(tt.d, d, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected dimensions:\n%s", diff)
				}
			})
		}
		sh.Close()
	}
}

func TestShards_FieldDimensions(t *testing.T) {
	var shard1, shard2 *Shard

	setup := func(index string) {
		shard1 = NewShard(index)
		if err := shard1.Open(); err != nil {
			t.Fatal(err)
		}

		shard1.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

		shard2 = NewShard(index)
		if err := shard2.Open(); err != nil {
			t.Fatal(err)
		}

		shard2.MustWritePointsString(`
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
_reserved,region=uswest value="foo" 0
`)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		setup(index)
		sh := tsdb.Shards([]*tsdb.Shard{shard1.Shard, shard2.Shard})
		for _, tt := range []struct {
			sources []string
			f       map[string]influxql.DataType
			d       map[string]struct{}
		}{
			{
				sources: []string{"cpu"},
				f: map[string]influxql.DataType{
					"value": influxql.Float,
					"val2":  influxql.Float,
				},
				d: map[string]struct{}{
					"host":   {},
					"region": {},
				},
			},
			{
				sources: []string{"mem"},
				f: map[string]influxql.DataType{
					"value": influxql.Integer,
					"val3":  influxql.Boolean,
				},
				d: map[string]struct{}{
					"host": {},
				},
			},
			{
				sources: []string{"cpu", "mem"},
				f: map[string]influxql.DataType{
					"value": influxql.Float,
					"val2":  influxql.Float,
					"val3":  influxql.Boolean,
				},
				d: map[string]struct{}{
					"host":   {},
					"region": {},
				},
			},
			{
				sources: []string{"_fieldKeys"},
				f: map[string]influxql.DataType{
					"fieldKey":  influxql.String,
					"fieldType": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_series"},
				f: map[string]influxql.DataType{
					"key": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_tagKeys"},
				f: map[string]influxql.DataType{
					"tagKey": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_reserved"},
				f: map[string]influxql.DataType{
					"value": influxql.String,
				},
				d: map[string]struct{}{
					"region": {},
				},
			},
			{
				sources: []string{"unknown"},
				f:       map[string]influxql.DataType{},
				d:       map[string]struct{}{},
			},
		} {
			name := fmt.Sprintf("%s_%s", index, strings.Join(tt.sources, ","))
			t.Run(name, func(t *testing.T) {
				f, d, err := sh.FieldDimensions(tt.sources)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if diff := cmp.Diff(tt.f, f, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected fields:\n%s", diff)
				}
				if diff := cmp.Diff(tt.d, d, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected dimensions:\n%s", diff)
				}
			})
		}
		shard1.Close()
		shard2.Close()
	}
}

func TestShards_MapType(t *testing.T) {
	var shard1, shard2 *Shard

	setup := func(index string) {
		shard1 = NewShard(index)
		if err := shard1.Open(); err != nil {
			t.Fatal(err)
		}

		shard1.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

		shard2 = NewShard(index)
		if err := shard2.Open(); err != nil {
			t.Fatal(err)
		}

		shard2.MustWritePointsString(`
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
_reserved,region=uswest value="foo" 0
`)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		setup(index)
		sh := tsdb.Shards([]*tsdb.Shard{shard1.Shard, shard2.Shard})
		for _, tt := range []struct {
			measurement string
			field       string
			typ         influxql.DataType
		}{
			{
				measurement: "cpu",
				field:       "value",
				typ:         influxql.Float,
			},
			{
				measurement: "cpu",
				field:       "host",
				typ:         influxql.Tag,
			},
			{
				measurement: "cpu",
				field:       "region",
				typ:         influxql.Tag,
			},
			{
				measurement: "cpu",
				field:       "val2",
				typ:         influxql.Float,
			},
			{
				measurement: "cpu",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "mem",
				field:       "value",
				typ:         influxql.Integer,
			},
			{
				measurement: "mem",
				field:       "val3",
				typ:         influxql.Boolean,
			},
			{
				measurement: "mem",
				field:       "host",
				typ:         influxql.Tag,
			},
			{
				measurement: "unknown",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_fieldKeys",
				field:       "fieldKey",
				typ:         influxql.String,
			},
			{
				measurement: "_fieldKeys",
				field:       "fieldType",
				typ:         influxql.String,
			},
			{
				measurement: "_fieldKeys",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_series",
				field:       "key",
				typ:         influxql.String,
			},
			{
				measurement: "_series",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_tagKeys",
				field:       "tagKey",
				typ:         influxql.String,
			},
			{
				measurement: "_tagKeys",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_reserved",
				field:       "value",
				typ:         influxql.String,
			},
			{
				measurement: "_reserved",
				field:       "region",
				typ:         influxql.Tag,
			},
		} {
			name := fmt.Sprintf("%s_%s_%s", index, tt.measurement, tt.field)
			t.Run(name, func(t *testing.T) {
				typ := sh.MapType(tt.measurement, tt.field)
				if have, want := typ, tt.typ; have != want {
					t.Errorf("unexpected data type: have=%#v want=%#v", have, want)
				}
			})
		}
		shard1.Close()
		shard2.Close()
	}
}

func TestShards_MeasurementsByRegex(t *testing.T) {
	var shard1, shard2 *Shard

	setup := func(index string) {
		shard1 = NewShard(index)
		if err := shard1.Open(); err != nil {
			t.Fatal(err)
		}

		shard1.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

		shard2 = NewShard(index)
		if err := shard2.Open(); err != nil {
			t.Fatal(err)
		}

		shard2.MustWritePointsString(`
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
_reserved,region=uswest value="foo" 0
`)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		setup(index)
		sh := tsdb.Shards([]*tsdb.Shard{shard1.Shard, shard2.Shard})
		for _, tt := range []struct {
			regex        string
			measurements []string
		}{
			{regex: `cpu`, measurements: []string{"cpu"}},
			{regex: `mem`, measurements: []string{"mem"}},
			{regex: `cpu|mem`, measurements: []string{"cpu", "mem"}},
			{regex: `gpu`, measurements: []string{}},
			{regex: `pu`, measurements: []string{"cpu"}},
			{regex: `p|m`, measurements: []string{"cpu", "mem"}},
		} {
			t.Run(tt.regex, func(t *testing.T) {
				re := regexp.MustCompile(tt.regex)
				measurements := sh.MeasurementsByRegex(re)
				sort.Strings(measurements)
				if diff := cmp.Diff(tt.measurements, measurements, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected measurements:\n%s", diff)
				}
			})
		}
		shard1.Close()
		shard2.Close()
	}
}

func TestMeasurementFieldSet_SaveLoad(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	path := filepath.Join(dir, "fields.idx")
	mf, err := tsdb.NewMeasurementFieldSet(path)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}

	fields := mf.CreateFieldsIfNotExists([]byte("cpu"))
	if err := fields.CreateFieldIfNotExists([]byte("value"), influxql.Float); err != nil {
		t.Fatalf("create field error: %v", err)
	}

	if err := mf.Save(); err != nil {
		t.Fatalf("save error: %v", err)
	}

	mf, err = tsdb.NewMeasurementFieldSet(path)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}

	fields = mf.Fields("cpu")
	field := fields.Field("value")
	if field == nil {
		t.Fatalf("field is null")
	}

	if got, exp := field.Type, influxql.Float; got != exp {
		t.Fatalf("field type mismatch: got %v, exp %v", got, exp)
	}
}

func TestMeasurementFieldSet_Corrupt(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	path := filepath.Join(dir, "fields.idx")
	mf, err := tsdb.NewMeasurementFieldSet(path)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}

	fields := mf.CreateFieldsIfNotExists([]byte("cpu"))
	if err := fields.CreateFieldIfNotExists([]byte("value"), influxql.Float); err != nil {
		t.Fatalf("create field error: %v", err)
	}

	if err := mf.Save(); err != nil {
		t.Fatalf("save error: %v", err)
	}

	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat error: %v", err)
	}

	// Truncate the file to simulate a a corrupted file
	if err := os.Truncate(path, stat.Size()-3); err != nil {
		t.Fatalf("truncate error: %v", err)
	}

	mf, err = tsdb.NewMeasurementFieldSet(path)
	if err == nil {
		t.Fatal("NewMeasurementFieldSet expected error")
	}

	fields = mf.Fields("cpu")
	if fields != nil {
		t.Fatal("expecte fields to be nil")
	}
}
func TestMeasurementFieldSet_DeleteEmpty(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	path := filepath.Join(dir, "fields.idx")
	mf, err := tsdb.NewMeasurementFieldSet(path)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}

	fields := mf.CreateFieldsIfNotExists([]byte("cpu"))
	if err := fields.CreateFieldIfNotExists([]byte("value"), influxql.Float); err != nil {
		t.Fatalf("create field error: %v", err)
	}

	if err := mf.Save(); err != nil {
		t.Fatalf("save error: %v", err)
	}

	mf, err = tsdb.NewMeasurementFieldSet(path)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}

	fields = mf.Fields("cpu")
	field := fields.Field("value")
	if field == nil {
		t.Fatalf("field is null")
	}

	if got, exp := field.Type, influxql.Float; got != exp {
		t.Fatalf("field type mismatch: got %v, exp %v", got, exp)
	}

	mf.Delete("cpu")

	if err := mf.Save(); err != nil {
		t.Fatalf("save after delete error: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("got %v, not exist err", err)
	}
}

func TestMeasurementFieldSet_InvalidFormat(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	path := filepath.Join(dir, "fields.idx")

	if err := ioutil.WriteFile(path, []byte{0, 0}, 0666); err != nil {
		t.Fatalf("error writing fields.index: %v", err)
	}

	_, err := tsdb.NewMeasurementFieldSet(path)
	if err != tsdb.ErrUnknownFieldsFormat {
		t.Fatalf("unexpected error: got %v, exp %v", err, tsdb.ErrUnknownFieldsFormat)
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
		tmpShard := path.Join(tmpDir, "shard")
		tmpWal := path.Join(tmpDir, "wal")
		shard := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, tsdb.NewEngineOptions())
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
	tmpShard := path.Join(tmpDir, "shard")
	tmpWal := path.Join(tmpDir, "wal")
	shard := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, tsdb.NewEngineOptions())
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

// NewShard returns a new instance of Shard with temp paths.
func NewShard(index string) *Shard {
	// Create temporary path for data and WAL.
	dir, err := ioutil.TempDir("", "influxdb-tsdb-")
	if err != nil {
		panic(err)
	}

	sfile := MustOpenSeriesFile()

	// Build engine options.
	opt := tsdb.NewEngineOptions()
	opt.IndexVersion = index
	opt.Config.WALDir = filepath.Join(dir, "wal")
	if index == "inmem" {
		opt.InmemIndex = inmem.NewIndex(path.Base(dir), sfile.SeriesFile)
	}
	// Initialise series id sets. Need to do this as it's normally done at the
	// store level.
	seriesIDs := tsdb.NewSeriesIDSet()
	opt.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{seriesIDs})

	return &Shard{
		Shard: tsdb.NewShard(0,
			filepath.Join(dir, "data", "db0", "rp0", "1"),
			filepath.Join(dir, "wal", "db0", "rp0", "1"),
			sfile.SeriesFile,
			opt,
		),
		sfile: sfile,
		path:  dir,
	}
}

// MustNewOpenShard creates and opens a shard with the provided index.
func MustNewOpenShard(index string) *Shard {
	sh := NewShard(index)
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
