package storage_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/v1/tsdb"
	"github.com/influxdata/influxdb/v2/v1/tsdb/engine/tsm1"
	"github.com/prometheus/client_golang/prometheus"
)

func TestEngine_WriteAndIndex(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()

	// Calling WritePoints when the engine is not open will return
	// ErrEngineClosed.
	if got, exp := engine.Engine.WritePoints(context.TODO(), nil), storage.ErrEngineClosed; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.Tags{
			{Key: models.MeasurementTagKeyBytes, Value: []byte("cpu")},
			{Key: []byte("host"), Value: []byte("server")},
			{Key: models.FieldKeyTagKeyBytes, Value: []byte("value")},
		},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Engine.WritePoints(context.TODO(), []models.Point{pt}); err != nil {
		t.Fatal(err)
	}

	pt.SetTime(time.Unix(2, 3))
	if err := engine.Engine.WritePoints(context.TODO(), []models.Point{pt}); err != nil {
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
	if err := engine.Engine.WritePoints(context.TODO(), []models.Point{pt}); err != nil {
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

	if err := engine.Engine.WritePoints(context.TODO(), []models.Point{pt}); err == nil {
		t.Fatal("expected error: got nil")
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"foo": "bar", "time": "value"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Engine.WritePoints(context.TODO(), []models.Point{pt}); err == nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEngine_InvalidTag(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"\xf2": "cpu"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.WritePoints(context.TODO(), []models.Point{pt}); err == nil {
		fmt.Println(pt.String())
		t.Fatal("expected error: got nil")
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"foo": "bar", string([]byte{0, 255, 188, 233}): "value"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.WritePoints(context.TODO(), []models.Point{pt}); err == nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWrite_TimeField(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	name := tsdb.EncodeNameString(engine.org, engine.bucket)

	pt := models.MustNewPoint(
		name,
		models.NewTags(map[string]string{models.FieldKeyTagKey: "time", models.MeasurementTagKey: "cpu"}),
		map[string]interface{}{"time": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Engine.WritePoints(context.TODO(), []models.Point{pt}); err == nil {
		t.Fatal("expected error: got nil")
	}

	var points []models.Point
	points = append(points, models.MustNewPoint(
		name,
		models.NewTags(map[string]string{models.FieldKeyTagKey: "time", models.MeasurementTagKey: "cpu"}),
		map[string]interface{}{"time": 1.0},
		time.Unix(1, 2),
	))
	points = append(points, models.MustNewPoint(
		name,
		models.NewTags(map[string]string{models.FieldKeyTagKey: "value", models.MeasurementTagKey: "cpu"}),
		map[string]interface{}{"value": 1.1},
		time.Unix(1, 2),
	))

	if err := engine.Engine.WritePoints(context.TODO(), points); err == nil {
		t.Fatal("expected error: got nil")
	}
}

func TestEngine_WriteAddNewField(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	name := tsdb.EncodeNameString(engine.org, engine.bucket)

	if err := engine.Engine.WritePoints(context.TODO(), []models.Point{models.MustNewPoint(
		name,
		models.NewTags(map[string]string{models.FieldKeyTagKey: "value", models.MeasurementTagKey: "cpu", "host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)}); err != nil {
		t.Fatalf(err.Error())
	}

	if err := engine.Engine.WritePoints(context.TODO(), []models.Point{
		models.MustNewPoint(
			name,
			models.NewTags(map[string]string{models.FieldKeyTagKey: "value", models.MeasurementTagKey: "cpu", "host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		),
		models.MustNewPoint(
			name,
			models.NewTags(map[string]string{models.FieldKeyTagKey: "value2", models.MeasurementTagKey: "cpu", "host": "server"}),
			map[string]interface{}{"value2": 2.0},
			time.Unix(1, 2),
		),
	}); err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := engine.SeriesCardinality(), int64(2); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

func TestEngine_DeleteBucket(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	orgID, _ := influxdb.IDFromString("3131313131313131")
	bucketID, _ := influxdb.IDFromString("8888888888888888")

	err := engine.Engine.WritePoints(context.TODO(), []models.Point{models.MustNewPoint(
		tsdb.EncodeNameString(engine.org, engine.bucket),
		models.NewTags(map[string]string{models.FieldKeyTagKey: "value", models.MeasurementTagKey: "cpu", "host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)})
	if err != nil {
		t.Fatal(err)
	}

	// Same org, different bucket.
	err = engine.Engine.WritePoints(context.TODO(), []models.Point{
		models.MustNewPoint(
			tsdb.EncodeNameString(*orgID, *bucketID),
			models.NewTags(map[string]string{models.FieldKeyTagKey: "value", models.MeasurementTagKey: "cpu", "host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 3),
		),
		models.MustNewPoint(
			tsdb.EncodeNameString(*orgID, *bucketID),
			models.NewTags(map[string]string{models.FieldKeyTagKey: "value2", models.MeasurementTagKey: "cpu", "host": "server"}),
			map[string]interface{}{"value2": 2.0},
			time.Unix(1, 3),
		),
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, exp := engine.SeriesCardinality(), int64(3); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

	// Remove the original bucket.
	if err := engine.DeleteBucket(context.Background(), engine.org, engine.bucket); err != nil {
		t.Fatal(err)
	}

	// Check only one bucket was removed.
	if got, exp := engine.SeriesCardinality(), int64(2); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

func TestEngine_DeleteBucket_Predicate(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	p := func(m, f string, kvs ...string) models.Point {
		tags := map[string]string{models.FieldKeyTagKey: f, models.MeasurementTagKey: m}
		for i := 0; i < len(kvs)-1; i += 2 {
			tags[kvs[i]] = kvs[i+1]
		}
		return models.MustNewPoint(
			tsdb.EncodeNameString(engine.org, engine.bucket),
			models.NewTags(tags),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		)
	}

	err := engine.Engine.WritePoints(context.TODO(), []models.Point{
		p("cpu", "value", "tag1", "val1"),
		p("cpu", "value", "tag2", "val2"),
		p("cpu", "value", "tag3", "val3"),
		p("mem", "value", "tag1", "val1"),
		p("mem", "value", "tag2", "val2"),
		p("mem", "value", "tag3", "val3"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check the series cardinality.
	if got, exp := engine.SeriesCardinality(), int64(6); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

	// Construct a predicate to remove tag2
	pred, err := tsm1.NewProtobufPredicate(&datatypes.Predicate{
		Root: &datatypes.Node{
			NodeType: datatypes.NodeTypeComparisonExpression,
			Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
			Children: []*datatypes.Node{
				{NodeType: datatypes.NodeTypeTagRef,
					Value: &datatypes.Node_TagRefValue{TagRefValue: "tag2"},
				},
				{NodeType: datatypes.NodeTypeLiteral,
					Value: &datatypes.Node_StringValue{StringValue: "val2"},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Remove the matching series.
	if err := engine.DeleteBucketRangePredicate(context.Background(), engine.org, engine.bucket,
		math.MinInt64, math.MaxInt64, pred); err != nil {
		t.Fatal(err)
	}

	// Check only matching series were removed.
	if got, exp := engine.SeriesCardinality(), int64(4); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

	// Delete based on field key.
	pred, err = tsm1.NewProtobufPredicate(&datatypes.Predicate{
		Root: &datatypes.Node{
			NodeType: datatypes.NodeTypeComparisonExpression,
			Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
			Children: []*datatypes.Node{
				{NodeType: datatypes.NodeTypeTagRef,
					Value: &datatypes.Node_TagRefValue{TagRefValue: models.FieldKeyTagKey},
				},
				{NodeType: datatypes.NodeTypeLiteral,
					Value: &datatypes.Node_StringValue{StringValue: "value"},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Remove the matching series.
	if err := engine.DeleteBucketRangePredicate(context.Background(), engine.org, engine.bucket,
		math.MinInt64, math.MaxInt64, pred); err != nil {
		t.Fatal(err)
	}

	// Check only matching series were removed.
	if got, exp := engine.SeriesCardinality(), int64(0); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

}

func TestEngine_OpenClose(t *testing.T) {
	engine := NewDefaultEngine()
	engine.MustOpen()

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	if err := engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEngine_InitializeMetrics(t *testing.T) {
	engine := NewDefaultEngine()

	engine.MustOpen()
	reg := prometheus.NewRegistry()
	reg.MustRegister(engine.PrometheusCollectors()...)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	files := promtest.MustFindMetric(t, mfs, "storage_tsm_files_total", prometheus.Labels{
		"node_id":   fmt.Sprint(engine.nodeID),
		"engine_id": fmt.Sprint(engine.engineID),
		"level":     "1",
	})
	if m, got, exp := files, files.GetGauge().GetValue(), 0.0; got != exp {
		t.Errorf("[%s] got %v, expected %v", m, got, exp)
	}

	bytes := promtest.MustFindMetric(t, mfs, "storage_tsm_files_disk_bytes", prometheus.Labels{
		"node_id":   fmt.Sprint(engine.nodeID),
		"engine_id": fmt.Sprint(engine.engineID),
		"level":     "1",
	})
	if m, got, exp := bytes, bytes.GetGauge().GetValue(), 0.0; got != exp {
		t.Errorf("[%s] got %v, expected %v", m, got, exp)
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
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
		models.Tags{
			{Key: models.MeasurementTagKeyBytes, Value: []byte("cpu")},
			{Key: []byte("host"), Value: []byte("server")},
			{Key: models.FieldKeyTagKeyBytes, Value: []byte("value")},
		},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := engine.Engine.WritePoints(context.TODO(), []models.Point{pt})
	if err != nil {
		t.Fatal(err)
	}

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

	// ensure the index gets loaded after closing and opening the shard
	engine.Engine.Close() // Don't destroy temporary data.
	engine.Open(context.Background())

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

func TestEngine_WALDisabled(t *testing.T) {
	config := storage.NewConfig()
	config.WAL.Enabled = false

	engine := NewEngine(config, rand.Int(), rand.Int())
	defer engine.Close()
	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.Tags{
			{Key: models.MeasurementTagKeyBytes, Value: []byte("cpu")},
			{Key: []byte("host"), Value: []byte("server")},
			{Key: models.FieldKeyTagKeyBytes, Value: []byte("value")},
		},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Engine.WritePoints(context.TODO(), []models.Point{pt}); err != nil {
		t.Fatal(err)
	}
}

func TestEngine_WriteConflictingBatch(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	name := tsdb.EncodeNameString(engine.org, engine.bucket)

	err := engine.Engine.WritePoints(context.TODO(), []models.Point{
		models.MustNewPoint(
			name,
			models.NewTags(map[string]string{models.FieldKeyTagKey: "value", models.MeasurementTagKey: "cpu", "host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		),
		models.MustNewPoint(
			name,
			models.NewTags(map[string]string{models.FieldKeyTagKey: "value", models.MeasurementTagKey: "cpu", "host": "server"}),
			map[string]interface{}{"value": 2},
			time.Unix(1, 2),
		),
	})
	if _, ok := err.(tsdb.PartialWriteError); !ok {
		t.Fatal("expected partial write error. got:", err)
	}
}

// BenchmarkWritePoints_100K demonstrates the impact that batch size has on
// writing a fixed number of points into storage. In this case 100K points are
// written according to varying batch sizes.
//
// Typical results from a laptop.
//
// BenchmarkWritePoints_100K/wal_on_batch_size_10-8         	       1	73067875393 ns/op	140772760 B/op	 1490501 allocs/op
// BenchmarkWritePoints_100K/wal_on_batch_size_100-8        	       1	8485389740 ns/op	86121960 B/op	  700262 allocs/op
// BenchmarkWritePoints_100K/wal_on_batch_size_1000-8       	       1	1102477562 ns/op	87796384 B/op	  561415 allocs/op
// BenchmarkWritePoints_100K/wal_on_batch_size_10000-8      	       4	 305958369 ns/op	98445778 B/op	  756521 allocs/op
// BenchmarkWritePoints_100K/wal_on_batch_size_100000-8     	       3	 399678388 ns/op	228627397 B/op	 2440186 allocs/op
// BenchmarkWritePoints_100K/wal_off_batch_size_10-8        	       2	 565581060 ns/op	134326648 B/op	 1424452 allocs/op
// BenchmarkWritePoints_100K/wal_off_batch_size_100-8       	       5	 219888477 ns/op	84745681 B/op	  689427 allocs/op
// BenchmarkWritePoints_100K/wal_off_batch_size_1000-8      	       6	 184525844 ns/op	86766286 B/op	  556131 allocs/op
// BenchmarkWritePoints_100K/wal_off_batch_size_10000-8     	       5	 216334467 ns/op	98397942 B/op	  756227 allocs/op
// BenchmarkWritePoints_100K/wal_off_batch_size_100000-8    	       3	 360319162 ns/op	219879885 B/op	 2440234 allocs/op
//
func BenchmarkWritePoints_100K(b *testing.B) {
	var engine *Engine

	genBatch := func(n int) models.Points {
		points := make([]models.Point, n)
		for i := 0; i < n; i++ {
			points[i] = models.MustNewPoint(
				"cpu",
				models.NewTags(map[string]string{
					models.MeasurementTagKey: "cpu-1",
					"host":                   "server",
					"server":                 fmt.Sprint(i),
					models.FieldKeyTagKey:    "temp",
				}),
				map[string]interface{}{"value": i},
				time.Unix(1, 2),
			)
		}
		return points
	}

	run := func(b *testing.B, setup func(), batchSize int) {
		setup()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 100000; j += batchSize {
				b.StopTimer()
				points := genBatch(batchSize) // create a new batch
				b.StartTimer()
				if err := engine.WritePoints(context.Background(), points); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
			if err := engine.Close(); err != nil {
				panic(err)
			}
			setup()
			b.StartTimer()
		}
	}

	for i := 1; i <= 5; i++ {
		batchSize := int(math.Pow10(i))
		b.Run(fmt.Sprintf("wal_on_batch_size_%d", batchSize), func(b *testing.B) {
			run(b, func() {
				cfg := storage.NewConfig()
				engine = NewEngine(cfg, rand.Int(), rand.Int())
				engine.MustOpen()
			}, batchSize)
		})
	}

	for i := 1; i <= 5; i++ {
		batchSize := int(math.Pow10(i))
		b.Run(fmt.Sprintf("wal_off_batch_size_%d", batchSize), func(b *testing.B) {
			run(b, func() {
				cfg := storage.NewConfig()
				cfg.WAL.Enabled = false // Disable WAL
				engine = NewEngine(cfg, rand.Int(), rand.Int())
				engine.MustOpen()
			}, batchSize)
		})
	}
}

// Typical benchmarks on a laptop
// pkg: github.com/influxdata/influxdb/storage
// BenchmarkDeleteBucket/cardinality_10-8         	     162	   7242260 ns/op	    9584 B/op	     106 allocs/op
// BenchmarkDeleteBucket/cardinality_100-8        	     163	   7514230 ns/op	   10407 B/op	     117 allocs/op
// BenchmarkDeleteBucket/cardinality_1000-8       	     160	   7152721 ns/op	   19632 B/op	     237 allocs/op
// BenchmarkDeleteBucket/cardinality_10000-8      	     160	   7343742 ns/op	  124481 B/op	    1420 allocs/op
// BenchmarkDeleteBucket/cardinality_100000-8     	      99	  10482284 ns/op	 1915489 B/op	   21349 allocs/op
func BenchmarkDeleteBucket(b *testing.B) {
	var engine *Engine
	setup := func(card int) {
		cfg := storage.NewConfig()
		cfg.WAL.Enabled = false // Disable WAL
		engine = NewEngine(cfg, rand.Int(), rand.Int())
		engine.MustOpen()

		points := make([]models.Point, card)
		for i := 0; i < card; i++ {
			points[i] = models.MustNewPoint(
				"cpu",
				models.NewTags(map[string]string{
					models.MeasurementTagKey: "cpu-1",
					"host":                   "server",
					models.FieldKeyTagKey:    "temp",
				}),
				map[string]interface{}{"value": i},
				time.Unix(1, 2),
			)
		}

		if err := engine.Engine.WritePoints(context.TODO(), points); err != nil {
			panic(err)
		}
	}

	for i := 1; i <= 5; i++ {
		card := int(math.Pow10(i))

		b.Run(fmt.Sprintf("cardinality_%d", card), func(b *testing.B) {
			setup(card)
			for i := 0; i < b.N; i++ {
				if err := engine.DeleteBucket(context.Background(), engine.org, engine.bucket); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				if err := engine.Close(); err != nil {
					panic(err)
				}
				setup(card)
				b.StartTimer()
			}
		})

	}
}

type Engine struct {
	path        string
	org, bucket influxdb.ID

	engineID int
	nodeID   int
	*storage.Engine
}

// NewEngine create a new wrapper around a storage engine.
func NewEngine(c storage.Config, engineID, nodeID int) *Engine {
	path, _ := ioutil.TempDir("", "storage_engine_test")

	engine := storage.NewEngine(path, c, storage.WithEngineID(engineID), storage.WithNodeID(nodeID))

	org, err := influxdb.IDFromString("3131313131313131")
	if err != nil {
		panic(err)
	}

	bucket, err := influxdb.IDFromString("3232323232323232")
	if err != nil {
		panic(err)
	}

	return &Engine{
		path:     path,
		org:      *org,
		bucket:   *bucket,
		engineID: engineID,
		nodeID:   nodeID,
		Engine:   engine,
	}
}

// NewDefaultEngine returns a new Engine with a default configuration.
func NewDefaultEngine() *Engine {
	return NewEngine(storage.NewConfig(), rand.Int(), rand.Int())
}

// MustOpen opens the engine or panicks.
func (e *Engine) MustOpen() {
	if err := e.Engine.Open(context.Background()); err != nil {
		panic(err)
	}
}

// Close closes the engine and removes all temporary data.
func (e *Engine) Close() error {
	defer os.RemoveAll(e.path)
	return e.Engine.Close()
}
