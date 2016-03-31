package cluster_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cluster"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
)

const (
	// DefaultDatabase is the default database name used in tests.
	DefaultDatabase = "db0"

	// DefaultRetentionPolicy is the default retention policy name used in tests.
	DefaultRetentionPolicy = "rp0"
)

// Ensure query executor can execute a simple SELECT statement.
func TestQueryExecutor_ExecuteQuery_SelectStatement(t *testing.T) {
	e := DefaultQueryExecutor()

	// The meta client should return a single shard owned by the local node.
	e.MetaClient.ShardsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
		return []meta.ShardInfo{{ID: 100, Owners: []meta.ShardOwner{{NodeID: 0}}}}, nil
	}

	// The TSDB store should return an IteratorCreator for shard.
	// This IteratorCreator returns a single iterator with "value" in the aux fields.
	e.TSDBStore.ShardIteratorCreatorFn = func(id uint64) influxql.IteratorCreator {
		if id != 100 {
			t.Fatalf("unexpected shard id: %d", id)
		}

		var ic IteratorCreator
		ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
			return &FloatIterator{Points: []influxql.FloatPoint{
				{Name: "cpu", Time: int64(0 * time.Second), Aux: []interface{}{float64(100)}},
				{Name: "cpu", Time: int64(1 * time.Second), Aux: []interface{}{float64(200)}},
			}}, nil
		}
		ic.FieldDimensionsFn = func(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
			return map[string]struct{}{"value": struct{}{}}, nil, nil
		}
		ic.SeriesKeysFn = func(opt influxql.IteratorOptions) (influxql.SeriesList, error) {
			return influxql.SeriesList{
				{Name: "cpu", Aux: []influxql.DataType{influxql.Float}},
			}, nil
		}
		return &ic
	}

	// Verify all results from the query.
	if a := ReadAllResults(e.ExecuteQuery(`SELECT * FROM cpu`, "db0", 0)); !reflect.DeepEqual(a, []*influxql.Result{
		{
			StatementID: 0,
			Series: []*models.Row{{
				Name:    "cpu",
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{time.Unix(0, 0).UTC(), float64(100)},
					{time.Unix(1, 0).UTC(), float64(200)},
				},
			}},
		},
	}) {
		t.Fatalf("unexpected results: %s", spew.Sdump(a))
	}
}

// Ensure query executor can enforce a maximum series selection count.
func TestQueryExecutor_ExecuteQuery_MaxSelectSeriesN(t *testing.T) {
	e := DefaultQueryExecutor()
	e.MaxSelectSeriesN = 3

	// The meta client should return a two shards on the local node.
	e.MetaClient.ShardsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
		return []meta.ShardInfo{
			{ID: 100, Owners: []meta.ShardOwner{{NodeID: 0}}},
			{ID: 101, Owners: []meta.ShardOwner{{NodeID: 0}}},
		}, nil
	}

	// This iterator creator returns an iterator that operates on 2 series.
	// Reuse this iterator for both shards. This brings the total series count to 4.
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{
			Points: []influxql.FloatPoint{{Name: "cpu", Time: int64(0 * time.Second), Aux: []interface{}{float64(100)}}},
			stats:  influxql.IteratorStats{SeriesN: 2},
		}, nil
	}
	ic.FieldDimensionsFn = func(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
		return map[string]struct{}{"value": struct{}{}}, nil, nil
	}
	ic.SeriesKeysFn = func(opt influxql.IteratorOptions) (influxql.SeriesList, error) {
		return influxql.SeriesList{
			{Name: "cpu", Aux: []influxql.DataType{influxql.Float}},
		}, nil
	}
	e.TSDBStore.ShardIteratorCreatorFn = func(id uint64) influxql.IteratorCreator { return &ic }

	// Verify all results from the query.
	if a := ReadAllResults(e.ExecuteQuery(`SELECT count(value) FROM cpu`, "db0", 0)); !reflect.DeepEqual(a, []*influxql.Result{
		{
			StatementID: 0,
			Err:         errors.New("max select series count exceeded: 4 series"),
		},
	}) {
		t.Fatalf("unexpected results: %s", spew.Sdump(a))
	}
}

// Ensure query executor can enforce a maximum bucket selection count.
func TestQueryExecutor_ExecuteQuery_MaxSelectBucketsN(t *testing.T) {
	e := DefaultQueryExecutor()
	e.MaxSelectBucketsN = 3

	// The meta client should return a single shards on the local node.
	e.MetaClient.ShardsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
		return []meta.ShardInfo{
			{ID: 100, Owners: []meta.ShardOwner{{NodeID: 0}}},
		}, nil
	}

	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{
			Points: []influxql.FloatPoint{{Name: "cpu", Time: int64(0 * time.Second), Aux: []interface{}{float64(100)}}},
		}, nil
	}
	ic.FieldDimensionsFn = func(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
		return map[string]struct{}{"value": struct{}{}}, nil, nil
	}
	ic.SeriesKeysFn = func(opt influxql.IteratorOptions) (influxql.SeriesList, error) {
		return influxql.SeriesList{
			{Name: "cpu", Aux: []influxql.DataType{influxql.Float}},
		}, nil
	}
	e.TSDBStore.ShardIteratorCreatorFn = func(id uint64) influxql.IteratorCreator { return &ic }

	// Verify all results from the query.
	if a := ReadAllResults(e.ExecuteQuery(`SELECT count(value) FROM cpu WHERE time >= '2000-01-01T00:00:05Z' AND time < '2000-01-01T00:00:35Z' GROUP BY time(10s)`, "db0", 0)); !reflect.DeepEqual(a, []*influxql.Result{
		{
			StatementID: 0,
			Err:         errors.New("max select bucket count exceeded: 4 buckets"),
		},
	}) {
		t.Fatalf("unexpected results: %s", spew.Sdump(a))
	}
}

// QueryExecutor is a test wrapper for cluster.QueryExecutor.
type QueryExecutor struct {
	*cluster.QueryExecutor

	MetaClient MetaClient
	TSDBStore  TSDBStore
	LogOutput  bytes.Buffer
}

// NewQueryExecutor returns a new instance of QueryExecutor.
// This query executor always has a node id of 0.
func NewQueryExecutor() *QueryExecutor {
	e := &QueryExecutor{
		QueryExecutor: cluster.NewQueryExecutor(),
	}
	e.Node = &influxdb.Node{ID: 0}
	e.QueryExecutor.MetaClient = &e.MetaClient
	e.QueryExecutor.TSDBStore = &e.TSDBStore

	e.QueryExecutor.LogOutput = &e.LogOutput
	if testing.Verbose() {
		e.QueryExecutor.LogOutput = io.MultiWriter(e.QueryExecutor.LogOutput, os.Stderr)
	}

	return e
}

// DefaultQueryExecutor returns a QueryExecutor with a database (db0) and retention policy (rp0).
func DefaultQueryExecutor() *QueryExecutor {
	e := NewQueryExecutor()
	e.MetaClient.DatabaseFn = DefaultMetaClientDatabaseFn
	e.TSDBStore.ExpandSourcesFn = DefaultTSDBStoreExpandSourcesFn
	return e
}

// ExecuteQuery parses query and executes against the database.
func (e *QueryExecutor) ExecuteQuery(query, database string, chunkSize int) <-chan *influxql.Result {
	return e.QueryExecutor.ExecuteQuery(MustParseQuery(query), database, chunkSize, make(chan struct{}))
}

// TSDBStore is a mockable implementation of cluster.TSDBStore.
type TSDBStore struct {
	CreateShardFn  func(database, policy string, shardID uint64) error
	WriteToShardFn func(shardID uint64, points []models.Point) error

	DeleteDatabaseFn                func(name string) error
	DeleteMeasurementFn             func(database, name string) error
	DeleteRetentionPolicyFn         func(database, name string) error
	DeleteShardFn                   func(id uint64) error
	DeleteSeriesFn                  func(database string, sources []influxql.Source, condition influxql.Expr) error
	ExecuteShowFieldKeysStatementFn func(stmt *influxql.ShowFieldKeysStatement, database string) (models.Rows, error)
	ExecuteShowTagValuesStatementFn func(stmt *influxql.ShowTagValuesStatement, database string) (models.Rows, error)
	ExpandSourcesFn                 func(sources influxql.Sources) (influxql.Sources, error)
	ShardIteratorCreatorFn          func(id uint64) influxql.IteratorCreator
}

func (s *TSDBStore) CreateShard(database, policy string, shardID uint64) error {
	if s.CreateShardFn == nil {
		return nil
	}
	return s.CreateShardFn(database, policy, shardID)
}

func (s *TSDBStore) WriteToShard(shardID uint64, points []models.Point) error {
	return s.WriteToShardFn(shardID, points)
}

func (s *TSDBStore) DeleteDatabase(name string) error {
	return s.DeleteDatabaseFn(name)
}

func (s *TSDBStore) DeleteMeasurement(database, name string) error {
	return s.DeleteMeasurementFn(database, name)
}

func (s *TSDBStore) DeleteRetentionPolicy(database, name string) error {
	return s.DeleteRetentionPolicyFn(database, name)
}

func (s *TSDBStore) DeleteShard(id uint64) error {
	return s.DeleteShardFn(id)
}

func (s *TSDBStore) DeleteSeries(database string, sources []influxql.Source, condition influxql.Expr) error {
	return s.DeleteSeriesFn(database, sources, condition)
}

func (s *TSDBStore) ExecuteShowFieldKeysStatement(stmt *influxql.ShowFieldKeysStatement, database string) (models.Rows, error) {
	return s.ExecuteShowFieldKeysStatementFn(stmt, database)
}

func (s *TSDBStore) ExecuteShowTagValuesStatement(stmt *influxql.ShowTagValuesStatement, database string) (models.Rows, error) {
	return s.ExecuteShowTagValuesStatementFn(stmt, database)
}

func (s *TSDBStore) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return s.ExpandSourcesFn(sources)
}

func (s *TSDBStore) ShardIteratorCreator(id uint64) influxql.IteratorCreator {
	return s.ShardIteratorCreatorFn(id)
}

// DefaultTSDBStoreExpandSourcesFn expands a single source using the default database & retention policy.
func DefaultTSDBStoreExpandSourcesFn(sources influxql.Sources) (influxql.Sources, error) {
	return influxql.Sources{&influxql.Measurement{
		Database:        DefaultDatabase,
		RetentionPolicy: DefaultRetentionPolicy,
		Name:            sources[0].(*influxql.Measurement).Name},
	}, nil
}

// MustParseQuery parses s into a query. Panic on error.
func MustParseQuery(s string) *influxql.Query {
	q, err := influxql.ParseQuery(s)
	if err != nil {
		panic(err)
	}
	return q
}

// ReadAllResults reads all results from c and returns as a slice.
func ReadAllResults(c <-chan *influxql.Result) []*influxql.Result {
	var a []*influxql.Result
	for result := range c {
		a = append(a, result)
	}
	return a
}

// IteratorCreator is a mockable implementation of IteratorCreator.
type IteratorCreator struct {
	CreateIteratorFn  func(opt influxql.IteratorOptions) (influxql.Iterator, error)
	FieldDimensionsFn func(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error)
	SeriesKeysFn      func(opt influxql.IteratorOptions) (influxql.SeriesList, error)
	ExpandSourcesFn   func(sources influxql.Sources) (influxql.Sources, error)
}

func (ic *IteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return ic.CreateIteratorFn(opt)
}

func (ic *IteratorCreator) FieldDimensions(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
	return ic.FieldDimensionsFn(sources)
}

func (ic *IteratorCreator) SeriesKeys(opt influxql.IteratorOptions) (influxql.SeriesList, error) {
	return ic.SeriesKeysFn(opt)
}

func (ic *IteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return ic.ExpandSourcesFn(sources)
}

// FloatIterator is a represents an iterator that reads from a slice.
type FloatIterator struct {
	Points []influxql.FloatPoint
	stats  influxql.IteratorStats
}

func (itr *FloatIterator) Stats() influxql.IteratorStats { return itr.stats }
func (itr *FloatIterator) Close() error                  { return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *FloatIterator) Next() *influxql.FloatPoint {
	if len(itr.Points) == 0 {
		return nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v
}
