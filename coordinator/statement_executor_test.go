package coordinator_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
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
	e.MetaClient.ShardGroupsByTimeRangeFn = func(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
		return []meta.ShardGroupInfo{
			{ID: 1, Shards: []meta.ShardInfo{
				{ID: 100, Owners: []meta.ShardOwner{{NodeID: 0}}},
			}},
		}, nil
	}

	// The TSDB store should return an IteratorCreator for shard.
	// This IteratorCreator returns a single iterator with "value" in the aux fields.
	e.TSDBStore.ShardGroupFn = func(ids []uint64) tsdb.ShardGroup {
		if !reflect.DeepEqual(ids, []uint64{100}) {
			t.Fatalf("unexpected shard ids: %v", ids)
		}

		var sh MockShard
		sh.CreateIteratorFn = func(_ context.Context, _ *influxql.Measurement, _ query.IteratorOptions) (query.Iterator, error) {
			return &FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Time: int64(0 * time.Second), Aux: []interface{}{float64(100)}},
				{Name: "cpu", Time: int64(1 * time.Second), Aux: []interface{}{float64(200)}},
			}}, nil
		}
		sh.FieldDimensionsFn = func(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
			if !reflect.DeepEqual(measurements, []string{"cpu"}) {
				t.Fatalf("unexpected source: %#v", measurements)
			}
			return map[string]influxql.DataType{"value": influxql.Float}, nil, nil
		}
		return &sh
	}

	// Verify all results from the query.
	if a := ReadAllResults(e.ExecuteQuery(`SELECT * FROM cpu`, "db0", 0)); !reflect.DeepEqual(a, []*query.Result{
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

// Ensure query executor can enforce a maximum bucket selection count.
func TestQueryExecutor_ExecuteQuery_MaxSelectBucketsN(t *testing.T) {
	e := DefaultQueryExecutor()
	e.StatementExecutor.MaxSelectBucketsN = 3

	// The meta client should return a single shards on the local node.
	e.MetaClient.ShardGroupsByTimeRangeFn = func(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
		return []meta.ShardGroupInfo{
			{ID: 1, Shards: []meta.ShardInfo{
				{ID: 100, Owners: []meta.ShardOwner{{NodeID: 0}}},
			}},
		}, nil
	}

	e.TSDBStore.ShardGroupFn = func(ids []uint64) tsdb.ShardGroup {
		if !reflect.DeepEqual(ids, []uint64{100}) {
			t.Fatalf("unexpected shard ids: %v", ids)
		}

		var sh MockShard
		sh.CreateIteratorFn = func(_ context.Context, _ *influxql.Measurement, _ query.IteratorOptions) (query.Iterator, error) {
			return &FloatIterator{
				Points: []query.FloatPoint{{Name: "cpu", Time: int64(0 * time.Second), Aux: []interface{}{float64(100)}}},
			}, nil
		}
		sh.FieldDimensionsFn = func(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
			if !reflect.DeepEqual(measurements, []string{"cpu"}) {
				t.Fatalf("unexpected source: %#v", measurements)
			}
			return map[string]influxql.DataType{"value": influxql.Float}, nil, nil
		}
		return &sh
	}

	// Verify all results from the query.
	if a := ReadAllResults(e.ExecuteQuery(`SELECT count(value) FROM cpu WHERE time >= '2000-01-01T00:00:05Z' AND time < '2000-01-01T00:00:35Z' GROUP BY time(10s)`, "db0", 0)); !reflect.DeepEqual(a, []*query.Result{
		{
			StatementID: 0,
			Err:         errors.New("max-select-buckets limit exceeded: (4/3)"),
		},
	}) {
		t.Fatalf("unexpected results: %s", spew.Sdump(a))
	}
}

func TestStatementExecutor_NormalizeDropSeries(t *testing.T) {
	q, err := influxql.ParseQuery("DROP SERIES FROM cpu")
	if err != nil {
		t.Fatalf("unexpected error parsing query: %v", err)
	}

	stmt := q.Statements[0].(*influxql.DropSeriesStatement)

	s := &coordinator.StatementExecutor{
		MetaClient: &internal.MetaClientMock{
			DatabaseFn: func(name string) *meta.DatabaseInfo {
				t.Fatal("meta client should not be called")
				return nil
			},
		},
	}
	if err := s.NormalizeStatement(stmt, "foo"); err != nil {
		t.Fatalf("unexpected error normalizing statement: %v", err)
	}

	m := stmt.Sources[0].(*influxql.Measurement)
	if m.Database != "" {
		t.Fatalf("database rewritten when not supposed to: %v", m.Database)
	}
	if m.RetentionPolicy != "" {
		t.Fatalf("database rewritten when not supposed to: %v", m.RetentionPolicy)
	}

	if exp, got := "DROP SERIES FROM cpu", q.String(); exp != got {
		t.Fatalf("generated query does match parsed: exp %v, got %v", exp, got)
	}
}

func TestStatementExecutor_NormalizeDeleteSeries(t *testing.T) {
	q, err := influxql.ParseQuery("DELETE FROM cpu")
	if err != nil {
		t.Fatalf("unexpected error parsing query: %v", err)
	}

	stmt := q.Statements[0].(*influxql.DeleteSeriesStatement)

	s := &coordinator.StatementExecutor{
		MetaClient: &internal.MetaClientMock{
			DatabaseFn: func(name string) *meta.DatabaseInfo {
				t.Fatal("meta client should not be called")
				return nil
			},
		},
	}
	if err := s.NormalizeStatement(stmt, "foo"); err != nil {
		t.Fatalf("unexpected error normalizing statement: %v", err)
	}

	m := stmt.Sources[0].(*influxql.Measurement)
	if m.Database != "" {
		t.Fatalf("database rewritten when not supposed to: %v", m.Database)
	}
	if m.RetentionPolicy != "" {
		t.Fatalf("database rewritten when not supposed to: %v", m.RetentionPolicy)
	}

	if exp, got := "DELETE FROM cpu", q.String(); exp != got {
		t.Fatalf("generated query does match parsed: exp %v, got %v", exp, got)
	}
}

type mockAuthorizer struct {
	AuthorizeDatabaseFn func(influxql.Privilege, string) bool
}

func (a *mockAuthorizer) AuthorizeDatabase(p influxql.Privilege, name string) bool {
	return a.AuthorizeDatabaseFn(p, name)
}

func (m *mockAuthorizer) AuthorizeQuery(database string, query *influxql.Query) error {
	panic("fail")
}

func (m *mockAuthorizer) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	panic("fail")
}

func (m *mockAuthorizer) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	panic("fail")
}

func TestQueryExecutor_ExecuteQuery_ShowDatabases(t *testing.T) {
	qe := query.NewQueryExecutor()
	qe.StatementExecutor = &coordinator.StatementExecutor{
		MetaClient: &internal.MetaClientMock{
			DatabasesFn: func() []meta.DatabaseInfo {
				return []meta.DatabaseInfo{
					{Name: "db1"}, {Name: "db2"}, {Name: "db3"}, {Name: "db4"},
				}
			},
		},
	}

	opt := query.ExecutionOptions{
		Authorizer: &mockAuthorizer{
			AuthorizeDatabaseFn: func(p influxql.Privilege, name string) bool {
				return name == "db2" || name == "db4"
			},
		},
	}

	q, err := influxql.ParseQuery("SHOW DATABASES")
	if err != nil {
		t.Fatal(err)
	}

	results := ReadAllResults(qe.ExecuteQuery(q, opt, make(chan struct{})))
	exp := []*query.Result{
		{
			StatementID: 0,
			Series: []*models.Row{{
				Name:    "databases",
				Columns: []string{"name"},
				Values: [][]interface{}{
					{"db2"}, {"db4"},
				},
			}},
		},
	}
	if !reflect.DeepEqual(results, exp) {
		t.Fatalf("unexpected results: exp %s, got %s", spew.Sdump(exp), spew.Sdump(results))
	}
}

// QueryExecutor is a test wrapper for coordinator.QueryExecutor.
type QueryExecutor struct {
	*query.QueryExecutor

	MetaClient        MetaClient
	TSDBStore         *internal.TSDBStoreMock
	StatementExecutor *coordinator.StatementExecutor
	LogOutput         bytes.Buffer
}

// NewQueryExecutor returns a new instance of QueryExecutor.
// This query executor always has a node id of 0.
func NewQueryExecutor() *QueryExecutor {
	e := &QueryExecutor{
		QueryExecutor: query.NewQueryExecutor(),
		TSDBStore:     &internal.TSDBStoreMock{},
	}

	e.TSDBStore.CreateShardFn = func(database, policy string, shardID uint64, enabled bool) error {
		return nil
	}

	e.TSDBStore.MeasurementNamesFn = func(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
		return nil, nil
	}

	e.TSDBStore.TagValuesFn = func(_ query.Authorizer, _ []uint64, _ influxql.Expr) ([]tsdb.TagValues, error) {
		return nil, nil
	}

	e.StatementExecutor = &coordinator.StatementExecutor{
		MetaClient: &e.MetaClient,
		TSDBStore:  e.TSDBStore,
		ShardMapper: &coordinator.LocalShardMapper{
			MetaClient: &e.MetaClient,
			TSDBStore:  e.TSDBStore,
		},
	}
	e.QueryExecutor.StatementExecutor = e.StatementExecutor

	var out io.Writer = &e.LogOutput
	if testing.Verbose() {
		out = io.MultiWriter(out, os.Stderr)
	}
	e.QueryExecutor.WithLogger(logger.New(out))

	return e
}

// DefaultQueryExecutor returns a QueryExecutor with a database (db0) and retention policy (rp0).
func DefaultQueryExecutor() *QueryExecutor {
	e := NewQueryExecutor()
	e.MetaClient.DatabaseFn = DefaultMetaClientDatabaseFn
	return e
}

// ExecuteQuery parses query and executes against the database.
func (e *QueryExecutor) ExecuteQuery(q, database string, chunkSize int) <-chan *query.Result {
	return e.QueryExecutor.ExecuteQuery(MustParseQuery(q), query.ExecutionOptions{
		Database:  database,
		ChunkSize: chunkSize,
	}, make(chan struct{}))
}

type MockShard struct {
	Measurements      []string
	FieldDimensionsFn func(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	CreateIteratorFn  func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error)
	IteratorCostFn    func(m string, opt query.IteratorOptions) (query.IteratorCost, error)
	ExpandSourcesFn   func(sources influxql.Sources) (influxql.Sources, error)
}

func (sh *MockShard) MeasurementsByRegex(re *regexp.Regexp) []string {
	names := make([]string, 0, len(sh.Measurements))
	for _, name := range sh.Measurements {
		if re.MatchString(name) {
			names = append(names, name)
		}
	}
	return names
}

func (sh *MockShard) FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return sh.FieldDimensionsFn(measurements)
}

func (sh *MockShard) MapType(measurement, field string) influxql.DataType {
	f, d, err := sh.FieldDimensions([]string{measurement})
	if err != nil {
		return influxql.Unknown
	}

	if typ, ok := f[field]; ok {
		return typ
	} else if _, ok := d[field]; ok {
		return influxql.Tag
	}
	return influxql.Unknown
}

func (sh *MockShard) CreateIterator(ctx context.Context, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	return sh.CreateIteratorFn(ctx, measurement, opt)
}

func (sh *MockShard) IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	return sh.IteratorCostFn(measurement, opt)
}

func (sh *MockShard) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return sh.ExpandSourcesFn(sources)
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
func ReadAllResults(c <-chan *query.Result) []*query.Result {
	var a []*query.Result
	for result := range c {
		a = append(a, result)
	}
	return a
}

// FloatIterator is a represents an iterator that reads from a slice.
type FloatIterator struct {
	Points []query.FloatPoint
	stats  query.IteratorStats
}

func (itr *FloatIterator) Stats() query.IteratorStats { return itr.stats }
func (itr *FloatIterator) Close() error               { return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *FloatIterator) Next() (*query.FloatPoint, error) {
	if len(itr.Points) == 0 {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v, nil
}
