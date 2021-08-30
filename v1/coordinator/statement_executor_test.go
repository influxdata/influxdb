package coordinator_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/dbrp/mocks"
	influxql2 "github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/influxql/control"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/internal"
	"github.com/influxdata/influxdb/v2/models"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxql"
	"go.uber.org/zap/zaptest"
)

const (
	// DefaultDatabase is the default database name used in tests.
	DefaultDatabase = "db0"

	// DefaultRetentionPolicy is the default retention policy name used in tests.
	DefaultRetentionPolicy = "rp0"
)

// Ensure query executor can execute a simple SELECT statement.
func TestQueryExecutor_ExecuteQuery_SelectStatement(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbrp := mocks.NewMockDBRPMappingService(ctrl)
	orgID := platform.ID(0xff00)
	empty := ""
	filt := influxdb.DBRPMappingFilter{OrgID: &orgID, Database: &empty, RetentionPolicy: &empty}
	res := []*influxdb.DBRPMapping{{}}
	dbrp.EXPECT().
		FindMany(gomock.Any(), filt).
		Return(res, 1, nil)

	e := DefaultQueryExecutor(t, WithDBRP(dbrp))

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
	if a := ReadAllResults(e.ExecuteQuery(context.Background(), `SELECT * FROM cpu`, "db0", 0, orgID)); !reflect.DeepEqual(a, []*query.Result{
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbrp := mocks.NewMockDBRPMappingService(ctrl)
	orgID := platform.ID(0xff00)
	empty := ""
	filt := influxdb.DBRPMappingFilter{OrgID: &orgID, Database: &empty, RetentionPolicy: &empty}
	res := []*influxdb.DBRPMapping{{}}
	dbrp.EXPECT().
		FindMany(gomock.Any(), filt).
		Return(res, 1, nil)

	e := DefaultQueryExecutor(t, WithDBRP(dbrp))

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
	if a := ReadAllResults(e.ExecuteQuery(context.Background(), `SELECT count(value) FROM cpu WHERE time >= '2000-01-01T00:00:05Z' AND time < '2000-01-01T00:00:35Z' GROUP BY time(10s)`, "db0", 0, orgID)); !reflect.DeepEqual(a, []*query.Result{
		{
			StatementID: 0,
			Err:         errors.New("max-select-buckets limit exceeded: (4/3)"),
		},
	}) {
		t.Fatalf("unexpected results: %s", spew.Sdump(a))
	}
}

func TestStatementExecutor_NormalizeStatement(t *testing.T) {

	testCases := []struct {
		name       string
		query      string
		defaultDB  string
		defaultRP  string
		expectedDB string
		expectedRP string
	}{
		{
			name:       "defaults",
			query:      "SELECT f FROM m",
			defaultDB:  DefaultDatabase,
			defaultRP:  "",
			expectedDB: DefaultDatabase,
			expectedRP: DefaultRetentionPolicy,
		},
		{
			name:       "alternate database via param",
			query:      "SELECT f FROM m",
			defaultDB:  "dbalt",
			defaultRP:  "",
			expectedDB: "dbalt",
			expectedRP: DefaultRetentionPolicy,
		},
		{
			name:       "alternate database via query",
			query:      fmt.Sprintf("SELECT f FROM dbalt.%s.m", DefaultRetentionPolicy),
			defaultDB:  DefaultDatabase,
			defaultRP:  "",
			expectedDB: "dbalt",
			expectedRP: DefaultRetentionPolicy,
		},
		{
			name:       "alternate RP via param",
			query:      "SELECT f FROM m",
			defaultDB:  DefaultDatabase,
			defaultRP:  "rpalt",
			expectedDB: DefaultDatabase,
			expectedRP: "rpalt",
		},
		{
			name:       "alternate RP via query",
			query:      fmt.Sprintf("SELECT f FROM %s.rpalt.m", DefaultDatabase),
			defaultDB:  DefaultDatabase,
			defaultRP:  "",
			expectedDB: DefaultDatabase,
			expectedRP: "rpalt",
		},
		{
			name:       "alternate RP query disagrees with param and query wins",
			query:      fmt.Sprintf("SELECT f FROM %s.rpquery.m", DefaultDatabase),
			defaultDB:  DefaultDatabase,
			defaultRP:  "rpparam",
			expectedDB: DefaultDatabase,
			expectedRP: "rpquery",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dbrp := mocks.NewMockDBRPMappingService(ctrl)
			orgID := platform.ID(0xff00)
			bucketID := platform.ID(0xffee)
			filt := influxdb.DBRPMappingFilter{OrgID: &orgID, Database: &testCase.expectedDB}
			res := []*influxdb.DBRPMapping{{Database: testCase.expectedDB, RetentionPolicy: testCase.expectedRP, OrganizationID: orgID, BucketID: bucketID, Default: true}}
			dbrp.EXPECT().
				FindMany(gomock.Any(), filt).
				Return(res, 1, nil)

			e := DefaultQueryExecutor(t, WithDBRP(dbrp))

			q, err := influxql.ParseQuery(testCase.query)
			if err != nil {
				t.Fatalf("unexpected error parsing query: %v", err)
			}

			stmt := q.Statements[0].(*influxql.SelectStatement)

			err = e.StatementExecutor.NormalizeStatement(context.Background(), stmt, testCase.defaultDB, testCase.defaultRP, &query.ExecutionContext{ExecutionOptions: query.ExecutionOptions{OrgID: orgID}})
			if err != nil {
				t.Fatalf("unexpected error normalizing statement: %v", err)
			}

			m := stmt.Sources[0].(*influxql.Measurement)
			if m.Database != testCase.expectedDB {
				t.Errorf("database got %v, want %v", m.Database, testCase.expectedDB)
			}
			if m.RetentionPolicy != testCase.expectedRP {
				t.Errorf("retention policy got %v, want %v", m.RetentionPolicy, testCase.expectedRP)
			}
		})
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
	if err := s.NormalizeStatement(context.Background(), stmt, "foo", "bar", &query.ExecutionContext{}); err != nil {
		t.Fatalf("unexpected error normalizing statement: %v", err)
	}

	m := stmt.Sources[0].(*influxql.Measurement)
	if m.Database != "" {
		t.Fatalf("database rewritten when not supposed to: %v", m.Database)
	}
	if m.RetentionPolicy != "" {
		t.Fatalf("retention policy rewritten when not supposed to: %v", m.RetentionPolicy)
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
	if err := s.NormalizeStatement(context.Background(), stmt, "foo", "bar", &query.ExecutionContext{}); err != nil {
		t.Fatalf("unexpected error normalizing statement: %v", err)
	}

	m := stmt.Sources[0].(*influxql.Measurement)
	if m.Database != "" {
		t.Fatalf("database rewritten when not supposed to: %v", m.Database)
	}
	if m.RetentionPolicy != "" {
		t.Fatalf("retention policy rewritten when not supposed to: %v", m.RetentionPolicy)
	}

	if exp, got := "DELETE FROM cpu", q.String(); exp != got {
		t.Fatalf("generated query does match parsed: exp %v, got %v", exp, got)
	}
}

func TestQueryExecutor_ExecuteQuery_ShowDatabases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbrp := mocks.NewMockDBRPMappingService(ctrl)
	orgID := platform.ID(0xff00)
	filt := influxdb.DBRPMappingFilter{OrgID: &orgID}
	res := []*influxdb.DBRPMapping{
		{Database: "db1", OrganizationID: orgID, BucketID: 0xffe0},
		{Database: "db2", OrganizationID: orgID, BucketID: 0xffe1},
		{Database: "db3", OrganizationID: orgID, BucketID: 0xffe2},
		{Database: "db4", OrganizationID: orgID, BucketID: 0xffe3},
	}
	dbrp.EXPECT().
		FindMany(gomock.Any(), filt).
		Return(res, 4, nil)

	qe := query.NewExecutor(zaptest.NewLogger(t), control.NewControllerMetrics([]string{}))
	qe.StatementExecutor = &coordinator.StatementExecutor{
		DBRP: dbrp,
	}

	opt := query.ExecutionOptions{
		OrgID: orgID,
	}

	q, err := influxql.ParseQuery("SHOW DATABASES")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	ctx = icontext.SetAuthorizer(ctx, &influxdb.Authorization{
		ID:     orgID,
		OrgID:  orgID,
		Status: influxdb.Active,
		Permissions: []influxdb.Permission{
			*itesting.MustNewPermissionAtID(0xffe1, influxdb.ReadAction, influxdb.BucketsResourceType, orgID),
			*itesting.MustNewPermissionAtID(0xffe3, influxdb.ReadAction, influxdb.BucketsResourceType, orgID),
		},
	})

	results := ReadAllResults(qe.ExecuteQuery(ctx, q, opt))
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
	*query.Executor

	MetaClient        MetaClient
	TSDBStore         *internal.TSDBStoreMock
	DBRP              *mocks.MockDBRPMappingService
	StatementExecutor *coordinator.StatementExecutor
	LogOutput         bytes.Buffer
}

// NewQueryExecutor returns a new instance of Executor.
// This query executor always has a node id of 0.
func NewQueryExecutor(t *testing.T, opts ...optFn) *QueryExecutor {
	e := &QueryExecutor{
		Executor:  query.NewExecutor(zaptest.NewLogger(t), control.NewControllerMetrics([]string{})),
		TSDBStore: &internal.TSDBStoreMock{},
	}

	for _, opt := range opts {
		opt(e)
	}

	e.TSDBStore.CreateShardFn = func(database, policy string, shardID uint64, enabled bool) error {
		return nil
	}

	e.TSDBStore.MeasurementNamesFn = func(_ context.Context, auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
		return nil, nil
	}

	e.TSDBStore.TagValuesFn = func(_ context.Context, _ query.Authorizer, _ []uint64, _ influxql.Expr) ([]tsdb.TagValues, error) {
		return nil, nil
	}

	e.StatementExecutor = &coordinator.StatementExecutor{
		MetaClient: &e.MetaClient,
		TSDBStore:  e.TSDBStore,
		DBRP:       e.DBRP,
		ShardMapper: &coordinator.LocalShardMapper{
			MetaClient: &e.MetaClient,
			TSDBStore:  e.TSDBStore,
			DBRP:       e.DBRP,
		},
	}
	e.Executor.StatementExecutor = e.StatementExecutor

	return e
}

type optFn func(qe *QueryExecutor)

func WithDBRP(dbrp *mocks.MockDBRPMappingService) optFn {
	return func(qe *QueryExecutor) {
		qe.DBRP = dbrp
	}
}

// DefaultQueryExecutor returns a Executor with a database (db0) and retention policy (rp0).
func DefaultQueryExecutor(t *testing.T, opts ...optFn) *QueryExecutor {
	e := NewQueryExecutor(t, opts...)
	e.MetaClient.DatabaseFn = DefaultMetaClientDatabaseFn
	return e
}

// ExecuteQuery parses query and executes against the database.
func (e *QueryExecutor) ExecuteQuery(ctx context.Context, q, database string, chunkSize int, orgID platform.ID) (<-chan *query.Result, *influxql2.Statistics) {
	return e.Executor.ExecuteQuery(ctx, MustParseQuery(q), query.ExecutionOptions{
		OrgID:     orgID,
		Database:  database,
		ChunkSize: chunkSize,
	})
}

type MockShard struct {
	Measurements             []string
	FieldDimensionsFn        func(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	FieldKeysByMeasurementFn func(name []byte) []string
	CreateIteratorFn         func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error)
	IteratorCostFn           func(ctx context.Context, m string, opt query.IteratorOptions) (query.IteratorCost, error)
	ExpandSourcesFn          func(sources influxql.Sources) (influxql.Sources, error)
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

func (sh *MockShard) FieldKeysByMeasurement(name []byte) []string {
	return sh.FieldKeysByMeasurementFn(name)
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

func (sh *MockShard) IteratorCost(ctx context.Context, measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	return sh.IteratorCostFn(ctx, measurement, opt)
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
func ReadAllResults(c <-chan *query.Result, _ *influxql2.Statistics) []*query.Result {
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
