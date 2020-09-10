package query_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	iql "github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/influxql/control"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/influxql/query/mocks"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

var errUnexpected = errors.New("unexpected error")

type StatementExecutor struct {
	ExecuteStatementFn func(ctx context.Context, stmt influxql.Statement, ectx *query.ExecutionContext) error
}

func (e *StatementExecutor) ExecuteStatement(ctx context.Context, stmt influxql.Statement, ectx *query.ExecutionContext) error {
	return e.ExecuteStatementFn(ctx, stmt, ectx)
}

func NewQueryExecutor(t *testing.T) *query.Executor {
	return query.NewExecutor(zaptest.NewLogger(t), control.NewControllerMetrics([]string{}))
}

func TestQueryExecutor_Interrupt(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor(t)
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(ctx context.Context, stmt influxql.Statement, ectx *query.ExecutionContext) error {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(100 * time.Millisecond):
				t.Error("killing the query did not close the channel after 100 milliseconds")
				return errUnexpected
			}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	results, _ := e.ExecuteQuery(ctx, q, query.ExecutionOptions{})
	cancel()

	result := <-results
	if result != nil && result.Err != query.ErrQueryInterrupted {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Abort(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	ch1 := make(chan struct{})
	ch2 := make(chan struct{})

	e := NewQueryExecutor(t)
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(ctx context.Context, stmt influxql.Statement, ectx *query.ExecutionContext) error {
			<-ch1
			if err := ectx.Send(ctx, &query.Result{Err: errUnexpected}); err == nil {
				t.Errorf("expected error")
			}
			close(ch2)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, _ := e.ExecuteQuery(ctx, q, query.ExecutionOptions{})
	close(ch1)

	<-ch2
	discardOutput(results)
}

func TestQueryExecutor_Panic(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor(t)
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(ctx context.Context, stmt influxql.Statement, ectx *query.ExecutionContext) error {
			panic("test error")
		},
	}

	results, _ := e.ExecuteQuery(context.Background(), q, query.ExecutionOptions{})
	result := <-results
	if len(result.Series) != 0 {
		t.Errorf("expected %d rows, got %d", 0, len(result.Series))
	}
	if result.Err == nil || result.Err.Error() != "SELECT count(value) FROM cpu [panic:test error]" {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_InvalidSource(t *testing.T) {
	e := NewQueryExecutor(t)
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(ctx context.Context, stmt influxql.Statement, ectx *query.ExecutionContext) error {
			return errors.New("statement executed unexpectedly")
		},
	}

	for i, tt := range []struct {
		q   string
		err string
	}{
		{
			q:   `SELECT fieldKey, fieldType FROM _fieldKeys`,
			err: `unable to use system source '_fieldKeys': use SHOW FIELD KEYS instead`,
		},
		{
			q:   `SELECT "name" FROM _measurements`,
			err: `unable to use system source '_measurements': use SHOW MEASUREMENTS instead`,
		},
		{
			q:   `SELECT "key" FROM _series`,
			err: `unable to use system source '_series': use SHOW SERIES instead`,
		},
		{
			q:   `SELECT tagKey FROM _tagKeys`,
			err: `unable to use system source '_tagKeys': use SHOW TAG KEYS instead`,
		},
		{
			q:   `SELECT "key", value FROM _tags`,
			err: `unable to use system source '_tags': use SHOW TAG VALUES instead`,
		},
	} {
		q, err := influxql.ParseQuery(tt.q)
		if err != nil {
			t.Errorf("%d. unable to parse: %s", i, tt.q)
			continue
		}

		results, _ := e.ExecuteQuery(context.Background(), q, query.ExecutionOptions{})
		result := <-results
		if len(result.Series) != 0 {
			t.Errorf("%d. expected %d rows, got %d", 0, i, len(result.Series))
		}
		if result.Err == nil || result.Err.Error() != tt.err {
			t.Errorf("%d. unexpected error: %s", i, result.Err)
		}
	}
}

// This test verifies Statistics are gathered
// and that ExecuteDuration accounts for PlanDuration
func TestExecutor_ExecuteQuery_Statistics(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	stmt := influxql.MustParseStatement("SELECT f0 FROM m0")
	q := &influxql.Query{Statements: influxql.Statements{stmt, stmt}}

	se := mocks.NewMockStatementExecutor(ctl)
	se.EXPECT().ExecuteStatement(gomock.Any(), stmt, gomock.Any()).
		Times(2).
		DoAndReturn(func(ctx context.Context, statement influxql.Statement, ectx *query.ExecutionContext) error {
			time.Sleep(10 * time.Millisecond)
			ectx.StatisticsGatherer.Append(iql.NewImmutableCollector(iql.Statistics{PlanDuration: 5 * time.Millisecond}))
			return nil
		})

	e := NewQueryExecutor(t)
	e.StatementExecutor = se

	ctx := context.Background()
	results, stats := e.ExecuteQuery(ctx, q, query.ExecutionOptions{Quiet: true})
	<-results
	assert.GreaterOrEqual(t, int64(stats.ExecuteDuration), int64(10*time.Millisecond))
	assert.Equal(t, 10*time.Millisecond, stats.PlanDuration)
	assert.Equal(t, 2, stats.StatementCount)
}

func discardOutput(results <-chan *query.Result) {
	for range results {
		// Read all results and discard.
	}
}
