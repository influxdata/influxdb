package query_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
)

var errUnexpected = errors.New("unexpected error")

type StatementExecutor struct {
	ExecuteStatementFn func(stmt influxql.Statement, ctx query.ExecutionContext) error
}

func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx query.ExecutionContext) error {
	return e.ExecuteStatementFn(stmt, ctx)
}

func NewQueryExecutor() *query.QueryExecutor {
	return query.NewQueryExecutor()
}

func TestQueryExecutor_AttachQuery(t *testing.T) {
	abort := make(chan struct{})
	defer close(abort)

	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
			if ctx.QueryID != 1 {
				t.Errorf("incorrect query id: exp=1 got=%d", ctx.QueryID)
			}
			return nil
		},
	}

	discardOutput(e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil))
}

func TestQueryExecutor_KillQuery(t *testing.T) {
	abort := make(chan struct{})
	defer close(abort)

	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qid := make(chan uint64)

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
			switch stmt.(type) {
			case *influxql.KillQueryStatement:
				return e.TaskManager.ExecuteStatement(stmt, ctx)
			}

			qid <- ctx.QueryID
			select {
			case <-ctx.InterruptCh:
				return query.ErrQueryInterrupted
			case <-time.After(100 * time.Millisecond):
				t.Error("killing the query did not close the channel after 100 milliseconds")
				return errUnexpected
			}
		},
	}

	results := e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil)
	q, err = influxql.ParseQuery(fmt.Sprintf("KILL QUERY %d", <-qid))
	if err != nil {
		t.Fatal(err)
	}
	discardOutput(e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil))

	result := <-results
	if result.Err != query.ErrQueryInterrupted {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_KillQuery_Zombie(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qid := make(chan uint64)
	done := make(chan struct{})

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
			switch stmt.(type) {
			case *influxql.KillQueryStatement, *influxql.ShowQueriesStatement:
				return e.TaskManager.ExecuteStatement(stmt, ctx)
			}

			qid <- ctx.QueryID
			select {
			case <-ctx.InterruptCh:
				select {
				case <-done:
					// Keep the query running until we run SHOW QUERIES.
				case <-time.After(100 * time.Millisecond):
					// Ensure that we don't have a lingering goroutine.
				}
				return query.ErrQueryInterrupted
			case <-time.After(100 * time.Millisecond):
				t.Error("killing the query did not close the channel after 100 milliseconds")
				return errUnexpected
			}
		},
	}

	results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
	q, err = influxql.ParseQuery(fmt.Sprintf("KILL QUERY %d", <-qid))
	if err != nil {
		t.Fatal(err)
	}
	discardOutput(e.ExecuteQuery(q, query.ExecutionOptions{}, nil))

	// Display the queries and ensure that the original is still in there.
	q, err = influxql.ParseQuery("SHOW QUERIES")
	if err != nil {
		t.Fatal(err)
	}
	tasks := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)

	// The killed query should still be there.
	task := <-tasks
	var series *query.Series
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case series = <-task.SeriesCh():
		timer.Stop()
	case <-timer.C:
		t.Fatal("unexpected timeout")
	}

	// Should retrieve two rows from the series.
	rows := make([][]interface{}, 0, 2)
	for row := range series.RowCh() {
		if row.Err != nil {
			t.Errorf("unexpected error: %s", row.Err)
		}
		rows = append(rows, row.Values)
	}
	if len(rows) != 2 {
		t.Errorf("expected %d rows, got %d", 2, len(rows))
	}
	close(done)

	// The original query should return.
	result := <-results
	if result.Err != query.ErrQueryInterrupted {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Interrupt(t *testing.T) {
	abort := make(chan struct{})
	defer close(abort)

	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
			select {
			case <-ctx.InterruptCh:
				return query.ErrQueryInterrupted
			case <-time.After(100 * time.Millisecond):
				t.Error("killing the query did not close the channel after 100 milliseconds")
				return errUnexpected
			}
		},
	}

	closing := make(chan struct{})
	results := e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, closing)
	close(closing)
	result := <-results
	if result.Err != query.ErrQueryInterrupted {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_ShowQueries(t *testing.T) {
	abort := make(chan struct{})
	defer close(abort)

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
			switch stmt.(type) {
			case *influxql.ShowQueriesStatement:
				return e.TaskManager.ExecuteStatement(stmt, ctx)
			}

			t.Errorf("unexpected statement: %s", stmt)
			return errUnexpected
		},
	}

	q, err := influxql.ParseQuery(`SHOW QUERIES`)
	if err != nil {
		t.Fatal(err)
	}

	results := e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil)
	result := <-results

	// Should be able to retrieve a single series.
	var series *query.Series
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case series = <-result.SeriesCh():
		timer.Stop()
	case <-timer.C:
		t.Fatal("unexpected timeout")
	}

	// Should only retrieve one row from the series.
	rows := make([][]interface{}, 0, 1)
	for row := range series.RowCh() {
		if row.Err != nil {
			t.Errorf("unexpected error: %s", row.Err)
		}
		rows = append(rows, row.Values)
	}
	if len(rows) != 1 {
		t.Errorf("expected %d rows, got %d", 1, len(rows))
	}

	// No more series should be returned.
	if series := <-result.SeriesCh(); series != nil {
		t.Fatal("unexpected series")
	}
}

func TestQueryExecutor_Limit_Timeout(t *testing.T) {
	abort := make(chan struct{})
	defer close(abort)

	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
			select {
			case <-ctx.InterruptCh:
				return query.ErrQueryInterrupted
			case <-time.After(time.Second):
				t.Errorf("timeout has not killed the query")
				return errUnexpected
			}
		},
	}
	e.TaskManager.QueryTimeout = time.Nanosecond

	results := e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil)
	result := <-results
	if result.Err == nil || !strings.Contains(result.Err.Error(), "query-timeout") {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Limit_ConcurrentQueries(t *testing.T) {
	abort := make(chan struct{})
	defer close(abort)

	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qid := make(chan uint64)

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
			qid <- ctx.QueryID
			<-ctx.InterruptCh
			return query.ErrQueryInterrupted
		},
	}
	e.TaskManager.MaxConcurrentQueries = 1
	defer e.Close()

	// Start first query and wait for it to be executing.
	go discardOutput(e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil))
	<-qid

	// Start second query and expect for it to fail.
	results := e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil)

	select {
	case result := <-results:
		if result.Err == nil || !strings.Contains(result.Err.Error(), "max-concurrent-queries") {
			t.Errorf("unexpected error: %s", result.Err)
		}
	case <-qid:
		t.Errorf("unexpected statement execution for the second query")
	}
}

func TestQueryExecutor_Close(t *testing.T) {
	abort := make(chan struct{})
	defer close(abort)

	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	ch1 := make(chan struct{})
	ch2 := make(chan struct{})

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
			close(ch1)
			<-ctx.InterruptCh
			return query.ErrQueryInterrupted
		},
	}

	results := e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil)
	go func(results <-chan *query.ResultSet) {
		result := <-results
		if result.Err != query.ErrQueryEngineShutdown {
			t.Errorf("unexpected error: %s", result.Err)
		}
		close(ch2)
	}(results)

	// Wait for the statement to start executing.
	<-ch1

	// Close the query executor.
	e.Close()

	// Check that the statement gets interrupted and finishes.
	select {
	case <-ch2:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("closing the query manager did not kill the query after 100 milliseconds")
	}

	results = e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil)
	result := <-results
	if result.Err != query.ErrQueryEngineShutdown {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Panic(t *testing.T) {
	abort := make(chan struct{})
	defer close(abort)

	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
			panic("test error")
		},
	}

	results := e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil)
	result := <-results
	if result.Err == nil || result.Err.Error() != "SELECT count(value) FROM cpu [panic:test error]" {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_InvalidSource(t *testing.T) {
	abort := make(chan struct{})
	defer close(abort)

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx query.ExecutionContext) error {
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

		results := e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: abort}, nil)
		result := <-results
		if result.Err == nil || result.Err.Error() != tt.err {
			t.Errorf("%d. unexpected error: %s", i, result.Err)
		}
	}
}

func discardOutput(results <-chan *query.ResultSet) {
	for r := range results {
		// Read all results and discard.
		for s := range r.SeriesCh() {
			// Read all series and discard.
			for range s.RowCh() {
				// Read all rows and discard.
			}
		}
	}
}
