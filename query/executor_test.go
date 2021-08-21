package query_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

var errUnexpected = errors.New("unexpected error")

type StatementExecutor struct {
	ExecuteStatementFn func(stmt influxql.Statement, ctx *query.ExecutionContext) error
}

func (e *StatementExecutor) ExecuteStatement(ctx *query.ExecutionContext, stmt influxql.Statement) error {
	return e.ExecuteStatementFn(stmt, ctx)
}
type StatementNormalizerExecutor struct {
	StatementExecutor
	NormalizeStatementFn  func(stmt influxql.Statement, database, retentionPolicy string) error
}

func (e *StatementNormalizerExecutor) NormalizeStatement(stmt influxql.Statement, database, retentionPolicy string) error {
	return e.NormalizeStatementFn(stmt, database, retentionPolicy)
}

func NewQueryExecutor() *query.Executor {
	return query.NewExecutor()
}

func TestQueryExecutor_AttachQuery(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			if ctx.QueryID != 1 {
				t.Errorf("incorrect query id: exp=1 got=%d", ctx.QueryID)
			}
			return nil
		},
	}

	discardOutput(e.ExecuteQuery(q, query.ExecutionOptions{}, nil))
}

func TestQueryExecutor_KillQuery(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qid := make(chan uint64)

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			switch stmt.(type) {
			case *influxql.KillQueryStatement:
				return e.TaskManager.ExecuteStatement(ctx, stmt)
			}

			qid <- ctx.QueryID
			select {
			case <-ctx.Done():
				return ctx.Err()
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
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			switch stmt.(type) {
			case *influxql.KillQueryStatement, *influxql.ShowQueriesStatement:
				return e.TaskManager.ExecuteStatement(ctx, stmt)
			}

			qid <- ctx.QueryID
			select {
			case <-ctx.Done():
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
	if len(task.Series) != 1 {
		t.Errorf("expected %d series, got %d", 1, len(task.Series))
	} else if len(task.Series[0].Values) != 2 {
		t.Errorf("expected %d rows, got %d", 2, len(task.Series[0].Values))
	}
	close(done)

	// The original query should return.
	result := <-results
	if result.Err != query.ErrQueryInterrupted {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_KillQuery_CloseTaskManager(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qid := make(chan uint64)

	// Open a channel to stall the statement executor forever. This keeps the statement executor
	// running even after we kill the query which can happen with some queries. We only close it once
	// the test has finished running.
	done := make(chan struct{})
	defer close(done)

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			switch stmt.(type) {
			case *influxql.KillQueryStatement, *influxql.ShowQueriesStatement:
				return e.TaskManager.ExecuteStatement(ctx, stmt)
			}

			qid <- ctx.QueryID
			<-done
			return nil
		},
	}

	// Kill the query. This should switch it into a zombie state.
	go discardOutput(e.ExecuteQuery(q, query.ExecutionOptions{}, nil))
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
	if len(task.Series) != 1 {
		t.Errorf("expected %d series, got %d", 1, len(task.Series))
	} else if len(task.Series[0].Values) != 2 {
		t.Errorf("expected %d rows, got %d", 2, len(task.Series[0].Values))
	}

	// Close the task manager to ensure it doesn't cause a panic.
	if err := e.TaskManager.Close(); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestQueryExecutor_KillQuery_AlreadyKilled(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qid := make(chan uint64)

	// Open a channel to stall the statement executor forever. This keeps the statement executor
	// running even after we kill the query which can happen with some queries. We only close it once
	// the test has finished running.
	done := make(chan struct{})
	defer close(done)

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			switch stmt.(type) {
			case *influxql.KillQueryStatement, *influxql.ShowQueriesStatement:
				return e.TaskManager.ExecuteStatement(ctx, stmt)
			}

			qid <- ctx.QueryID
			<-done
			return nil
		},
	}

	// Kill the query. This should switch it into a zombie state.
	go discardOutput(e.ExecuteQuery(q, query.ExecutionOptions{}, nil))
	q, err = influxql.ParseQuery(fmt.Sprintf("KILL QUERY %d", <-qid))
	if err != nil {
		t.Fatal(err)
	}
	discardOutput(e.ExecuteQuery(q, query.ExecutionOptions{}, nil))

	// Now attempt to kill it again. We should get an error.
	results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
	result := <-results
	if got, want := result.Err, query.ErrAlreadyKilled; got != want {
		t.Errorf("unexpected error: got=%v want=%v", got, want)
	}
}

func TestQueryExecutor_Interrupt(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				t.Error("killing the query did not close the channel after 100 milliseconds")
				return errUnexpected
			}
		},
	}

	closing := make(chan struct{})
	results := e.ExecuteQuery(q, query.ExecutionOptions{}, closing)
	close(closing)
	result := <-results
	if result.Err != query.ErrQueryInterrupted {
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

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			<-ch1
			if err := ctx.Send(&query.Result{Err: errUnexpected}); err != query.ErrQueryAborted {
				t.Errorf("unexpected error: %v", err)
			}
			close(ch2)
			return nil
		},
	}

	done := make(chan struct{})
	close(done)

	results := e.ExecuteQuery(q, query.ExecutionOptions{AbortCh: done}, nil)
	close(ch1)

	<-ch2
	discardOutput(results)
}

func TestQueryExecutor_ShowQueries(t *testing.T) {
	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			switch stmt.(type) {
			case *influxql.ShowQueriesStatement:
				return e.TaskManager.ExecuteStatement(ctx, stmt)
			}

			t.Errorf("unexpected statement: %s", stmt)
			return errUnexpected
		},
	}

	q, err := influxql.ParseQuery(`SHOW QUERIES`)
	if err != nil {
		t.Fatal(err)
	}

	results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
	result := <-results
	if len(result.Series) != 1 {
		t.Errorf("expected %d series, got %d", 1, len(result.Series))
	} else if len(result.Series[0].Values) != 1 {
		t.Errorf("expected %d row, got %d", 1, len(result.Series[0].Values))
	}
	if result.Err != nil {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Limit_Timeout(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				t.Errorf("timeout has not killed the query")
				return errUnexpected
			}
		},
	}
	e.TaskManager.QueryTimeout = time.Nanosecond

	results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
	result := <-results
	if result.Err == nil || !strings.Contains(result.Err.Error(), "query-timeout") {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Limit_ConcurrentQueries(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qid := make(chan uint64)

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			qid <- ctx.QueryID
			<-ctx.Done()
			return ctx.Err()
		},
	}
	e.TaskManager.MaxConcurrentQueries = 1
	defer e.Close()

	// Start first query and wait for it to be executing.
	go discardOutput(e.ExecuteQuery(q, query.ExecutionOptions{}, nil))
	<-qid

	// Start second query and expect for it to fail.
	results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)

	select {
	case result := <-results:
		if len(result.Series) != 0 {
			t.Errorf("expected %d rows, got %d", 0, len(result.Series))
		}
		if result.Err == nil || !strings.Contains(result.Err.Error(), "max-concurrent-queries") {
			t.Errorf("unexpected error: %s", result.Err)
		}
	case <-qid:
		t.Errorf("unexpected statement execution for the second query")
	}
}

func TestQueryExecutor_Close(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	ch1 := make(chan struct{})
	ch2 := make(chan struct{})

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			close(ch1)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
	go func(results <-chan *query.Result) {
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

	results = e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
	result := <-results
	if len(result.Series) != 0 {
		t.Errorf("expected %d rows, got %d", 0, len(result.Series))
	}
	if result.Err != query.ErrQueryEngineShutdown {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Panic(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			panic("test error")
		},
	}

	results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
	result := <-results
	if len(result.Series) != 0 {
		t.Errorf("expected %d rows, got %d", 0, len(result.Series))
	}
	if result.Err == nil || result.Err.Error() != "SELECT count(value) FROM cpu [panic:test error]" {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

const goodStatement = `SELECT count(value) FROM cpu`

func TestQueryExecutor_NotExecuted(t *testing.T) {
	var executorFailIteration int
	var normalizerFailIteration int
	var executorCallCount int
	var normalizerCallCount int
	queryStatements := []string{goodStatement, goodStatement, goodStatement, goodStatement, goodStatement}
	queryStr := strings.Join(queryStatements, ";")

	q, err := influxql.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("parsing %s: %v", queryStr, err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = &StatementNormalizerExecutor{
		StatementExecutor: StatementExecutor{
			ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
				executorCallCount++
				if executorFailIteration == executorCallCount {
					return fmt.Errorf("executor failure on call %d", executorCallCount)
				} else {
					return ctx.Send(&query.Result{Err: nil})
				}
			},
		},
		NormalizeStatementFn: func(stmt influxql.Statement, database, retentionPolicy string) error {
			normalizerCallCount++
			if normalizerFailIteration == normalizerCallCount {
				return fmt.Errorf("normalizer failure on call %d", normalizerCallCount)
			} else {
				return nil
			}
		},
	}
	test := func(testName string, i int, failIter, ignoreIter *int) {
		*failIter = i + 1
		*ignoreIter = -1
		executorCallCount = 0
		normalizerCallCount = 0

		results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
		j := 1
		for result := range results {
			if j < *failIter && result.Err != nil {
				t.Fatalf("%s failed early: %v", testName, result.Err)
			} else if j == *failIter && result.Err == nil {
				t.Fatalf("%s unexpected success", testName)
			} else if j > *failIter && result.Err != query.ErrNotExecuted {
				t.Fatalf("expected ErrorNotExecuted from %s but got: %v", testName, result.Err)
			}
			j++
		}
		if j != (len(queryStatements) + 1) {
			t.Fatalf("wrong number of results from %s - got: %d, expected: %d", testName, j, len(queryStatements))
		}
	}
	for i := 0; i < len(queryStatements); i++ {

		test("executor", i, &executorFailIteration, &normalizerFailIteration)
		test("normalizer", i, &normalizerFailIteration, &executorFailIteration)
	}
}

func TestQueryExecutor_SystemNameNotExecuted(t *testing.T) {
	e := NewQueryExecutor()
	e.StatementExecutor = &StatementNormalizerExecutor{
		StatementExecutor: StatementExecutor{
			ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
				return ctx.Send(&query.Result{Err: nil})
			},
		},
		NormalizeStatementFn: func(stmt influxql.Statement, database, retentionPolicy string) error {
			return nil
		},
	}
	const metaTestCnt = 3
	for j := 0; j < metaTestCnt; j++ {
		stmt := make([]string, 0, metaTestCnt)
		for i := 0; i < metaTestCnt; i++ {
			if i != j {
				stmt = append(stmt, goodStatement)
			} else {
				stmt = append(stmt, "SELECT * FROM _fieldKeys")
			}
		}
		queryStr := strings.Join(stmt, ";")
		q, err := influxql.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("parsing %s: %v", queryStr, err)
		}
		results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
		r := 0
		const testName = "system measurement detection"
		for result := range results {
			if r < j && result.Err != nil {
				t.Fatalf("%s failed early: %v", testName, result.Err)
			} else if r == j && result.Err == nil {
				t.Fatalf("%s unexpected success", testName)
			} else if r > j && result.Err != query.ErrNotExecuted {
				t.Fatalf("expected ErrorNotExecuted from %s but got: %v", testName, result.Err)
			}
			r++
		}
	}
}

func TestQueryExecutor_InvalidSource(t *testing.T) {
	e := NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
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

		results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil)
		result := <-results
		if len(result.Series) != 0 {
			t.Errorf("%d. expected %d rows, got %d", 0, i, len(result.Series))
		}
		if result.Err == nil || result.Err.Error() != tt.err {
			t.Errorf("%d. unexpected error: %s", i, result.Err)
		}
	}
}

func discardOutput(results <-chan *query.Result) {
	for range results {
		// Read all results and discard.
	}
}
