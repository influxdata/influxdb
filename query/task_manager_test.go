package query_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestTaskManager_Queries_Host verifies that the requesting client host passed
// in ExecutionOptions is threaded through onto the tracked Task and surfaced in
// the QueryInfo returned by Queries().
func TestTaskManager_Queries_Host(t *testing.T) {
	tm := query.NewTaskManager()

	q, err := influxql.ParseQuery("SELECT * FROM cpu")
	require.NoError(t, err)

	const clientHost = "10.0.0.5:12345"
	_, detach, err := tm.AttachQuery(q, query.ExecutionOptions{
		Database: "mydb",
		UserID:   "alice",
		Host:     clientHost,
	}, nil)
	require.NoError(t, err)
	defer detach()

	queries := tm.Queries()
	require.Len(t, queries, 1)
	require.Equal(t, clientHost, queries[0].Host)
	// Sanity check that the pre-existing fields are still populated correctly.
	require.Equal(t, "mydb", queries[0].Database)
	require.Equal(t, "alice", queries[0].User)
}

// TestTaskManager_LogCurrentQueries_Host verifies that LogCurrentQueries emits
// the host as the first structured field, matching queryFieldNames ordering.
func TestTaskManager_LogCurrentQueries_Host(t *testing.T) {
	tm := query.NewTaskManager()

	q, err := influxql.ParseQuery("SELECT * FROM cpu")
	require.NoError(t, err)

	const clientHost = "192.0.2.10:5000"
	_, detach, err := tm.AttachQuery(q, query.ExecutionOptions{Host: clientHost}, nil)
	require.NoError(t, err)
	defer detach()

	fieldsByMsg := make(map[string][]zap.Field)
	tm.LogCurrentQueries(func(msg string, fields ...zap.Field) {
		fieldsByMsg[msg] = fields
	})

	fields, ok := fieldsByMsg["Current Queries"]
	require.True(t, ok, "expected a \"Current Queries\" log entry")
	require.NotEmpty(t, fields)

	require.Equal(t, "host", fields[0].Key)
	require.Equal(t, clientHost, fields[0].String)
}

// TestTaskManager_SlowQueryLog_Host verifies that the slow-query warning names
// the client the query originated from.
func TestTaskManager_SlowQueryLog_Host(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)

	tm := query.NewTaskManager()
	tm.Logger = zap.New(core)
	tm.LogQueriesAfter = time.Millisecond

	q, err := influxql.ParseQuery("SELECT * FROM cpu")
	require.NoError(t, err)

	const clientHost = "203.0.113.9:4444"
	_, detach, err := tm.AttachQuery(q, query.ExecutionOptions{
		Database: "mydb",
		UserID:   "alice",
		Host:     clientHost,
	}, nil)
	require.NoError(t, err)
	defer detach()

	require.Eventually(t, func() bool {
		return logs.FilterMessageSnippet("Detected slow query from " + clientHost).Len() > 0
	}, time.Second, 5*time.Millisecond, "expected a slow-query warning naming the client host")
}

// TestTaskManager_ShowQueries_HostColumn verifies that SHOW QUERIES reports the
// client host and that its column headers stay aligned with the row values.
func TestTaskManager_ShowQueries_HostColumn(t *testing.T) {
	e := NewQueryExecutor()

	// blockCh keeps the SELECT alive so it stays visible in SHOW QUERIES.
	blockCh := make(chan struct{})
	defer close(blockCh)

	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			switch stmt.(type) {
			case *influxql.ShowQueriesStatement:
				return e.TaskManager.ExecuteStatement(ctx, stmt)
			case *influxql.SelectStatement:
				<-blockCh
				return nil
			}
			t.Errorf("unexpected statement: %s", stmt)
			return errUnexpected
		},
	}

	const clientHost = "198.51.100.23:5555"
	sel, err := influxql.ParseQuery("SELECT * FROM cpu")
	require.NoError(t, err)
	go func() {
		discardOutput(e.ExecuteQuery(sel, query.ExecutionOptions{
			Database: "mydb",
			UserID:   "alice",
			Host:     clientHost,
		}, nil))
	}()

	// Wait for the background SELECT to register with the TaskManager.
	require.Eventually(t, func() bool {
		return len(e.TaskManager.Queries()) >= 1
	}, time.Second, 5*time.Millisecond)

	showQ, err := influxql.ParseQuery("SHOW QUERIES")
	require.NoError(t, err)

	results := e.ExecuteQuery(showQ, query.ExecutionOptions{
		UserID:           "alice",
		CoarseAuthorizer: &meta.UserInfo{Name: "alice", Admin: true},
	}, nil)
	result := <-results
	require.NoError(t, result.Err)
	require.Len(t, result.Series, 1)

	series := result.Series[0]
	require.Equal(t, "host", series.Columns[0])

	// Every row must have exactly one value per column, otherwise the reported
	// values are shifted relative to their headers.
	for _, row := range series.Values {
		require.Lenf(t, row, len(series.Columns),
			"SHOW QUERIES row has %d values but %d columns: %v", len(row), len(series.Columns), row)
	}

	// The SELECT we launched must be reported under the host column.
	var found bool
	for _, row := range series.Values {
		if host, ok := row[0].(string); ok && host == clientHost {
			found = true
			break
		}
	}
	require.Truef(t, found, "expected SHOW QUERIES to report host %q; rows=%v", clientHost, series.Values)
}
