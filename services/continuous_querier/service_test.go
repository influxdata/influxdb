package continuous_querier

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
)

var (
	errExpected   = errors.New("expected error")
	errUnexpected = errors.New("unexpected error")
)

// Test closing never opened, open, open already open, close, and close already closed.
func TestOpenAndClose(t *testing.T) {
	s := NewTestService(t)

	if err := s.Close(); err != nil {
		t.Error(err)
	} else if err = s.Open(); err != nil {
		t.Error(err)
	} else if err = s.Open(); err != nil {
		t.Error(err)
	} else if err = s.Close(); err != nil {
		t.Error(err)
	} else if err = s.Close(); err != nil {
		t.Error(err)
	}
}

// Test Run method.
func TestContinuousQueryService_Run(t *testing.T) {
	s := NewTestService(t)

	// Set RunInterval high so we can trigger using Run method.
	s.RunInterval = 10 * time.Minute

	done := make(chan struct{})
	expectCallCnt := 3
	callCnt := 0

	// Set a callback for ExecuteStatement.
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			callCnt++
			if callCnt >= expectCallCnt {
				done <- struct{}{}
			}
			ctx.Results <- &query.Result{}
			return nil
		},
	}

	// Use a custom "now" time since the internals of last run care about
	// what the actual time is. Truncate to 10 minutes we are starting on an interval.
	now := time.Now().Truncate(10 * time.Minute)

	s.Open()
	// Trigger service to run all CQs.
	s.Run("", "", now)
	// Shouldn't time out.
	if err := wait(done, 100*time.Millisecond); err != nil {
		t.Error(err)
	}
	// This time it should timeout because ExecuteQuery should not get called again.
	if err := wait(done, 100*time.Millisecond); err == nil {
		t.Error("too many queries executed")
	}
	s.Close()

	// Now test just one query.
	expectCallCnt = 1
	callCnt = 0
	s.Open()
	s.Run("db", "cq", now)
	// Shouldn't time out.
	if err := wait(done, 100*time.Millisecond); err != nil {
		t.Error(err)
	}
	// This time it should timeout because ExecuteQuery should not get called again.
	if err := wait(done, 100*time.Millisecond); err == nil {
		t.Error("too many queries executed")
	}
	s.Close()
}

func TestContinuousQueryService_ResampleOptions(t *testing.T) {
	s := NewTestService(t)
	mc := NewMetaClient(t)
	mc.CreateDatabase("db", "")
	mc.CreateContinuousQuery("db", "cq", `CREATE CONTINUOUS QUERY cq ON db RESAMPLE EVERY 10s FOR 2m BEGIN SELECT mean(value) INTO cpu_mean FROM cpu GROUP BY time(1m) END`)
	s.MetaClient = mc

	db := s.MetaClient.Database("db")

	cq, err := NewContinuousQuery(db.Name, &db.ContinuousQueries[0])
	if err != nil {
		t.Fatal(err)
	} else if cq.Resample.Every != 10*time.Second {
		t.Errorf("expected resample every to be 10s, got %s", influxql.FormatDuration(cq.Resample.Every))
	} else if cq.Resample.For != 2*time.Minute {
		t.Errorf("expected resample for 2m, got %s", influxql.FormatDuration(cq.Resample.For))
	}

	// Set RunInterval high so we can trigger using Run method.
	s.RunInterval = 10 * time.Minute

	done := make(chan struct{})
	var expected struct {
		min time.Time
		max time.Time
	}

	// Set a callback for ExecuteStatement.
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			s := stmt.(*influxql.SelectStatement)
			valuer := &influxql.NowValuer{Location: s.Location}
			_, timeRange, err := influxql.ConditionExpr(s.Condition, valuer)
			if err != nil {
				t.Errorf("unexpected error parsing time range: %s", err)
			} else if !expected.min.Equal(timeRange.Min) || !expected.max.Equal(timeRange.Max) {
				t.Errorf("mismatched time range: got=(%s, %s) exp=(%s, %s)", timeRange.Min, timeRange.Max, expected.min, expected.max)
			}
			done <- struct{}{}
			ctx.Results <- &query.Result{}
			return nil
		},
	}

	s.Open()
	defer s.Close()

	// Set the 'now' time to the start of a 10 minute interval. Then trigger a run.
	// This should trigger two queries (one for the current time interval, one for the previous).
	now := time.Now().UTC().Truncate(10 * time.Minute)
	expected.min = now.Add(-2 * time.Minute)
	expected.max = now.Add(-1)
	s.RunCh <- &RunRequest{Now: now}

	if err := wait(done, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Trigger another run 10 seconds later. Another two queries should happen,
	// but it will be a different two queries.
	expected.min = expected.min.Add(time.Minute)
	expected.max = expected.max.Add(time.Minute)
	s.RunCh <- &RunRequest{Now: now.Add(10 * time.Second)}

	if err := wait(done, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Reset the time period and send the initial request at 5 seconds after the
	// 10 minute mark. There should be exactly one call since the current interval is too
	// young and only one interval matches the FOR duration.
	expected.min = now.Add(-time.Minute)
	expected.max = now.Add(-1)
	s.Run("", "", now.Add(5*time.Second))

	if err := wait(done, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Send a message 10 minutes later and ensure that the system plays catchup.
	expected.max = now.Add(10*time.Minute - 1)
	s.RunCh <- &RunRequest{Now: now.Add(10 * time.Minute)}

	if err := wait(done, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// No overflow should be sent.
	if err := wait(done, 100*time.Millisecond); err == nil {
		t.Error("too many queries executed")
	}
}

func TestContinuousQueryService_EveryHigherThanInterval(t *testing.T) {
	s := NewTestService(t)
	ms := NewMetaClient(t)
	ms.CreateDatabase("db", "")
	ms.CreateContinuousQuery("db", "cq", `CREATE CONTINUOUS QUERY cq ON db RESAMPLE EVERY 1m BEGIN SELECT mean(value) INTO cpu_mean FROM cpu GROUP BY time(30s) END`)
	s.MetaClient = ms

	// Set RunInterval high so we can trigger using Run method.
	s.RunInterval = 10 * time.Minute

	done := make(chan struct{})
	var expected struct {
		min time.Time
		max time.Time
	}

	// Set a callback for ExecuteQuery.
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			s := stmt.(*influxql.SelectStatement)
			valuer := &influxql.NowValuer{Location: s.Location}
			_, timeRange, err := influxql.ConditionExpr(s.Condition, valuer)
			if err != nil {
				t.Errorf("unexpected error parsing time range: %s", err)
			} else if !expected.min.Equal(timeRange.Min) || !expected.max.Equal(timeRange.Max) {
				t.Errorf("mismatched time range: got=(%s, %s) exp=(%s, %s)", timeRange.Min, timeRange.Max, expected.min, expected.max)
			}
			done <- struct{}{}
			ctx.Results <- &query.Result{}
			return nil
		},
	}

	s.Open()
	defer s.Close()

	// Set the 'now' time to the start of a 10 minute interval. Then trigger a run.
	// This should trigger two queries (one for the current time interval, one for the previous)
	// since the default FOR interval should be EVERY, not the GROUP BY interval.
	now := time.Now().Truncate(10 * time.Minute)
	expected.min = now.Add(-time.Minute)
	expected.max = now.Add(-1)
	s.RunCh <- &RunRequest{Now: now}

	if err := wait(done, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Trigger 30 seconds later. Nothing should run.
	s.RunCh <- &RunRequest{Now: now.Add(30 * time.Second)}

	if err := wait(done, 100*time.Millisecond); err == nil {
		t.Fatal("too many queries")
	}

	// Run again 1 minute later. Another two queries should run.
	expected.min = now
	expected.max = now.Add(time.Minute - 1)
	s.RunCh <- &RunRequest{Now: now.Add(time.Minute)}

	if err := wait(done, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// No overflow should be sent.
	if err := wait(done, 100*time.Millisecond); err == nil {
		t.Error("too many queries executed")
	}
}

func TestContinuousQueryService_GroupByOffset(t *testing.T) {
	s := NewTestService(t)
	mc := NewMetaClient(t)
	mc.CreateDatabase("db", "")
	mc.CreateContinuousQuery("db", "cq", `CREATE CONTINUOUS QUERY cq ON db BEGIN SELECT mean(value) INTO cpu_mean FROM cpu GROUP BY time(1m, 30s) END`)
	s.MetaClient = mc

	// Set RunInterval high so we can trigger using Run method.
	s.RunInterval = 10 * time.Minute

	done := make(chan struct{})
	var expected struct {
		min time.Time
		max time.Time
	}

	// Set a callback for ExecuteStatement.
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			s := stmt.(*influxql.SelectStatement)
			valuer := &influxql.NowValuer{Location: s.Location}
			_, timeRange, err := influxql.ConditionExpr(s.Condition, valuer)
			if err != nil {
				t.Errorf("unexpected error parsing time range: %s", err)
			} else if !expected.min.Equal(timeRange.Min) || !expected.max.Equal(timeRange.Max) {
				t.Errorf("mismatched time range: got=(%s, %s) exp=(%s, %s)", timeRange.Min, timeRange.Max, expected.min, expected.max)
			}
			done <- struct{}{}
			ctx.Results <- &query.Result{}
			return nil
		},
	}

	s.Open()
	defer s.Close()

	// Set the 'now' time to the start of a 10 minute interval with a 30 second offset.
	// Then trigger a run. This should trigger two queries (one for the current time
	// interval, one for the previous).
	now := time.Now().UTC().Truncate(10 * time.Minute).Add(30 * time.Second)
	expected.min = now.Add(-time.Minute)
	expected.max = now.Add(-1)
	s.RunCh <- &RunRequest{Now: now}

	if err := wait(done, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// Test service when not the cluster leader (CQs shouldn't run).
func TestContinuousQueryService_NotLeader(t *testing.T) {
	s := NewTestService(t)
	// Set RunInterval high so we can test triggering with the RunCh below.
	s.RunInterval = 10 * time.Second
	s.MetaClient.(*MetaClient).Leader = false

	done := make(chan struct{})
	// Set a callback for ExecuteStatement. Shouldn't get called because we're not the leader.
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			done <- struct{}{}
			ctx.Results <- &query.Result{Err: errUnexpected}
			return nil
		},
	}

	s.Open()
	// Trigger service to run CQs.
	s.RunCh <- &RunRequest{Now: time.Now()}
	// Expect timeout error because ExecuteQuery callback wasn't called.
	if err := wait(done, 100*time.Millisecond); err == nil {
		t.Error(err)
	}
	s.Close()
}

// Test ExecuteContinuousQuery with invalid queries.
func TestExecuteContinuousQuery_InvalidQueries(t *testing.T) {
	s := NewTestService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			return errUnexpected
		},
	}
	dbis := s.MetaClient.Databases()
	dbi := dbis[0]
	cqi := dbi.ContinuousQueries[0]

	cqi.Query = `this is not a query`
	if _, err := s.ExecuteContinuousQuery(&dbi, &cqi, time.Now()); err == nil {
		t.Error("expected error but got nil")
	}

	// Valid query but invalid continuous query.
	cqi.Query = `SELECT * FROM cpu`
	if _, err := s.ExecuteContinuousQuery(&dbi, &cqi, time.Now()); err == nil {
		t.Error("expected error but got nil")
	}

	// Group by requires aggregate.
	cqi.Query = `SELECT value INTO other_value FROM cpu WHERE time > now() - 1h GROUP BY time(1s)`
	if _, err := s.ExecuteContinuousQuery(&dbi, &cqi, time.Now()); err == nil {
		t.Error("expected error but got nil")
	}
}

// Test the time range for different CQ durations.
func TestExecuteContinuousQuery_TimeRange(t *testing.T) {
	// Choose a start date that is not on an interval border for anyone.
	now := mustParseTime(t, "2000-01-01T00:00:00Z")
	for _, tt := range []struct {
		d          string
		start, end time.Time
	}{
		{
			d:     "10s",
			start: mustParseTime(t, "2000-01-01T00:00:00Z"),
			end:   mustParseTime(t, "2000-01-01T00:00:10Z"),
		},
		{
			d:     "1m",
			start: mustParseTime(t, "2000-01-01T00:00:00Z"),
			end:   mustParseTime(t, "2000-01-01T00:01:00Z"),
		},
		{
			d:     "10m",
			start: mustParseTime(t, "2000-01-01T00:00:00Z"),
			end:   mustParseTime(t, "2000-01-01T00:10:00Z"),
		},
		{
			d:     "30m",
			start: mustParseTime(t, "2000-01-01T00:00:00Z"),
			end:   mustParseTime(t, "2000-01-01T00:30:00Z"),
		},
		{
			d:     "1h",
			start: mustParseTime(t, "2000-01-01T00:00:00Z"),
			end:   mustParseTime(t, "2000-01-01T01:00:00Z"),
		},
		{
			d:     "2h",
			start: mustParseTime(t, "2000-01-01T00:00:00Z"),
			end:   mustParseTime(t, "2000-01-01T02:00:00Z"),
		},
		{
			d:     "12h",
			start: mustParseTime(t, "2000-01-01T00:00:00Z"),
			end:   mustParseTime(t, "2000-01-01T12:00:00Z"),
		},
		{
			d:     "1d",
			start: mustParseTime(t, "2000-01-01T00:00:00Z"),
			end:   mustParseTime(t, "2000-01-02T00:00:00Z"),
		},
		{
			d:     "1w",
			start: mustParseTime(t, "1999-12-30T00:00:00Z"),
			end:   mustParseTime(t, "2000-01-06T00:00:00Z"),
		},
	} {
		t.Run(tt.d, func(t *testing.T) {
			d, err := influxql.ParseDuration(tt.d)
			if err != nil {
				t.Fatalf("unable to parse duration: %s", err)
			}

			s := NewTestService(t)
			mc := NewMetaClient(t)
			mc.CreateDatabase("db", "")
			mc.CreateContinuousQuery("db", "cq",
				fmt.Sprintf(`CREATE CONTINUOUS QUERY cq ON db BEGIN SELECT mean(value) INTO cpu_mean FROM cpu GROUP BY time(%s) END`, tt.d))
			s.MetaClient = mc

			// Set RunInterval high so we can trigger using Run method.
			s.RunInterval = 10 * time.Minute
			done := make(chan struct{})

			// Set a callback for ExecuteStatement.
			s.QueryExecutor.StatementExecutor = &StatementExecutor{
				ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
					s := stmt.(*influxql.SelectStatement)
					valuer := &influxql.NowValuer{Location: s.Location}
					_, timeRange, err := influxql.ConditionExpr(s.Condition, valuer)
					timeRange.Max = timeRange.Max.Add(time.Nanosecond)
					if err != nil {
						t.Errorf("unexpected error parsing time range: %s", err)
					} else if !tt.start.Equal(timeRange.Min) || !tt.end.Equal(timeRange.Max) {
						t.Errorf("mismatched time range: got=(%s, %s) exp=(%s, %s)", timeRange.Min, timeRange.Max, tt.start, tt.end)
					}
					done <- struct{}{}
					ctx.Results <- &query.Result{}
					return nil
				},
			}

			s.Open()
			defer s.Close()

			// Send an initial run request one nanosecond after the start to
			// prime the last CQ map.
			s.RunCh <- &RunRequest{Now: now.Add(time.Nanosecond)}
			// Execute the real request after the time interval.
			s.RunCh <- &RunRequest{Now: now.Add(d)}
			if err := wait(done, 100*time.Millisecond); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Test the time range for different CQ durations.
func TestExecuteContinuousQuery_TimeZone(t *testing.T) {
	type test struct {
		now        time.Time
		start, end time.Time
	}

	// Choose a start date that is not on an interval border for anyone.
	for _, tt := range []struct {
		name    string
		d       string
		options string
		initial time.Time
		tests   []test
	}{
		{
			name:    "DaylightSavingsStart/1d",
			d:       "1d",
			initial: mustParseTime(t, "2000-04-02T00:00:00-05:00"),
			tests: []test{
				{
					start: mustParseTime(t, "2000-04-02T00:00:00-05:00"),
					end:   mustParseTime(t, "2000-04-03T00:00:00-04:00"),
				},
			},
		},
		{
			name:    "DaylightSavingsStart/2h",
			d:       "2h",
			initial: mustParseTime(t, "2000-04-02T00:00:00-05:00"),
			tests: []test{
				{
					start: mustParseTime(t, "2000-04-02T00:00:00-05:00"),
					end:   mustParseTime(t, "2000-04-02T03:00:00-04:00"),
				},
				{
					start: mustParseTime(t, "2000-04-02T03:00:00-04:00"),
					end:   mustParseTime(t, "2000-04-02T04:00:00-04:00"),
				},
			},
		},
		{
			name:    "DaylightSavingsEnd/1d",
			d:       "1d",
			initial: mustParseTime(t, "2000-10-29T00:00:00-04:00"),
			tests: []test{
				{
					start: mustParseTime(t, "2000-10-29T00:00:00-04:00"),
					end:   mustParseTime(t, "2000-10-30T00:00:00-05:00"),
				},
			},
		},
		{
			name:    "DaylightSavingsEnd/2h",
			d:       "2h",
			initial: mustParseTime(t, "2000-10-29T00:00:00-04:00"),
			tests: []test{
				{
					start: mustParseTime(t, "2000-10-29T00:00:00-04:00"),
					end:   mustParseTime(t, "2000-10-29T02:00:00-05:00"),
				},
				{
					start: mustParseTime(t, "2000-10-29T02:00:00-05:00"),
					end:   mustParseTime(t, "2000-10-29T04:00:00-05:00"),
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := NewTestService(t)
			mc := NewMetaClient(t)
			mc.CreateDatabase("db", "")
			mc.CreateContinuousQuery("db", "cq",
				fmt.Sprintf(`CREATE CONTINUOUS QUERY cq ON db %s BEGIN SELECT mean(value) INTO cpu_mean FROM cpu GROUP BY time(%s) TZ('America/New_York') END`, tt.options, tt.d))
			s.MetaClient = mc

			// Set RunInterval high so we can trigger using Run method.
			s.RunInterval = 10 * time.Minute
			done := make(chan struct{})

			// Set a callback for ExecuteStatement.
			tests := make(chan test, 1)
			s.QueryExecutor.StatementExecutor = &StatementExecutor{
				ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
					test := <-tests
					s := stmt.(*influxql.SelectStatement)
					valuer := &influxql.NowValuer{Location: s.Location}
					_, timeRange, err := influxql.ConditionExpr(s.Condition, valuer)
					timeRange.Max = timeRange.Max.Add(time.Nanosecond)
					if err != nil {
						t.Errorf("unexpected error parsing time range: %s", err)
					} else if !test.start.Equal(timeRange.Min) || !test.end.Equal(timeRange.Max) {
						t.Errorf("mismatched time range: got=(%s, %s) exp=(%s, %s)", timeRange.Min, timeRange.Max, test.start, test.end)
					}
					done <- struct{}{}
					ctx.Results <- &query.Result{}
					return nil
				},
			}

			s.Open()
			defer s.Close()

			// Send an initial run request one nanosecond after the start to
			// prime the last CQ map.
			s.RunCh <- &RunRequest{Now: tt.initial.Add(time.Nanosecond)}
			// Execute each of the tests and ensure the times are correct.
			for i, test := range tt.tests {
				tests <- test
				now := test.now
				if now.IsZero() {
					now = test.end
				}
				s.RunCh <- &RunRequest{Now: now}
				if err := wait(done, 100*time.Millisecond); err != nil {
					t.Fatal(fmt.Errorf("%d. %s", i+1, err))
				}
			}
		})
	}
}

// Test ExecuteContinuousQuery when QueryExecutor returns an error.
func TestExecuteContinuousQuery_QueryExecutor_Error(t *testing.T) {
	s := NewTestService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			return errExpected
		},
	}

	dbis := s.MetaClient.Databases()
	dbi := dbis[0]
	cqi := dbi.ContinuousQueries[0]

	now := time.Now().Truncate(10 * time.Minute)
	if _, err := s.ExecuteContinuousQuery(&dbi, &cqi, now); err != errExpected {
		t.Errorf("exp = %s, got = %v", errExpected, err)
	}
}

func TestService_ExecuteContinuousQuery_LogsToMonitor(t *testing.T) {
	s := NewTestService(t)
	const writeN = int64(50)

	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			ctx.Results <- &query.Result{
				Series: []*models.Row{{
					Name:    "result",
					Columns: []string{"time", "written"},
					Values:  [][]interface{}{{time.Time{}, writeN}},
				}},
			}
			return nil
		},
	}
	s.queryStatsEnabled = true
	var point models.Point
	s.Monitor = &monitor{
		EnabledFn: func() bool { return true },
		WritePointsFn: func(p models.Points) error {
			if len(p) != 1 {
				t.Fatalf("expected point")
			}
			point = p[0]
			return nil
		},
	}

	dbis := s.MetaClient.Databases()
	dbi := dbis[0]
	cqi := dbi.ContinuousQueries[0]

	now := time.Now().Truncate(10 * time.Minute)
	if ok, err := s.ExecuteContinuousQuery(&dbi, &cqi, now); !ok || err != nil {
		t.Fatalf("ExecuteContinuousQuery failed, ok=%t, err=%v", ok, err)
	}

	if point == nil {
		t.Fatal("expected Monitor.WritePoints call")
	}

	f, _ := point.Fields()
	if got, ok := f["pointsWrittenOK"].(int64); !ok || got != writeN {
		t.Errorf("unexpected value for written; exp=%d, got=%d", writeN, got)
	}
}

func TestService_ExecuteContinuousQuery_LogToMonitor_DisabledByDefault(t *testing.T) {
	s := NewTestService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			ctx.Send(&query.Result{})
			return nil
		},
	}
	s.Monitor = &monitor{
		EnabledFn: func() bool { return true },
		WritePointsFn: func(p models.Points) error {
			t.Fatalf("unexpected Monitor.WritePoints call")
			return nil
		},
	}

	dbis := s.MetaClient.Databases()
	dbi := dbis[0]
	cqi := dbi.ContinuousQueries[0]

	now := time.Now().Truncate(10 * time.Minute)
	if ok, err := s.ExecuteContinuousQuery(&dbi, &cqi, now); !ok || err != nil {
		t.Fatalf("ExecuteContinuousQuery failed, ok=%t, err=%v", ok, err)
	}
}

// NewTestService returns a new *Service with default mock object members.
func NewTestService(t *testing.T) *Service {
	s := NewService(NewConfig())
	ms := NewMetaClient(t)
	s.MetaClient = ms
	s.QueryExecutor = query.NewExecutor()
	s.RunInterval = time.Millisecond

	// Set Logger to write to dev/null so stdout isn't polluted.
	if testing.Verbose() {
		s.WithLogger(logger.New(os.Stderr))
	}

	// Add a couple test databases and CQs.
	ms.CreateDatabase("db", "rp")
	ms.CreateContinuousQuery("db", "cq", `CREATE CONTINUOUS QUERY cq ON db BEGIN SELECT count(cpu) INTO cpu_count FROM cpu WHERE time > now() - 1h GROUP BY time(1s) END`)
	ms.CreateDatabase("db2", "default")
	ms.CreateContinuousQuery("db2", "cq2", `CREATE CONTINUOUS QUERY cq2 ON db2 BEGIN SELECT mean(value) INTO cpu_mean FROM cpu WHERE time > now() - 10m GROUP BY time(1m) END`)
	ms.CreateDatabase("db3", "default")
	ms.CreateContinuousQuery("db3", "cq3", `CREATE CONTINUOUS QUERY cq3 ON db3 BEGIN SELECT mean(value) INTO "1hAverages".:MEASUREMENT FROM /cpu[0-9]?/ GROUP BY time(10s) END`)

	return s
}

// MetaClient is a mock meta store.
type MetaClient struct {
	mu            sync.RWMutex
	Leader        bool
	AllowLease    bool
	DatabaseInfos []meta.DatabaseInfo
	Err           error
	t             *testing.T
	nodeID        uint64
}

// NewMetaClient returns a *MetaClient.
func NewMetaClient(t *testing.T) *MetaClient {
	return &MetaClient{
		Leader:     true,
		AllowLease: true,
		t:          t,
		nodeID:     1,
	}
}

// NodeID returns the client's node ID.
func (ms *MetaClient) NodeID() uint64 { return ms.nodeID }

// AcquireLease attempts to acquire the specified lease.
func (ms *MetaClient) AcquireLease(name string) (l *meta.Lease, err error) {
	if ms.Leader {
		if ms.AllowLease {
			return &meta.Lease{Name: name}, nil
		}
		return nil, errors.New("another node owns the lease")
	}
	return nil, meta.ErrServiceUnavailable
}

// Databases returns a list of database info about each database in the coordinator.
func (ms *MetaClient) Databases() []meta.DatabaseInfo {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.DatabaseInfos
}

// Database returns a single database by name.
func (ms *MetaClient) Database(name string) *meta.DatabaseInfo {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.database(name)
}

func (ms *MetaClient) database(name string) *meta.DatabaseInfo {
	if ms.Err != nil {
		return nil
	}
	for i := range ms.DatabaseInfos {
		if ms.DatabaseInfos[i].Name == name {
			return &ms.DatabaseInfos[i]
		}
	}
	return nil
}

// CreateDatabase adds a new database to the meta store.
func (ms *MetaClient) CreateDatabase(name, defaultRetentionPolicy string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.Err != nil {
		return ms.Err
	}

	// See if the database already exists.
	for _, dbi := range ms.DatabaseInfos {
		if dbi.Name == name {
			return fmt.Errorf("database already exists: %s", name)
		}
	}

	// Create database.
	ms.DatabaseInfos = append(ms.DatabaseInfos, meta.DatabaseInfo{
		Name: name,

		DefaultRetentionPolicy: defaultRetentionPolicy,
	})

	return nil
}

// CreateContinuousQuery adds a CQ to the meta store.
func (ms *MetaClient) CreateContinuousQuery(database, name, query string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.Err != nil {
		return ms.Err
	}

	dbi := ms.database(database)
	if dbi == nil {
		return fmt.Errorf("database not found: %s", database)
	}

	// See if CQ already exists.
	for _, cqi := range dbi.ContinuousQueries {
		if cqi.Name == name {
			return fmt.Errorf("continuous query already exists: %s", name)
		}
	}

	// Create a new CQ and store it.
	dbi.ContinuousQueries = append(dbi.ContinuousQueries, meta.ContinuousQueryInfo{
		Name:  name,
		Query: query,
	})

	return nil
}

// StatementExecutor is a mock statement executor.
type StatementExecutor struct {
	ExecuteStatementFn func(stmt influxql.Statement, ctx *query.ExecutionContext) error
}

func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx *query.ExecutionContext) error {
	return e.ExecuteStatementFn(stmt, ctx)
}

func wait(c chan struct{}, d time.Duration) (err error) {
	select {
	case <-c:
	case <-time.After(d):
		err = errors.New("timed out")
	}
	return
}

type monitor struct {
	EnabledFn     func() bool
	WritePointsFn func(models.Points) error
}

func (m *monitor) Enabled() bool                     { return m.EnabledFn() }
func (m *monitor) WritePoints(p models.Points) error { return m.WritePointsFn(p) }

func mustParseTime(t *testing.T, value string) time.Time {
	ts, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("unable to parse time: %s", err)
	}
	return ts
}
