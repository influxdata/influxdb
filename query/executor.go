package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")

	// ErrQueryInterrupted is an error returned when the query is interrupted.
	ErrQueryInterrupted = errors.New("query interrupted")

	// ErrQueryAborted is an error returned when the query is aborted.
	ErrQueryAborted = errors.New("query aborted")

	// ErrQueryEngineShutdown is an error sent when the query cannot be
	// created because the query engine was shutdown.
	ErrQueryEngineShutdown = errors.New("query engine shutdown")

	// ErrQueryTimeoutLimitExceeded is an error when a query hits the max time allowed to run.
	ErrQueryTimeoutLimitExceeded = errors.New("query-timeout limit exceeded")

	// ErrAlreadyKilled is returned when attempting to kill a query that has already been killed.
	ErrAlreadyKilled = errors.New("already killed")
)

// Statistics for the Executor
const (
	statQueriesActive          = "queriesActive"   // Number of queries currently being executed.
	statQueriesExecuted        = "queriesExecuted" // Number of queries that have been executed (started).
	statQueriesFinished        = "queriesFinished" // Number of queries that have finished.
	statQueryExecutionDuration = "queryDurationNs" // Total (wall) time spent executing queries.
	statRecoveredPanics        = "recoveredPanics" // Number of panics recovered by Query Executor.

	// PanicCrashEnv is the environment variable that, when set, will prevent
	// the handler from recovering any panics.
	PanicCrashEnv = "INFLUXDB_PANIC_CRASH"
)

// ErrDatabaseNotFound returns a database not found error for the given database name.
func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

// ErrMaxSelectPointsLimitExceeded is an error when a query hits the maximum number of points.
func ErrMaxSelectPointsLimitExceeded(n, limit int) error {
	return fmt.Errorf("max-select-point limit exceeed: (%d/%d)", n, limit)
}

// ErrMaxConcurrentQueriesLimitExceeded is an error when a query cannot be run
// because the maximum number of queries has been reached.
func ErrMaxConcurrentQueriesLimitExceeded(n, limit int) error {
	return fmt.Errorf("max-concurrent-queries limit exceeded(%d, %d)", n, limit)
}

// Authorizer determines if certain operations are authorized.
type Authorizer interface {
	// AuthorizeDatabase indicates whether the given Privilege is authorized on the database with the given name.
	AuthorizeDatabase(p influxql.Privilege, name string) bool

	// AuthorizeQuery returns an error if the query cannot be executed
	AuthorizeQuery(database string, query *influxql.Query) error

	// AuthorizeSeriesRead determines if a series is authorized for reading
	AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool

	// AuthorizeSeriesWrite determines if a series is authorized for writing
	AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool
}

// OpenAuthorizer is the Authorizer used when authorization is disabled.
// It allows all operations.
type openAuthorizer struct{}

// OpenAuthorizer can be shared by all goroutines.
var OpenAuthorizer = openAuthorizer{}

// AuthorizeDatabase returns true to allow any operation on a database.
func (a openAuthorizer) AuthorizeDatabase(influxql.Privilege, string) bool { return true }

// AuthorizeSeriesRead allows accesss to any series.
func (a openAuthorizer) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesWrite allows accesss to any series.
func (a openAuthorizer) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesRead allows any query to execute.
func (a openAuthorizer) AuthorizeQuery(_ string, _ *influxql.Query) error { return nil }

// AuthorizerIsOpen returns true if the provided Authorizer is guaranteed to
// authorize anything. A nil Authorizer returns true for this function, and this
// function should be preferred over directly checking if an Authorizer is nil
// or not.
func AuthorizerIsOpen(a Authorizer) bool {
	return a == nil || a == OpenAuthorizer
}

// ExecutionOptions contains the options for executing a query.
type ExecutionOptions struct {
	// The database the query is running against.
	Database string

	// The retention policy the query is running against.
	RetentionPolicy string

	// How to determine whether the query is allowed to execute,
	// what resources can be returned in SHOW queries, etc.
	Authorizer Authorizer

	// The requested maximum number of points to return in each result.
	ChunkSize int

	// If this query is being executed in a read-only context.
	ReadOnly bool

	// Node to execute on.
	NodeID uint64

	// Quiet suppresses non-essential output from the query executor.
	Quiet bool

	// AbortCh is a channel that signals when results are no longer desired by the caller.
	AbortCh <-chan struct{}
}

type contextKey int

const (
	iteratorsContextKey contextKey = iota
	monitorContextKey
)

// NewContextWithIterators returns a new context.Context with the *Iterators slice added.
// The query planner will add instances of AuxIterator to the Iterators slice.
func NewContextWithIterators(ctx context.Context, itr *Iterators) context.Context {
	return context.WithValue(ctx, iteratorsContextKey, itr)
}

// StatementExecutor executes a statement within the Executor.
type StatementExecutor interface {
	// ExecuteStatement executes a statement. Results should be sent to the
	// results channel in the ExecutionContext.
	ExecuteStatement(stmt influxql.Statement, ctx *ExecutionContext) error
}

// StatementNormalizer normalizes a statement before it is executed.
type StatementNormalizer interface {
	// NormalizeStatement adds a default database and policy to the
	// measurements in the statement.
	NormalizeStatement(stmt influxql.Statement, database, retentionPolicy string) error
}

// Executor executes every statement in an Query.
type Executor struct {
	// Used for executing a statement in the query.
	StatementExecutor StatementExecutor

	// Used for tracking running queries.
	TaskManager *TaskManager

	// Logger to use for all logging.
	// Defaults to discarding all log output.
	Logger *zap.Logger

	// expvar-based stats.
	stats *Statistics
}

// NewExecutor returns a new instance of Executor.
func NewExecutor() *Executor {
	return &Executor{
		TaskManager: NewTaskManager(),
		Logger:      zap.NewNop(),
		stats:       &Statistics{},
	}
}

// Statistics keeps statistics related to the Executor.
type Statistics struct {
	ActiveQueries          int64
	ExecutedQueries        int64
	FinishedQueries        int64
	QueryExecutionDuration int64
	RecoveredPanics        int64
}

// Statistics returns statistics for periodic monitoring.
func (e *Executor) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "queryExecutor",
		Tags: tags,
		Values: map[string]interface{}{
			statQueriesActive:          atomic.LoadInt64(&e.stats.ActiveQueries),
			statQueriesExecuted:        atomic.LoadInt64(&e.stats.ExecutedQueries),
			statQueriesFinished:        atomic.LoadInt64(&e.stats.FinishedQueries),
			statQueryExecutionDuration: atomic.LoadInt64(&e.stats.QueryExecutionDuration),
			statRecoveredPanics:        atomic.LoadInt64(&e.stats.RecoveredPanics),
		},
	}}
}

// Close kills all running queries and prevents new queries from being attached.
func (e *Executor) Close() error {
	return e.TaskManager.Close()
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (e *Executor) WithLogger(log *zap.Logger) {
	e.Logger = log.With(zap.String("service", "query"))
	e.TaskManager.Logger = e.Logger
}

// ExecuteQuery executes each statement within a query.
func (e *Executor) ExecuteQuery(query *influxql.Query, opt ExecutionOptions, closing chan struct{}) <-chan *Result {
	results := make(chan *Result)
	go e.executeQuery(query, opt, closing, results)
	return results
}

func (e *Executor) executeQuery(query *influxql.Query, opt ExecutionOptions, closing <-chan struct{}, results chan *Result) {
	defer close(results)
	defer e.recover(query, results)

	atomic.AddInt64(&e.stats.ActiveQueries, 1)
	atomic.AddInt64(&e.stats.ExecutedQueries, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&e.stats.ActiveQueries, -1)
		atomic.AddInt64(&e.stats.FinishedQueries, 1)
		atomic.AddInt64(&e.stats.QueryExecutionDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	ctx, detach, err := e.TaskManager.AttachQuery(query, opt, closing)
	if err != nil {
		select {
		case results <- &Result{Err: err}:
		case <-opt.AbortCh:
		}
		return
	}
	defer detach()

	// Setup the execution context that will be used when executing statements.
	ctx.Results = results

	var i int
LOOP:
	for ; i < len(query.Statements); i++ {
		ctx.statementID = i
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		defaultDB := opt.Database
		if defaultDB == "" {
			if s, ok := stmt.(influxql.HasDefaultDatabase); ok {
				defaultDB = s.DefaultDatabase()
			}
		}

		// Do not let queries manually use the system measurements. If we find
		// one, return an error. This prevents a person from using the
		// measurement incorrectly and causing a panic.
		if stmt, ok := stmt.(*influxql.SelectStatement); ok {
			for _, s := range stmt.Sources {
				switch s := s.(type) {
				case *influxql.Measurement:
					if influxql.IsSystemName(s.Name) {
						command := "the appropriate meta command"
						switch s.Name {
						case "_fieldKeys":
							command = "SHOW FIELD KEYS"
						case "_measurements":
							command = "SHOW MEASUREMENTS"
						case "_series":
							command = "SHOW SERIES"
						case "_tagKeys":
							command = "SHOW TAG KEYS"
						case "_tags":
							command = "SHOW TAG VALUES"
						}
						results <- &Result{
							Err: fmt.Errorf("unable to use system source '%s': use %s instead", s.Name, command),
						}
						break LOOP
					}
				}
			}
		}

		// Rewrite statements, if necessary.
		// This can occur on meta read statements which convert to SELECT statements.
		newStmt, err := RewriteStatement(stmt)
		if err != nil {
			results <- &Result{Err: err}
			break
		}
		stmt = newStmt

		// Normalize each statement if possible.
		if normalizer, ok := e.StatementExecutor.(StatementNormalizer); ok {
			if err := normalizer.NormalizeStatement(stmt, defaultDB, opt.RetentionPolicy); err != nil {
				if err := ctx.send(&Result{Err: err}); err == ErrQueryAborted {
					return
				}
				break
			}
		}

		// Log each normalized statement.
		if !ctx.Quiet {
			e.Logger.Info("Executing query", zap.Stringer("query", stmt))
		}

		// Send any other statements to the underlying statement executor.
		err = e.StatementExecutor.ExecuteStatement(stmt, ctx)
		if err == ErrQueryInterrupted {
			// Query was interrupted so retrieve the real interrupt error from
			// the query task if there is one.
			if qerr := ctx.Err(); qerr != nil {
				err = qerr
			}
		}

		// Send an error for this result if it failed for some reason.
		if err != nil {
			if err := ctx.send(&Result{
				StatementID: i,
				Err:         err,
			}); err == ErrQueryAborted {
				return
			}
			// Stop after the first error.
			break
		}

		// Check if the query was interrupted during an uninterruptible statement.
		interrupted := false
		select {
		case <-ctx.Done():
			interrupted = true
		default:
			// Query has not been interrupted.
		}

		if interrupted {
			break
		}
	}

	// Send error results for any statements which were not executed.
	for ; i < len(query.Statements)-1; i++ {
		if err := ctx.send(&Result{
			StatementID: i,
			Err:         ErrNotExecuted,
		}); err == ErrQueryAborted {
			return
		}
	}
}

// Determines if the Executor will recover any panics or let them crash
// the server.
var willCrash bool

func init() {
	var err error
	if willCrash, err = strconv.ParseBool(os.Getenv(PanicCrashEnv)); err != nil {
		willCrash = false
	}
}

func (e *Executor) recover(query *influxql.Query, results chan *Result) {
	if err := recover(); err != nil {
		atomic.AddInt64(&e.stats.RecoveredPanics, 1) // Capture the panic in _internal stats.
		e.Logger.Error(fmt.Sprintf("%s [panic:%s] %s", query.String(), err, debug.Stack()))
		results <- &Result{
			StatementID: -1,
			Err:         fmt.Errorf("%s [panic:%s]", query.String(), err),
		}

		if willCrash {
			e.Logger.Error(fmt.Sprintf("\n\n=====\nAll goroutines now follow:"))
			buf := debug.Stack()
			e.Logger.Error(fmt.Sprintf("%s", buf))
			os.Exit(1)
		}
	}
}

// Task is the internal data structure for managing queries.
// For the public use data structure that gets returned, see Task.
type Task struct {
	query     string
	database  string
	status    TaskStatus
	startTime time.Time
	closing   chan struct{}
	monitorCh chan error
	err       error
	mu        sync.Mutex
}

// Monitor starts a new goroutine that will monitor a query. The function
// will be passed in a channel to signal when the query has been finished
// normally. If the function returns with an error and the query is still
// running, the query will be terminated.
func (q *Task) Monitor(fn MonitorFunc) {
	go q.monitor(fn)
}

// Error returns any asynchronous error that may have occured while executing
// the query.
func (q *Task) Error() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.err
}

func (q *Task) setError(err error) {
	q.mu.Lock()
	q.err = err
	q.mu.Unlock()
}

func (q *Task) monitor(fn MonitorFunc) {
	if err := fn(q.closing); err != nil {
		select {
		case <-q.closing:
		case q.monitorCh <- err:
		}
	}
}

// close closes the query task closing channel if the query hasn't been previously killed.
func (q *Task) close() {
	q.mu.Lock()
	if q.status != KilledTask {
		// Set the status to killed to prevent closing the channel twice.
		q.status = KilledTask
		close(q.closing)
	}
	q.mu.Unlock()
}

func (q *Task) kill() error {
	q.mu.Lock()
	if q.status == KilledTask {
		q.mu.Unlock()
		return ErrAlreadyKilled
	}
	q.status = KilledTask
	close(q.closing)
	q.mu.Unlock()
	return nil
}
