package influxql

import (
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
)

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")

	// ErrQueryInterrupted is an error returned when the query is interrupted.
	ErrQueryInterrupted = errors.New("query interrupted")

	// ErrMaxConcurrentQueriesReached is an error when a query cannot be run
	// because the maximum number of queries has been reached.
	ErrMaxConcurrentQueriesReached = errors.New("max concurrent queries reached")

	// ErrQueryEngineShutdown is an error sent when the query cannot be
	// created because the query engine was shutdown.
	ErrQueryEngineShutdown = errors.New("query engine shutdown")

	// ErrMaxPointsReached is an error when a query hits the maximum number of
	// points.
	ErrMaxPointsReached = errors.New("max number of points reached")

	// ErrQueryTimeoutReached is an error when a query hits the timeout.
	ErrQueryTimeoutReached = errors.New("query timeout reached")
)

// Statistics for the QueryExecutor
const (
	statQueriesActive          = "queriesActive"   // Number of queries currently being executed
	statQueryExecutionDuration = "queryDurationNs" // Total (wall) time spent executing queries
)

// ErrDatabaseNotFound returns a database not found error for the given database name.
func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

// ErrMeasurementNotFound returns a measurement not found error for the given measurement name.
func ErrMeasurementNotFound(name string) error { return fmt.Errorf("measurement not found: %s", name) }

// ExecutionOptions contains the options for executing a query.
type ExecutionOptions struct {
	// The database the query is running against.
	Database string

	// The requested maximum number of points to return in each result.
	ChunkSize int

	// If this query is being executed in a read-only context.
	ReadOnly bool

	// Node to execute on.
	NodeID uint64
}

// ExecutionContext contains state that the query is currently executing with.
type ExecutionContext struct {
	// The statement ID of the executing query.
	StatementID int

	// The query ID of the executing query.
	QueryID uint64

	// The query task information available to the StatementExecutor.
	Query *QueryTask

	// Output channel where results and errors should be sent.
	Results chan *Result

	// Hold the query executor's logger.
	Log *log.Logger

	// A channel that is closed when the query is interrupted.
	InterruptCh <-chan struct{}

	// Options used to start this query.
	ExecutionOptions
}

// StatementExecutor executes a statement within the QueryExecutor.
type StatementExecutor interface {
	// ExecuteStatement executes a statement. Results should be sent to the
	// results channel in the ExecutionContext.
	ExecuteStatement(stmt Statement, ctx ExecutionContext) error
}

// StatementNormalizer normalizes a statement before it is executed.
type StatementNormalizer interface {
	// NormalizeStatement adds a default database and policy to the
	// measurements in the statement.
	NormalizeStatement(stmt Statement, database string) error
}

// QueryExecutor executes every statement in an Query.
type QueryExecutor struct {
	// Used for executing a statement in the query.
	StatementExecutor StatementExecutor

	// Used for tracking running queries.
	TaskManager *TaskManager

	// Logger to use for all logging.
	// Defaults to discarding all log output.
	Logger *log.Logger

	// expvar-based stats.
	statMap *expvar.Map
}

// NewQueryExecutor returns a new instance of QueryExecutor.
func NewQueryExecutor() *QueryExecutor {
	return &QueryExecutor{
		TaskManager: NewTaskManager(),
		Logger:      log.New(ioutil.Discard, "[query] ", log.LstdFlags),
		statMap:     influxdb.NewStatistics("queryExecutor", "queryExecutor", nil),
	}
}

// Close kills all running queries and prevents new queries from being attached.
func (e *QueryExecutor) Close() error {
	return e.TaskManager.Close()
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (e *QueryExecutor) SetLogOutput(w io.Writer) {
	e.Logger = log.New(w, "[query] ", log.LstdFlags)
	e.TaskManager.Logger = e.Logger
}

// ExecuteQuery executes each statement within a query.
func (e *QueryExecutor) ExecuteQuery(query *Query, opt ExecutionOptions, closing chan struct{}) <-chan *Result {
	results := make(chan *Result)
	go e.executeQuery(query, opt, closing, results)
	return results
}

func (e *QueryExecutor) executeQuery(query *Query, opt ExecutionOptions, closing <-chan struct{}, results chan *Result) {
	defer close(results)
	defer e.recover(query, results)

	e.statMap.Add(statQueriesActive, 1)
	defer func(start time.Time) {
		e.statMap.Add(statQueriesActive, -1)
		e.statMap.Add(statQueryExecutionDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	qid, task, err := e.TaskManager.AttachQuery(query, opt.Database, closing)
	if err != nil {
		results <- &Result{Err: err}
		return
	}
	defer e.TaskManager.KillQuery(qid)

	// Setup the execution context that will be used when executing statements.
	ctx := ExecutionContext{
		QueryID:          qid,
		Query:            task,
		Results:          results,
		Log:              e.Logger,
		InterruptCh:      task.closing,
		ExecutionOptions: opt,
	}

	var i int
	for ; i < len(query.Statements); i++ {
		ctx.StatementID = i
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		defaultDB := opt.Database
		if defaultDB == "" {
			if s, ok := stmt.(HasDefaultDatabase); ok {
				defaultDB = s.DefaultDatabase()
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
			if err := normalizer.NormalizeStatement(stmt, defaultDB); err != nil {
				results <- &Result{Err: err}
				break
			}
		}

		// Log each normalized statement.
		e.Logger.Println(stmt.String())

		// Send any other statements to the underlying statement executor.
		err = e.StatementExecutor.ExecuteStatement(stmt, ctx)
		if err == ErrQueryInterrupted {
			// Query was interrupted so retrieve the real interrupt error from
			// the query task if there is one.
			if qerr := task.Error(); qerr != nil {
				err = qerr
			}
		}

		// Send an error for this result if it failed for some reason.
		if err != nil {
			results <- &Result{
				StatementID: i,
				Err:         err,
			}
			// Stop after the first error.
			break
		}
	}

	// Send error results for any statements which were not executed.
	for ; i < len(query.Statements)-1; i++ {
		results <- &Result{
			StatementID: i,
			Err:         ErrNotExecuted,
		}
	}
}

func (e *QueryExecutor) recover(query *Query, results chan *Result) {
	if err := recover(); err != nil {
		e.Logger.Printf("%s [panic:%s] %s", query.String(), err, debug.Stack())
		results <- &Result{
			StatementID: -1,
			Err:         fmt.Errorf("%s [panic:%s]", query.String(), err),
		}
	}
}

// QueryMonitorFunc is a function that will be called to check if a query
// is currently healthy. If the query needs to be interrupted for some reason,
// the error should be returned by this function.
type QueryMonitorFunc func(<-chan struct{}) error

// QueryTask is the internal data structure for managing queries.
// For the public use data structure that gets returned, see QueryTask.
type QueryTask struct {
	query     string
	database  string
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
func (q *QueryTask) Monitor(fn QueryMonitorFunc) {
	go q.monitor(fn)
}

// Error returns any asynchronous error that may have occured while executing
// the query.
func (q *QueryTask) Error() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.err
}

func (q *QueryTask) setError(err error) {
	q.mu.Lock()
	q.err = err
	q.mu.Unlock()
}

func (q *QueryTask) monitor(fn QueryMonitorFunc) {
	if err := fn(q.closing); err != nil {
		select {
		case <-q.closing:
		case q.monitorCh <- err:
		}
	}
}
