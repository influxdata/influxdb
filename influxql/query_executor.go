package influxql

import (
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
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

const (
	// DefaultQueryTimeout is the default timeout for executing a query.
	// A value of zero will have no query timeout.
	DefaultQueryTimeout = time.Duration(0)
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

	// The database the query is running against.
	Database string

	// The requested maximum number of points to return in each result.
	ChunkSize int

	// Hold the query executor's logger.
	Log *log.Logger

	// A channel that is closed when the query is interrupted.
	InterruptCh <-chan struct{}
}

// StatementExecutor executes a statement within the QueryExecutor.
type StatementExecutor interface {
	// ExecuteStatement executes a statement. Results should be sent to the
	// results channel in the ExecutionContext.
	ExecuteStatement(stmt Statement, ctx *ExecutionContext) error

	// NormalizeStatement adds a default database and policy to the
	// measurements in the statement.
	NormalizeStatement(stmt Statement, database string) error
}

// QueryExecutor executes every statement in an Query.
type QueryExecutor struct {
	// Used for executing a statement in the query.
	StatementExecutor StatementExecutor

	// Query execution timeout.
	QueryTimeout time.Duration

	// Log queries if they are slower than this time.
	// If zero, slow queries will never be logged.
	LogQueriesAfter time.Duration

	// Maximum number of concurrent queries.
	MaxConcurrentQueries int

	// Logger to use for all logging.
	// Defaults to discarding all log output.
	Logger *log.Logger

	// Used for managing and tracking running queries.
	queries  map[uint64]*QueryTask
	nextID   uint64
	mu       sync.RWMutex
	shutdown bool

	// expvar-based stats.
	statMap *expvar.Map
}

// NewQueryExecutor returns a new instance of QueryExecutor.
func NewQueryExecutor() *QueryExecutor {
	return &QueryExecutor{
		QueryTimeout: DefaultQueryTimeout,
		Logger:       log.New(ioutil.Discard, "[query] ", log.LstdFlags),
		queries:      make(map[uint64]*QueryTask),
		nextID:       1,
		statMap:      influxdb.NewStatistics("queryExecutor", "queryExecutor", nil),
	}
}

// Close kills all running queries and prevents new queries from being attached.
func (e *QueryExecutor) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.shutdown = true
	for _, query := range e.queries {
		query.setError(ErrQueryEngineShutdown)
		close(query.closing)
	}
	e.queries = nil
	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (e *QueryExecutor) SetLogOutput(w io.Writer) {
	e.Logger = log.New(w, "[query] ", log.LstdFlags)
}

// ExecuteQuery executes each statement within a query.
func (e *QueryExecutor) ExecuteQuery(query *Query, database string, chunkSize int, closing chan struct{}) <-chan *Result {
	results := make(chan *Result)
	go e.executeQuery(query, database, chunkSize, closing, results)
	return results
}

func (e *QueryExecutor) executeQuery(query *Query, database string, chunkSize int, closing <-chan struct{}, results chan *Result) {
	defer close(results)
	defer e.recover(query, results)

	e.statMap.Add(statQueriesActive, 1)
	defer func(start time.Time) {
		e.statMap.Add(statQueriesActive, -1)
		e.statMap.Add(statQueryExecutionDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	qid, task, err := e.attachQuery(query, database, closing)
	if err != nil {
		results <- &Result{Err: err}
		return
	}
	defer e.killQuery(qid)

	// Setup the execution context that will be used when executing statements.
	ctx := ExecutionContext{
		QueryID:     qid,
		Query:       task,
		Results:     results,
		Database:    database,
		ChunkSize:   chunkSize,
		Log:         e.Logger,
		InterruptCh: task.closing,
	}

	var i int
loop:
	for ; i < len(query.Statements); i++ {
		ctx.StatementID = i
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		defaultDB := database
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

		// Normalize each statement.
		if err := e.StatementExecutor.NormalizeStatement(stmt, defaultDB); err != nil {
			results <- &Result{Err: err}
			break
		}

		// Log each normalized statement.
		e.Logger.Println(stmt.String())

		// Handle a query management queries specially so they don't go
		// to the underlying statement executor.
		switch stmt := stmt.(type) {
		case *ShowQueriesStatement:
			rows, err := e.executeShowQueriesStatement(stmt)
			results <- &Result{
				StatementID: i,
				Series:      rows,
				Err:         err,
			}

			if err != nil {
				break loop
			}
			continue loop
		case *KillQueryStatement:
			err := e.executeKillQueryStatement(stmt)
			results <- &Result{
				StatementID: i,
				Err:         err,
			}

			if err != nil {
				break loop
			}
			continue loop
		}

		// Send any other statements to the underlying statement executor.
		err = e.StatementExecutor.ExecuteStatement(stmt, &ctx)
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
		results <- &Result{
			StatementID: -1,
			Err:         fmt.Errorf("%s [panic:%s]", query.String(), err),
		}
	}
}

func (e *QueryExecutor) executeKillQueryStatement(stmt *KillQueryStatement) error {
	return e.killQuery(stmt.QueryID)
}

func (e *QueryExecutor) executeShowQueriesStatement(q *ShowQueriesStatement) (models.Rows, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()

	values := make([][]interface{}, 0, len(e.queries))
	for id, qi := range e.queries {
		d := now.Sub(qi.startTime)

		var ds string
		if d == 0 {
			ds = "0s"
		} else if d < time.Second {
			ds = fmt.Sprintf("%du", d)
		} else {
			ds = (d - (d % time.Second)).String()
		}
		values = append(values, []interface{}{id, qi.query, qi.database, ds})
	}

	return []*models.Row{{
		Columns: []string{"qid", "query", "database", "duration"},
		Values:  values,
	}}, nil
}

func (e *QueryExecutor) query(qid uint64) (*QueryTask, bool) {
	e.mu.RLock()
	query, ok := e.queries[qid]
	e.mu.RUnlock()
	return query, ok
}

// attachQuery attaches a running query to be managed by the QueryExecutor.
// Returns the query id of the newly attached query or an error if it was
// unable to assign a query id or attach the query to the QueryExecutor.
// This function also returns a channel that will be closed when this
// query finishes running.
//
// After a query finishes running, the system is free to reuse a query id.
func (e *QueryExecutor) attachQuery(q *Query, database string, interrupt <-chan struct{}) (uint64, *QueryTask, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.shutdown {
		return 0, nil, ErrQueryEngineShutdown
	}

	if e.MaxConcurrentQueries > 0 && len(e.queries) >= e.MaxConcurrentQueries {
		return 0, nil, ErrMaxConcurrentQueriesReached
	}

	qid := e.nextID
	query := &QueryTask{
		query:     q.String(),
		database:  database,
		startTime: time.Now(),
		closing:   make(chan struct{}),
		monitorCh: make(chan error),
	}
	e.queries[qid] = query

	go e.waitForQuery(qid, query.closing, interrupt, query.monitorCh)
	if e.LogQueriesAfter != 0 {
		go query.monitor(func(closing <-chan struct{}) error {
			t := time.NewTimer(e.LogQueriesAfter)
			defer t.Stop()

			select {
			case <-t.C:
				e.Logger.Printf("Detected slow query: %s (qid: %d, database: %s, threshold: %s)",
					query.query, qid, query.database, e.LogQueriesAfter)
			case <-closing:
			}
			return nil
		})
	}
	e.nextID++
	return qid, query, nil
}

// killQuery stops and removes a query from the QueryExecutor.
// This method can be used to forcefully terminate a running query.
func (e *QueryExecutor) killQuery(qid uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	query, ok := e.queries[qid]
	if !ok {
		return fmt.Errorf("no such query id: %d", qid)
	}

	close(query.closing)
	delete(e.queries, qid)
	return nil
}

func (e *QueryExecutor) waitForQuery(qid uint64, interrupt <-chan struct{}, closing <-chan struct{}, monitorCh <-chan error) {
	var timer <-chan time.Time
	if e.QueryTimeout != 0 {
		t := time.NewTimer(e.QueryTimeout)
		timer = t.C
		defer t.Stop()
	}

	select {
	case <-closing:
		query, ok := e.query(qid)
		if !ok {
			break
		}
		query.setError(ErrQueryInterrupted)
	case err := <-monitorCh:
		if err == nil {
			break
		}

		query, ok := e.query(qid)
		if !ok {
			break
		}
		query.setError(err)
	case <-timer:
		query, ok := e.query(qid)
		if !ok {
			break
		}
		query.setError(ErrQueryTimeoutReached)
	case <-interrupt:
		// Query was manually closed so exit the select.
		return
	}
	e.killQuery(qid)
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
