package influxql

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
)

var (
	// ErrNoQueryManager is an error sent when a SHOW QUERIES or KILL QUERY
	// statement is issued with no query manager.
	ErrNoQueryManager = errors.New("no query manager available")

	// ErrQueryInterrupted is an error returned when the query is interrupted.
	ErrQueryInterrupted = errors.New("query interrupted")

	// ErrMaxConcurrentQueriesReached is an error when a query cannot be run
	// because the maximum number of queries has been reached.
	ErrMaxConcurrentQueriesReached = errors.New("max concurrent queries reached")

	// ErrQueryManagerShutdown is an error sent when the query cannot be
	// attached because it was previous shutdown.
	ErrQueryManagerShutdown = errors.New("query manager shutdown")

	// ErrMaxPointsReached is an error when a query hits the maximum number of
	// points.
	ErrMaxPointsReached = errors.New("max number of points reached")

	// ErrQueryTimeoutReached is an error when a query hits the timeout.
	ErrQueryTimeoutReached = errors.New("query timeout reached")
)

// QueryTaskInfo holds information about a currently running query.
type QueryTaskInfo struct {
	ID       uint64
	Query    string
	Database string
	Duration time.Duration
}

// QueryParams holds the parameters used for creating a new query.
type QueryParams struct {
	// The query to be tracked. Required.
	Query *Query

	// The database this query is being run in. Required.
	Database string

	// The timeout for automatically killing a query that hasn't finished. Optional.
	Timeout time.Duration

	// The channel to watch for when this query is interrupted or finished.
	// Not required, but highly recommended. If this channel is not set, the
	// query needs to be manually managed by the caller.
	InterruptCh <-chan struct{}

	// Holds any error thrown by the QueryManager if there is a problem while
	// executing the query. Optional.
	Error *QueryError
}

// QueryError is an error thrown by the QueryManager when there is a problem
// while executing the query.
type QueryError struct {
	err error
	mu  sync.Mutex
}

// Error returns any reason why the QueryManager may have
// terminated the query. If a query completed successfully,
// this value will be nil.
func (q *QueryError) Error() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.err
}

// QueryMonitorFunc is a function that will be called to check if a query
// is currently healthy. If the query needs to be interrupted for some reason,
// the error should be returned by this function.
type QueryMonitorFunc func(<-chan struct{}) error

type QueryManager interface {
	// AttachQuery attaches a running query to be managed by the query manager.
	// Returns the query id of the newly attached query or an error if it was
	// unable to assign a query id or attach the query to the query manager.
	// This function also returns a channel that will be closed when this
	// query finishes running.
	//
	// After a query finishes running, the system is free to reuse a query id.
	AttachQuery(params *QueryParams) (qid uint64, closing <-chan struct{}, err error)

	// KillQuery stops and removes a query from the query manager.
	// This method can be used to forcefully terminate a running query.
	KillQuery(qid uint64) error

	// MonitorQuery starts a new goroutine that will monitor a query.
	// The function will be passed in a channel to signal when the query has been
	// finished normally. If the function returns with an error and the query is
	// still running, the query will be terminated.
	//
	// Query managers that do not implement this functionality should return an error.
	MonitorQuery(qid uint64, fn QueryMonitorFunc) error

	// Close kills all running queries and prevents new queries from being attached.
	Close() error

	// Queries lists the currently running tasks.
	Queries() []QueryTaskInfo
}

func DefaultQueryManager(maxQueries int) QueryManager {
	return &defaultQueryManager{
		queries:    make(map[uint64]*queryTask),
		nextID:     1,
		maxQueries: maxQueries,
	}
}

func ExecuteShowQueriesStatement(qm QueryManager, q *ShowQueriesStatement) (models.Rows, error) {
	if qm == nil {
		return nil, ErrNoQueryManager
	}

	queries := qm.Queries()
	values := make([][]interface{}, len(queries))
	for i, qi := range queries {
		var d string
		if qi.Duration == 0 {
			d = "0s"
		} else if qi.Duration < time.Second {
			d = fmt.Sprintf("%du", qi.Duration)
		} else {
			d = (qi.Duration - (qi.Duration % time.Second)).String()
		}
		values[i] = []interface{}{
			qi.ID,
			qi.Query,
			qi.Database,
			d,
		}
	}

	return []*models.Row{{
		Columns: []string{"qid", "query", "database", "duration"},
		Values:  values,
	}}, nil
}

// queryTask is the internal data structure for managing queries.
// For the public use data structure that gets returned, see QueryTask.
type queryTask struct {
	query     string
	database  string
	startTime time.Time
	closing   chan struct{}
	monitorCh chan error
	err       *QueryError
	once      sync.Once
}

func (q *queryTask) setError(err error) {
	if q.err != nil {
		q.err.mu.Lock()
		defer q.err.mu.Unlock()
		q.err.err = err
	}
}

func (q *queryTask) monitor(fn QueryMonitorFunc) {
	if err := fn(q.closing); err != nil {
		select {
		case <-q.closing:
		case q.monitorCh <- err:
		}
	}
}

type defaultQueryManager struct {
	queries    map[uint64]*queryTask
	nextID     uint64
	maxQueries int
	mu         sync.Mutex
	shutdown   bool
}

func (qm *defaultQueryManager) AttachQuery(params *QueryParams) (uint64, <-chan struct{}, error) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if qm.shutdown {
		return 0, nil, ErrQueryManagerShutdown
	}

	if qm.maxQueries > 0 && len(qm.queries) >= qm.maxQueries {
		return 0, nil, ErrMaxConcurrentQueriesReached
	}

	qid := qm.nextID
	query := &queryTask{
		query:     params.Query.String(),
		database:  params.Database,
		startTime: time.Now(),
		closing:   make(chan struct{}),
		monitorCh: make(chan error),
		err:       params.Error,
	}
	qm.queries[qid] = query

	go qm.waitForQuery(qid, query.closing, params.Timeout, params.InterruptCh, query.monitorCh)
	qm.nextID++
	return qid, query.closing, nil
}

func (qm *defaultQueryManager) Query(qid uint64) (*queryTask, bool) {
	qm.mu.Lock()
	query, ok := qm.queries[qid]
	qm.mu.Unlock()
	return query, ok
}

func (qm *defaultQueryManager) waitForQuery(qid uint64, interrupt <-chan struct{}, timeout time.Duration, closing <-chan struct{}, monitorCh <-chan error) {
	var timer <-chan time.Time
	if timeout != 0 {
		t := time.NewTimer(timeout)
		timer = t.C
		defer t.Stop()
	}

	select {
	case <-closing:
		query, ok := qm.Query(qid)
		if !ok {
			break
		}
		query.setError(ErrQueryInterrupted)
	case err := <-monitorCh:
		if err == nil {
			break
		}

		query, ok := qm.Query(qid)
		if !ok {
			break
		}
		query.setError(err)
	case <-timer:
		query, ok := qm.Query(qid)
		if !ok {
			break
		}
		query.setError(ErrQueryTimeoutReached)
	case <-interrupt:
		// Query was manually closed so exit the select.
		return
	}
	qm.KillQuery(qid)
}

func (qm *defaultQueryManager) MonitorQuery(qid uint64, fn QueryMonitorFunc) error {
	query, ok := qm.Query(qid)
	if !ok {
		return fmt.Errorf("no such query id: %d", qid)
	}
	go query.monitor(fn)
	return nil
}

func (qm *defaultQueryManager) KillQuery(qid uint64) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	query, ok := qm.queries[qid]
	if !ok {
		return fmt.Errorf("no such query id: %d", qid)
	}

	close(query.closing)
	delete(qm.queries, qid)
	return nil
}

func (qm *defaultQueryManager) Close() error {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	qm.shutdown = true
	for _, query := range qm.queries {
		query.setError(ErrQueryManagerShutdown)
		close(query.closing)
	}
	qm.queries = nil
	return nil
}

func (qm *defaultQueryManager) Queries() []QueryTaskInfo {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	now := time.Now()

	queries := make([]QueryTaskInfo, 0, len(qm.queries))
	for qid, task := range qm.queries {
		queries = append(queries, QueryTaskInfo{
			ID:       qid,
			Query:    task.query,
			Database: task.database,
			Duration: now.Sub(task.startTime),
		})
	}
	return queries
}
