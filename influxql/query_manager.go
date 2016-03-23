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
}

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

type queryTask struct {
	query     string
	database  string
	startTime time.Time
	closing   chan struct{}
	once      sync.Once
}

type defaultQueryManager struct {
	queries    map[uint64]*queryTask
	nextID     uint64
	maxQueries int
	mu         sync.Mutex
}

func (qm *defaultQueryManager) AttachQuery(params *QueryParams) (uint64, <-chan struct{}, error) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if qm.maxQueries > 0 && len(qm.queries) >= qm.maxQueries {
		return 0, nil, ErrMaxConcurrentQueriesReached
	}

	qid := qm.nextID
	query := &queryTask{
		query:     params.Query.String(),
		database:  params.Database,
		startTime: time.Now(),
		closing:   make(chan struct{}),
	}
	qm.queries[qid] = query

	if params.InterruptCh != nil || params.Timeout != 0 {
		go qm.waitForQuery(qid, params.Timeout, params.InterruptCh)
	}
	qm.nextID++
	return qid, query.closing, nil
}

func (qm *defaultQueryManager) waitForQuery(qid uint64, timeout time.Duration, closing <-chan struct{}) {
	var timer <-chan time.Time
	if timeout != 0 {
		timer = time.After(timeout)
	}

	select {
	case <-closing:
	case <-timer:
	}
	qm.KillQuery(qid)
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
