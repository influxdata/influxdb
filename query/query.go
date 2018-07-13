package query

import (
	"context"
	"sort"
	"time"

	"github.com/influxdata/platform"
)

// QueryService represents a service for performing queries.
type QueryService interface {
	// Query submits a query spec for execution returning a results iterator.
	Query(ctx context.Context, orgID platform.ID, query *Spec) (ResultIterator, error)
	// Query submits a query string for execution returning a results iterator.
	QueryWithCompile(ctx context.Context, orgID platform.ID, query string) (ResultIterator, error)
}

// ResultIterator allows iterating through all results
// ResultIterators may implement Statisticser.
type ResultIterator interface {
	// More indicates if there are more results.
	// More must be called until it returns false in order to free all resources.
	More() bool

	// Next returns the next result.
	// If More is false, Next panics.
	Next() Result

	// Cancel discards the remaining results.
	// If not all results are going to be read, Cancel must be called to free resources.
	Cancel()

	// Err reports the first error encountered.
	Err() error
}

// Statisticser reports statisitcs about query processing.
type Statisticser interface {
	// Statistics reports the statisitcs for the query.
	// The statisitcs are not complete until the query is finished.
	Statistics() Statistics
}

// Statistics is a collection of statisitcs about the processing of a query.
type Statistics struct {
	// TotalDuration is the total amount of time in nanoseconds spent.
	TotalDuration time.Duration `json:"total_duration"`
	// CompileDuration is the amount of time in nanoseconds spent compiling the query.
	CompileDuration time.Duration `json:"compile_duration"`
	// QueueDuration is the amount of time in nanoseconds spent queueing.
	QueueDuration time.Duration `json:"queue_duration"`
	// PlanDuration is the amount of time in nanoseconds spent in plannig the query.
	PlanDuration time.Duration `json:"plan_duration"`
	// RequeueDuration is the amount of time in nanoseconds spent requeueing.
	RequeueDuration time.Duration `json:"requeue_duration"`
	// ExecuteDuration is the amount of time in nanoseconds spent in executing the query.
	ExecuteDuration time.Duration `json:"execute_duration"`

	// Concurrency is the number of goroutines allocated to process the query
	Concurrency int `json:"concurrency"`
	// MaxAllocated is the maximum number of bytes the query allocated.
	MaxAllocated int64 `json:"max_allocated"`
}

// AsyncQueryService represents a service for performing queries where the results are delivered asynchronously.
type AsyncQueryService interface {
	// Query submits a query for execution returning immediately.
	// The spec must not be modified while the query is still active.
	// Done must be called on any returned Query objects.
	Query(ctx context.Context, orgID platform.ID, query *Spec) (Query, error)

	// QueryWithCompile submits a query for execution returning immediately.
	// The query string will be compiled before submitting for execution.
	// Done must be called on returned Query objects.
	QueryWithCompile(ctx context.Context, orgID platform.ID, query string) (Query, error)
}

// Query represents an active query.
type Query interface {
	// Spec returns the spec used to execute this query.
	// Spec must not be modified.
	Spec() *Spec

	// Ready returns a channel that will deliver the query results.
	// Its possible that the channel is closed before any results arrive,
	// in which case the query should be inspected for an error using Err().
	Ready() <-chan map[string]Result

	// Done must always be called to free resources.
	Done()

	// Cancel will stop the query execution.
	// Done must still be called to free resources.
	Cancel()

	// Err reports any error the query may have encountered.
	Err() error

	Statisticser
}

// QueryServiceBridge implements the QueryService interface while consuming the AsyncQueryService interface.
type QueryServiceBridge struct {
	AsyncQueryService AsyncQueryService
}

func (b QueryServiceBridge) Query(ctx context.Context, orgID platform.ID, spec *Spec) (ResultIterator, error) {
	query, err := b.AsyncQueryService.Query(ctx, orgID, spec)
	if err != nil {
		return nil, err
	}
	return newResultIterator(query), nil
}
func (b QueryServiceBridge) QueryWithCompile(ctx context.Context, orgID platform.ID, queryStr string) (ResultIterator, error) {
	query, err := b.AsyncQueryService.QueryWithCompile(ctx, orgID, queryStr)
	if err != nil {
		return nil, err
	}
	return newResultIterator(query), nil
}

// resultIterator implements a ResultIterator while consuming a Query
type resultIterator struct {
	query   Query
	cancel  chan struct{}
	ready   bool
	results *MapResultIterator
}

func newResultIterator(q Query) *resultIterator {
	return &resultIterator{
		query:  q,
		cancel: make(chan struct{}),
	}
}

func (r *resultIterator) More() bool {
	if !r.ready {
		select {
		case <-r.cancel:
			goto DONE
		case results, ok := <-r.query.Ready():
			if !ok {
				goto DONE
			}
			r.ready = true
			r.results = NewMapResultIterator(results)
		}
	}
	if r.results.More() {
		return true
	}

DONE:
	r.query.Done()
	return false
}

func (r *resultIterator) Next() Result {
	return r.results.Next()
}

func (r *resultIterator) Cancel() {
	select {
	case <-r.cancel:
	default:
		close(r.cancel)
	}
	r.query.Cancel()
}

func (r *resultIterator) Err() error {
	return r.query.Err()
}

func (r *resultIterator) Statistics() Statistics {
	return r.query.Statistics()
}

type MapResultIterator struct {
	results map[string]Result
	order   []string
}

func NewMapResultIterator(results map[string]Result) *MapResultIterator {
	order := make([]string, 0, len(results))
	for k := range results {
		order = append(order, k)
	}
	sort.Strings(order)
	return &MapResultIterator{
		results: results,
		order:   order,
	}
}

func (r *MapResultIterator) More() bool {
	return len(r.order) > 0
}

func (r *MapResultIterator) Next() Result {
	next := r.order[0]
	r.order = r.order[1:]
	return r.results[next]
}

func (r *MapResultIterator) Cancel() {

}

func (r *MapResultIterator) Err() error {
	return nil
}

type SliceResultIterator struct {
	results []Result
}

func NewSliceResultIterator(results []Result) *SliceResultIterator {
	return &SliceResultIterator{
		results: results,
	}
}

func (r *SliceResultIterator) More() bool {
	return len(r.results) > 0
}

func (r *SliceResultIterator) Next() Result {
	next := r.results[0]
	r.results = r.results[1:]
	return next
}

func (r *SliceResultIterator) Cancel() {
	r.results = nil
}

func (r *SliceResultIterator) Err() error {
	return nil
}
