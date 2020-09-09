package mock

import (
	"context"
	"io"
	"sync"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/metadata"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/query"
)

// ProxyQueryService mocks the idpe QueryService for testing.
type ProxyQueryService struct {
	QueryF func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error)
}

// Query writes the results of the query request.
func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	return s.QueryF(ctx, w, req)
}

func (s *ProxyQueryService) Check(ctx context.Context) check.Response {
	return check.Response{Name: "Mock Proxy Query Service", Status: check.StatusPass}
}

// QueryService mocks the idep QueryService for testing.
type QueryService struct {
	QueryF func(ctx context.Context, req *query.Request) (flux.ResultIterator, error)
}

// Query writes the results of the query request.
func (s *QueryService) Query(ctx context.Context, req *query.Request) (flux.ResultIterator, error) {
	return s.QueryF(ctx, req)
}

func (s *QueryService) Check(ctx context.Context) check.Response {
	return check.Response{Name: "Mock Query Service", Status: check.StatusPass}
}

// AsyncQueryService mocks the idep QueryService for testing.
type AsyncQueryService struct {
	QueryF func(ctx context.Context, req *query.Request) (flux.Query, error)
}

// Query writes the results of the query request.
func (s *AsyncQueryService) Query(ctx context.Context, req *query.Request) (flux.Query, error) {
	return s.QueryF(ctx, req)
}

// Query is a mock implementation of a flux.Query.
// It contains controls to ensure that the flux.Query object is used correctly.
// Note: Query will only return one result, specified by calling the SetResults method.
type Query struct {
	Metadata metadata.Metadata

	results chan flux.Result
	once    sync.Once
	err     error
	mu      sync.Mutex
	done    bool
}

var _ flux.Query = (*Query)(nil)

// NewQuery constructs a new asynchronous query.
func NewQuery() *Query {
	return &Query{
		Metadata: make(metadata.Metadata),
		results:  make(chan flux.Result, 1),
	}
}

func (q *Query) SetResults(results flux.Result) *Query {
	q.results <- results
	q.once.Do(func() {
		close(q.results)
	})
	return q
}

func (q *Query) SetErr(err error) *Query {
	q.err = err
	q.Cancel()
	return q
}

func (q *Query) Results() <-chan flux.Result {
	return q.results
}
func (q *Query) ProfilerResults() (flux.ResultIterator, error) {
	return nil, nil
}

func (q *Query) Done() {
	q.Cancel()

	q.mu.Lock()
	q.done = true
	q.mu.Unlock()
}

// Cancel closes the results channel.
func (q *Query) Cancel() {
	q.once.Do(func() {
		close(q.results)
	})
}

// Err will return an error if one was set.
func (q *Query) Err() error {
	return q.err
}

// Statistics will return Statistics. Unlike the normal flux.Query, this
// will panic if it is called before Done.
func (q *Query) Statistics() flux.Statistics {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.done {
		panic("call to query.Statistics() before the query has been finished")
	}
	return flux.Statistics{
		Metadata: q.Metadata,
	}
}
