package mock

import (
	"context"
	"io"
	"sync"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/kit/check"
	"github.com/influxdata/influxdb/query"
)

// ProxyQueryService mocks the idep QueryService for testing.
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
type Query struct {
	Metadata flux.Metadata

	spec  *flux.Spec
	ready chan map[string]flux.Result
	once  sync.Once
	err   error
	mu    sync.Mutex
	done  bool
}

var _ flux.Query = (*Query)(nil)

// NewQuery constructs a new asynchronous query.
func NewQuery(spec *flux.Spec) *Query {
	return &Query{
		Metadata: make(flux.Metadata),
		spec:     spec,
		ready:    make(chan map[string]flux.Result, 1),
	}
}

func (q *Query) SetResults(results map[string]flux.Result) *Query {
	q.ready <- results
	return q
}

func (q *Query) SetErr(err error) *Query {
	q.err = err
	q.Cancel()
	return q
}

func (q *Query) Spec() *flux.Spec {
	return q.spec
}

func (q *Query) Ready() <-chan map[string]flux.Result {
	return q.ready
}

func (q *Query) Done() {
	q.Cancel()

	q.mu.Lock()
	q.done = true
	q.mu.Unlock()
}

// Cancel closes the ready channel.
func (q *Query) Cancel() {
	q.once.Do(func() {
		close(q.ready)
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
