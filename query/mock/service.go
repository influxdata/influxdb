package mock

import (
	"context"
	"io"

	"github.com/influxdata/platform/query"
)

// ProxyQueryService mocks the idep QueryService for testing.
type ProxyQueryService struct {
	QueryF func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (int64, error)
}

// Query writes the results of the query request.
func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (int64, error) {
	return s.QueryF(ctx, w, req)
}

// QueryService mocks the idep QueryService for testing.
type QueryService struct {
	QueryF func(ctx context.Context, req *query.Request) (query.ResultIterator, error)
}

// Query writes the results of the query request.
func (s *QueryService) Query(ctx context.Context, req *query.Request) (query.ResultIterator, error) {
	return s.QueryF(ctx, req)
}

// AsyncQueryService mocks the idep QueryService for testing.
type AsyncQueryService struct {
	QueryF func(ctx context.Context, req *query.Request) (query.Query, error)
}

// Query writes the results of the query request.
func (s *AsyncQueryService) Query(ctx context.Context, req *query.Request) (query.Query, error) {
	return s.QueryF(ctx, req)
}
