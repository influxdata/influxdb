package mock

import (
	"context"
	"io"

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
