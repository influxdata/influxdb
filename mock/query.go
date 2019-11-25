package mock

import (
	"context"
	"io"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/kit/check"
	"github.com/influxdata/influxdb/query"
)

// ProxyQueryService performs queries and encodes the result into a writer.
// The results are opaque to a ProxyQueryService.
type ProxyQueryService struct {
	CheckFn func(ctx context.Context) check.Response

	// Query performs the requested query and encodes the results into w.
	// The number of bytes written to w is returned __independent__ of any error.
	QueryFn func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error)

	// Value that will be written to the io writer that QueryFn is provided.
	Response string
}

// NewQueryService returns a mock ProxyQueryService
func NewQueryService() *ProxyQueryService {
	return &ProxyQueryService{
		CheckFn: func(ctx context.Context) check.Response {
			return check.Response{}
		},
		QueryFn: func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
			return flux.Statistics{}, nil
		},
	}
}

// Check indicates a service whose health can be checked.
func (s *ProxyQueryService) Check(ctx context.Context) check.Response {
	return s.CheckFn(ctx)
}

// Query submits a query for execution returning a results iterator.
// Cancel must be called on any returned results to free resources.
func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	if _, err := w.Write([]byte(s.Response)); err != nil {
		return flux.Statistics{}, nil
	}
	return s.QueryFn(ctx, w, req)
}
