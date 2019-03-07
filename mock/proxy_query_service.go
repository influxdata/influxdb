package mock

import (
	"context"
	"io"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/query"
)

var _ query.ProxyQueryService = (*ProxyQueryService)(nil)

// ProxyQueryService is a mock implementation of a query.ProxyQueryService.
type ProxyQueryService struct {
	QueryFn func(context.Context, io.Writer, *query.ProxyRequest) (flux.Statistics, error)
}

// NewProxyQueryService returns a mock of ProxyQueryService where its methods will return zero values.
func NewProxyQueryService() *ProxyQueryService {
	return &ProxyQueryService{
		QueryFn: func(context.Context, io.Writer, *query.ProxyRequest) (flux.Statistics, error) {
			return flux.Statistics{}, nil
		},
	}
}

// Query performs the requested query and encodes the results into w.
func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	return s.QueryFn(ctx, w, req)
}
