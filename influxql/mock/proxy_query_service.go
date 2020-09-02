package mock

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/kit/check"
)

var _ influxql.ProxyQueryService = (*ProxyQueryService)(nil)

// ProxyQueryService mocks the InfluxQL QueryService for testing.
type ProxyQueryService struct {
	QueryF func(ctx context.Context, w io.Writer, req *influxql.QueryRequest) (influxql.Statistics, error)
}

func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *influxql.QueryRequest) (influxql.Statistics, error) {
	return s.QueryF(ctx, w, req)
}

func (s *ProxyQueryService) Check(ctx context.Context) check.Response {
	return check.Response{Name: "Mock InfluxQL Proxy Query Service", Status: check.StatusPass}
}
