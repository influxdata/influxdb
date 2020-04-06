package zap

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/v2/query"
	"go.uber.org/zap"
)

// ProxyQueryService logs the request but does not write to the writer.
type ProxyQueryService struct {
	log *zap.Logger
}

// NewProxyQueryService creates a new proxy query service with a log.
// If the logger is nil, then it will use a noop logger.
func NewProxyQueryService(log *zap.Logger) *ProxyQueryService {
	return &ProxyQueryService{
		log: log,
	}
}

// Query logs the query request.
func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (int64, error) {
	if req != nil {
		s.log.Info("Query", zap.Any("request", req))
	}
	n, err := w.Write([]byte{})
	return int64(n), err
}
