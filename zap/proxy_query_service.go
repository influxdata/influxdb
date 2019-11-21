package zap

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/query"
	"go.uber.org/zap"
)

// ProxyQueryService logs the request but does not write to the writer.
type ProxyQueryService struct {
	logger *zap.Logger
}

// NewProxyQueryService creates a new proxy query service with a logger.
// If the logger is nil, then it will use a noop logger.
func NewProxyQueryService(logger *zap.Logger) *ProxyQueryService {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ProxyQueryService{
		logger: logger,
	}
}

// Query logs the query request.
func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (int64, error) {
	if req != nil {
		s.logger.Info("query", zap.Any("request", req))
	}
	n, err := w.Write([]byte{})
	return int64(n), err
}
