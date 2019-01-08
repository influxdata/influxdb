package zap

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/query"
	"go.uber.org/zap"
)

// ProxyQueryService logs the request but does not write to the writer.
type ProxyQueryService struct {
	Logger *zap.Logger
}

// NewProxyQueryService creates a new proxy query service with a logger.
// If the logger is nil, then it will use a noop logger.
func NewProxyQueryService(l *zap.Logger) *ProxyQueryService {
	if l == nil {
		l = zap.NewNop()
	}
	return &ProxyQueryService{
		Logger: l,
	}
}

// Query logs the query request.
func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (int64, error) {
	if req != nil {
		s.Logger.Info("query", zap.Any("request", req))
	}
	n, err := w.Write([]byte{})
	return int64(n), err
}
