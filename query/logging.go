package query

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/iocounter"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LoggingProxyQueryService wraps a ProxyQueryService and logs the queries.
type LoggingProxyQueryService struct {
	proxyQueryService ProxyQueryService
	queryLogger       Logger
	nowFunction       func() time.Time
	log               *zap.Logger
}

func NewLoggingProxyQueryService(log *zap.Logger, queryLogger Logger, proxyQueryService ProxyQueryService) *LoggingProxyQueryService {
	return &LoggingProxyQueryService{
		proxyQueryService: proxyQueryService,
		queryLogger:       queryLogger,
		nowFunction:       time.Now,
		log:               log,
	}
}

func (s *LoggingProxyQueryService) SetNowFunctionForTesting(nowFunction func() time.Time) {
	s.nowFunction = nowFunction
}

// Query executes and logs the query.
func (s *LoggingProxyQueryService) Query(ctx context.Context, w io.Writer, req *ProxyRequest) (stats flux.Statistics, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var n int64
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			if entry := s.log.Check(zapcore.InfoLevel, "QueryLogging panic"); entry != nil {
				entry.Stack = string(debug.Stack())
				entry.Write(zap.Error(err))
			}
		}
		traceID, sampled, _ := tracing.InfoFromContext(ctx)
		log := Log{
			OrganizationID: req.Request.OrganizationID,
			TraceID:        traceID,
			Sampled:        sampled,
			ProxyRequest:   req,
			ResponseSize:   n,
			Time:           s.nowFunction(),
			Statistics:     stats,
			Error:          err,
		}
		s.queryLogger.Log(log)
	}()

	wc := &iocounter.Writer{Writer: w}
	stats, err = s.proxyQueryService.Query(ctx, wc, req)
	if err != nil {
		return stats, tracing.LogError(span, err)
	}
	n = wc.Count()
	return stats, nil
}

func (s *LoggingProxyQueryService) Check(ctx context.Context) check.Response {
	return s.proxyQueryService.Check(ctx)
}
