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
	cond              func(ctx context.Context) bool

	// If this is set then logging happens only if this key is present in the
	// metadata.
	requireMetadataKey string
}

// LoggingProxyQueryServiceOption provides a way to modify the
// behavior of LoggingProxyQueryService.
type LoggingProxyQueryServiceOption func(lpqs *LoggingProxyQueryService)

// ConditionalLogging returns a LoggingProxyQueryServiceOption
// that only logs if the passed in function returns true.
// Thus logging can be controlled by a request-scoped attribute, e.g., a feature flag.
func ConditionalLogging(cond func(context.Context) bool) LoggingProxyQueryServiceOption {
	return func(lpqs *LoggingProxyQueryService) {
		lpqs.cond = cond
	}
}

func RequireMetadataKey(metadataKey string) LoggingProxyQueryServiceOption {
	return func(lpqs *LoggingProxyQueryService) {
		lpqs.requireMetadataKey = metadataKey
	}
}

func NewLoggingProxyQueryService(log *zap.Logger, queryLogger Logger, proxyQueryService ProxyQueryService, opts ...LoggingProxyQueryServiceOption) *LoggingProxyQueryService {
	lpqs := &LoggingProxyQueryService{
		proxyQueryService: proxyQueryService,
		queryLogger:       queryLogger,
		nowFunction:       time.Now,
		log:               log,
	}

	for _, o := range opts {
		o(lpqs)
	}

	return lpqs
}

func (s *LoggingProxyQueryService) SetNowFunctionForTesting(nowFunction func() time.Time) {
	s.nowFunction = nowFunction
}

// Query executes and logs the query.
func (s *LoggingProxyQueryService) Query(ctx context.Context, w io.Writer, req *ProxyRequest) (stats flux.Statistics, err error) {
	if s.cond != nil && !s.cond(ctx) {
		// Logging is conditional, and we are not logging this request.
		// Just invoke the wrapped service directly.
		return s.proxyQueryService.Query(ctx, w, req)
	}

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

		// Enforce requireMetadataKey, if set.
		if s.requireMetadataKey != "" {
			if _, ok := stats.Metadata[s.requireMetadataKey]; !ok {
				return
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
