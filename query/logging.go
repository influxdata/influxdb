package query

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/kit/check"
	"github.com/influxdata/influxdb/kit/tracing"
)

// LoggingServiceBridge implements ProxyQueryService and logs the queries while consuming a QueryService interface.
type LoggingServiceBridge struct {
	QueryService QueryService
	QueryLogger  Logger
}

// Query executes and logs the query.
func (s *LoggingServiceBridge) Query(ctx context.Context, w io.Writer, req *ProxyRequest) (stats flux.Statistics, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var n int64
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
		log := Log{
			OrganizationID: req.Request.OrganizationID,
			ProxyRequest:   req,
			ResponseSize:   n,
			Time:           time.Now(),
			Statistics:     stats,
		}
		if err != nil {
			log.Error = err
		}
		s.QueryLogger.Log(log)
	}()

	results, err := s.QueryService.Query(ctx, &req.Request)
	if err != nil {
		return stats, tracing.LogError(span, err)
	}
	// Check if this result iterator reports stats. We call this defer before cancel because
	// the query needs to be finished before it will have valid statistics.
	defer func() {
		results.Release()
		stats = results.Statistics()
	}()

	encoder := req.Dialect.Encoder()
	n, err = encoder.Encode(w, results)
	if err != nil {
		return stats, tracing.LogError(span, err)
	}
	// The results iterator may have had an error independent of encoding errors.
	if err = results.Err(); err != nil {
		return stats, tracing.LogError(span, err)
	}
	return stats, nil
}

func (s *LoggingServiceBridge) Check(ctx context.Context) check.Response {
	return s.QueryService.Check(ctx)
}
