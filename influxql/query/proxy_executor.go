package query

import (
	"context"
	"io"
	"strings"
	"time"

	iql "github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxql"
	"github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
)

type ProxyExecutor struct {
	log      *zap.Logger
	executor *Executor
}

func NewProxyExecutor(log *zap.Logger, executor *Executor) *ProxyExecutor {
	return &ProxyExecutor{log: log, executor: executor}
}

func (s *ProxyExecutor) Check(ctx context.Context) check.Response {
	return check.Response{Name: "Query Service", Status: check.StatusPass}
}

func (s *ProxyExecutor) Query(ctx context.Context, w io.Writer, req *iql.QueryRequest) (iql.Statistics, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	logger := s.log.With(influxlogger.TraceFields(ctx)...)
	logger.Info("executing new query", zap.String("query", req.Query))

	p := influxql.NewParser(strings.NewReader(req.Query))
	p.SetParams(req.Params)
	q, err := p.ParseQuery()
	if err != nil {
		return iql.Statistics{}, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "failed to parse query",
			Err:  err,
		}
	}

	span.LogFields(log.String("query", q.String()))

	opts := ExecutionOptions{
		OrgID:           req.OrganizationID,
		Database:        req.DB,
		RetentionPolicy: req.RP,
		ChunkSize:       req.ChunkSize,
		ReadOnly:        true,
		Authorizer:      OpenAuthorizer,
	}

	epoch := req.Epoch
	rw := NewResponseWriter(req.EncodingFormat)

	results, stats := s.executor.ExecuteQuery(ctx, q, opts)
	if req.Chunked {
		for r := range results {
			// Ignore nil results.
			if r == nil {
				continue
			}

			// if requested, convert result timestamps to epoch
			if epoch != "" {
				convertToEpoch(r, epoch)
			}

			err = rw.WriteResponse(ctx, w, Response{Results: []*Result{r}})
			if err != nil {
				break
			}
		}
	} else {
		resp := Response{Results: GatherResults(results, epoch)}
		err = rw.WriteResponse(ctx, w, resp)
	}

	return *stats, err
}

// GatherResults consumes the results from the given channel and organizes them correctly.
// Results for various statements need to be combined together.
func GatherResults(ch <-chan *Result, epoch string) []*Result {
	var results []*Result
	for r := range ch {
		// Ignore nil results.
		if r == nil {
			continue
		}

		// if requested, convert result timestamps to epoch
		if epoch != "" {
			convertToEpoch(r, epoch)
		}

		// It's not chunked so buffer results in memory.
		// Results for statements need to be combined together.
		// We need to check if this new result is for the same statement as
		// the last result, or for the next statement.
		if l := len(results); l > 0 && results[l-1].StatementID == r.StatementID {
			if r.Err != nil {
				results[l-1] = r
				continue
			}

			cr := results[l-1]
			rowsMerged := 0
			if len(cr.Series) > 0 {
				lastSeries := cr.Series[len(cr.Series)-1]

				for _, row := range r.Series {
					if !lastSeries.SameSeries(row) {
						// Next row is for a different series than last.
						break
					}
					// Values are for the same series, so append them.
					lastSeries.Values = append(lastSeries.Values, row.Values...)
					lastSeries.Partial = row.Partial
					rowsMerged++
				}
			}

			// Append remaining rows as new rows.
			r.Series = r.Series[rowsMerged:]
			cr.Series = append(cr.Series, r.Series...)
			cr.Messages = append(cr.Messages, r.Messages...)
			cr.Partial = r.Partial
		} else {
			results = append(results, r)
		}
	}
	return results
}

// convertToEpoch converts result timestamps from time.Time to the specified epoch.
func convertToEpoch(r *Result, epoch string) {
	divisor := int64(1)

	switch epoch {
	case "u":
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}

	for _, s := range r.Series {
		for _, v := range s.Values {
			if ts, ok := v[0].(time.Time); ok {
				v[0] = ts.UnixNano() / divisor
			}
		}
	}
}
