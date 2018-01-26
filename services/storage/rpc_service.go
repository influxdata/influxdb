package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/pkg/metrics"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"go.uber.org/zap"
)

//go:generate protoc -I$GOPATH/src -I. --plugin=protoc-gen-yarpc=$GOPATH/bin/protoc-gen-yarpc --yarpc_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. storage.proto predicate.proto
//go:generate tmpl -data=@batch_cursor.gen.go.tmpldata batch_cursor.gen.go.tmpl
//go:generate tmpl -data=@batch_cursor.gen.go.tmpldata response_writer.gen.go.tmpl

const (
	batchSize  = 1000
	frameCount = 50
	writeSize  = 64 << 10 // 64k
)

type rpcService struct {
	loggingEnabled bool

	Store  *Store
	Logger *zap.Logger
}

func (r *rpcService) Capabilities(context.Context, *types.Empty) (*CapabilitiesResponse, error) {
	return nil, errors.New("not implemented")
}

func (r *rpcService) Hints(context.Context, *types.Empty) (*HintsResponse, error) {
	return nil, errors.New("not implemented")
}

func (r *rpcService) Read(req *ReadRequest, stream Storage_ReadServer) error {
	// TODO(sgc): implement frameWriter that handles the details of streaming frames
	var err error
	var wire opentracing.SpanContext

	if len(req.Trace) > 0 {
		wire, _ = opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapCarrier(req.Trace))
		// TODO(sgc): Log ignored error?
	}

	span := opentracing.StartSpan("storage.read", ext.RPCServerOption(wire))
	defer span.Finish()

	ext.DBInstance.Set(span, req.Database)

	// TODO(sgc): use yarpc stream.Context() once implemented
	ctx := context.Background()
	ctx = opentracing.ContextWithSpan(ctx, span)
	// TODO(sgc): this should be available via a generic API, such as tsdb.Store
	ctx = tsm1.NewContextWithMetricsGroup(ctx)

	var agg Aggregate_AggregateType
	if req.Aggregate != nil {
		agg = req.Aggregate.Type
	}
	pred := truncateString(PredicateToExprString(req.Predicate))
	groupKeys := truncateString(strings.Join(req.Grouping, ","))
	span.
		SetTag("predicate", pred).
		SetTag("series_limit", req.SeriesLimit).
		SetTag("series_offset", req.SeriesOffset).
		SetTag("points_limit", req.PointsLimit).
		SetTag("start", req.TimestampRange.Start).
		SetTag("end", req.TimestampRange.End).
		SetTag("desc", req.Descending).
		SetTag("group_keys", groupKeys).
		SetTag("aggregate", agg.String())

	if r.loggingEnabled {
		r.Logger.Info("request",
			zap.String("database", req.Database),
			zap.String("predicate", pred),
			zap.Uint64("series_limit", req.SeriesLimit),
			zap.Uint64("series_offset", req.SeriesOffset),
			zap.Uint64("points_limit", req.PointsLimit),
			zap.Int64("start", req.TimestampRange.Start),
			zap.Int64("end", req.TimestampRange.End),
			zap.Bool("desc", req.Descending),
			zap.String("group_keys", groupKeys),
			zap.String("aggregate", agg.String()),
		)
	}

	if req.PointsLimit == 0 {
		req.PointsLimit = math.MaxUint64
	}

	rs, err := r.Store.Read(ctx, req)
	if err != nil {
		r.Logger.Error("Store.Read failed", zap.Error(err))
		return err
	}

	if rs == nil {
		return nil
	}
	defer rs.Close()

	w := &responseWriter{
		stream: stream,
		res:    &ReadResponse{Frames: make([]ReadResponse_Frame, 0, frameCount)},
		logger: r.Logger,
	}

	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		w.startSeries(rs.Tags())

		switch cur := cur.(type) {
		case tsdb.IntegerBatchCursor:
			w.streamIntegerPoints(cur)
		case tsdb.FloatBatchCursor:
			w.streamFloatPoints(cur)
		case tsdb.UnsignedBatchCursor:
			w.streamUnsignedPoints(cur)
		case tsdb.BooleanBatchCursor:
			w.streamBooleanPoints(cur)
		case tsdb.StringBatchCursor:
			w.streamStringPoints(cur)
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}

		if w.err != nil {
			return w.err
		}
	}

	w.flushFrames()

	span.SetTag("num_values", w.vc)
	grp := tsm1.MetricsGroupFromContext(ctx)
	grp.ForEach(func(v metrics.Metric) {
		switch m := v.(type) {
		case *metrics.Counter:
			span.SetTag(m.Name(), m.Value())
		}
	})

	return nil
}
