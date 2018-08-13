package storage

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/metrics"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"go.uber.org/zap"
)

//go:generate protoc -I$GOPATH/src/github.com/influxdata/influxdb/vendor -I. --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,plugins=grpc:. storage.proto predicate.proto
//go:generate tmpl -data=@array_cursor.gen.go.tmpldata array_cursor.gen.go.tmpl
//go:generate tmpl -data=@array_cursor.gen.go.tmpldata response_writer.gen.go.tmpl

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
	var err error
	var wire opentracing.SpanContext

	if len(req.Trace) > 0 {
		wire, _ = opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapCarrier(req.Trace))
		// TODO(sgc): Log ignored error?
	}

	span := opentracing.StartSpan("storage.read", ext.RPCServerOption(wire))
	defer span.Finish()

	ext.DBInstance.Set(span, req.Database)

	ctx := stream.Context()
	ctx = opentracing.ContextWithSpan(ctx, span)
	// TODO(sgc): this should be available via a generic API, such as tsdb.Store
	ctx = tsm1.NewContextWithMetricsGroup(ctx)

	var agg Aggregate_AggregateType
	if req.Aggregate != nil {
		agg = req.Aggregate.Type
	}
	pred := truncateString(PredicateToExprString(req.Predicate))
	groupKeys := truncateString(strings.Join(req.GroupKeys, ","))
	span.
		SetTag("predicate", pred).
		SetTag("series_limit", req.SeriesLimit).
		SetTag("series_offset", req.SeriesOffset).
		SetTag("points_limit", req.PointsLimit).
		SetTag("start", req.TimestampRange.Start).
		SetTag("end", req.TimestampRange.End).
		SetTag("desc", req.Descending).
		SetTag("group", req.Group.String()).
		SetTag("group_keys", groupKeys).
		SetTag("aggregate", agg.String())

	log, logEnd := logger.NewOperation(r.Logger, "Read", "storage_read")
	defer logEnd()
	ctx = logger.NewContextWithLogger(ctx, log)

	if r.loggingEnabled {
		log.Info("Read request info",
			zap.String("database", req.Database),
			zap.String("predicate", pred),
			zap.String("hints", req.Hints.String()),
			zap.Int64("series_limit", req.SeriesLimit),
			zap.Int64("series_offset", req.SeriesOffset),
			zap.Int64("points_limit", req.PointsLimit),
			zap.Int64("start", req.TimestampRange.Start),
			zap.Int64("end", req.TimestampRange.End),
			zap.Bool("desc", req.Descending),
			zap.String("group", req.Group.String()),
			zap.String("group_keys", groupKeys),
			zap.String("aggregate", agg.String()),
		)
	}

	if req.Hints.NoPoints() {
		req.PointsLimit = -1
	}

	if req.PointsLimit == 0 {
		req.PointsLimit = math.MaxInt64
	}

	w := &responseWriter{
		stream: stream,
		res:    &ReadResponse{Frames: make([]ReadResponse_Frame, 0, frameCount)},
		logger: log,
		hints:  req.Hints,
	}

	switch req.Group {
	case GroupBy, GroupExcept:
		if len(req.GroupKeys) == 0 {
			return errors.New("read: GroupKeys must not be empty when GroupBy or GroupExcept specified")
		}
	case GroupNone, GroupAll:
		if len(req.GroupKeys) > 0 {
			return errors.New("read: GroupKeys must be empty when GroupNone or GroupAll specified")
		}
	default:
		return errors.New("read: unexpected value for Group")
	}

	if req.Group == GroupAll {
		err = r.handleRead(ctx, req, w)
	} else {
		err = r.handleGroupRead(ctx, req, w)
	}

	if err != nil {
		log.Error("Read failed", zap.Error(err))
	}

	w.flushFrames()

	if r.loggingEnabled {
		log.Info("Read completed", zap.Int("num_values", w.vc))
	}

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

func (r *rpcService) handleRead(ctx context.Context, req *ReadRequest, w *responseWriter) error {
	rs, err := r.Store.Read(ctx, req)
	if err != nil {
		return err
	}

	if rs == nil {
		return nil
	}
	defer rs.Close()

	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		w.startSeries(rs.Tags())
		w.streamCursor(cur)
		if w.err != nil {
			cur.Close()
			return w.err
		}
	}

	return nil
}

func (r *rpcService) handleGroupRead(ctx context.Context, req *ReadRequest, w *responseWriter) error {
	rs, err := r.Store.GroupRead(ctx, req)
	if err != nil {
		return err
	}

	if rs == nil {
		return nil
	}
	defer rs.Close()

	gc := rs.Next()
	for gc != nil {
		w.startGroup(gc.Keys(), gc.PartitionKeyVals())
		if !req.Hints.HintSchemaAllTime() {
			for gc.Next() {
				cur := gc.Cursor()
				if cur == nil {
					// no data for series key + field combination
					continue
				}

				w.startSeries(gc.Tags())
				w.streamCursor(cur)
				if w.err != nil {
					gc.Close()
					return w.err
				}
			}
		}
		gc.Close()
		gc = rs.Next()
	}

	return nil
}
