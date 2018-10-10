package storage

import (
	"context"
	"errors"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/metrics"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/platform/storage/reads"
	"github.com/influxdata/platform/storage/reads/datatypes"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"go.uber.org/zap"
)

//go:generate protoc -I$GOPATH/src/github.com/influxdata/influxdb/vendor -I. --gogofaster_out=. storage.proto

var (
	ErrMissingReadSource = errors.New("missing ReadSource")
)

type rpcService struct {
	loggingEnabled bool

	Store  reads.Store
	Logger *zap.Logger
}

func (r *rpcService) Capabilities(context.Context, *types.Empty) (*datatypes.CapabilitiesResponse, error) {
	return nil, errors.New("not implemented")
}

func (r *rpcService) Hints(context.Context, *types.Empty) (*datatypes.HintsResponse, error) {
	return nil, errors.New("not implemented")
}

func (r *rpcService) Read(req *datatypes.ReadRequest, stream datatypes.Storage_ReadServer) error {
	source, err := getReadSource(req)
	if err != nil {
		return err
	}

	// TODO(sgc): this should be available via a generic API, such as tsdb.Store
	ctx := tsm1.NewContextWithMetricsGroup(stream.Context())

	var agg datatypes.Aggregate_AggregateType
	if req.Aggregate != nil {
		agg = req.Aggregate.Type
	}
	pred := truncateString(reads.PredicateToExprString(req.Predicate))
	groupKeys := truncateString(strings.Join(req.GroupKeys, ","))

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span.
			SetTag(string(ext.DBInstance), source.Database).
			SetTag("rp", source.RetentionPolicy).
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
	}

	log, logEnd := logger.NewOperation(r.Logger, "Read", "storage_read")
	defer logEnd()
	ctx = logger.NewContextWithLogger(ctx, log)

	if r.loggingEnabled {
		log.Info("Read request info",
			zap.String("database", source.Database),
			zap.String("rp", source.RetentionPolicy),
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

	w := reads.NewResponseWriter(stream, req.Hints)

	switch req.Group {
	case datatypes.GroupBy, datatypes.GroupExcept:
		if len(req.GroupKeys) == 0 {
			return errors.New("read: GroupKeys must not be empty when GroupBy or GroupExcept specified")
		}
	case datatypes.GroupNone, datatypes.GroupAll:
		if len(req.GroupKeys) > 0 {
			return errors.New("read: GroupKeys must be empty when GroupNone or GroupAll specified")
		}
	default:
		return errors.New("read: unexpected value for Group")
	}

	if req.Group == datatypes.GroupAll {
		r.handleRead(ctx, req, w, log)
	} else {
		r.handleGroupRead(ctx, req, w, log)
	}

	if r.loggingEnabled {
		log.Info("Read completed", zap.Int("num_values", w.WrittenN()))
	}

	if span != nil {
		span.SetTag("num_values", w.WrittenN())
		grp := tsm1.MetricsGroupFromContext(ctx)
		grp.ForEach(func(v metrics.Metric) {
			switch m := v.(type) {
			case *metrics.Counter:
				span.SetTag(m.Name(), m.Value())
			}
		})
	}

	return nil
}

func (r *rpcService) handleRead(ctx context.Context, req *datatypes.ReadRequest, w *reads.ResponseWriter, log *zap.Logger) {
	rs, err := r.Store.Read(ctx, req)
	if err != nil {
		log.Error("Read failed", zap.Error(w.Err()))
		return
	}

	if rs == nil {
		return
	}
	defer rs.Close()
	w.WriteResultSet(rs)
	w.Flush()

	if w.Err() != nil {
		log.Error("Write failed", zap.Error(w.Err()))
	}
}

func (r *rpcService) handleGroupRead(ctx context.Context, req *datatypes.ReadRequest, w *reads.ResponseWriter, log *zap.Logger) {
	rs, err := r.Store.GroupRead(ctx, req)
	if err != nil {
		log.Error("GroupRead failed", zap.Error(w.Err()))
		return
	}

	if rs == nil {
		return
	}
	defer rs.Close()
	w.WriteGroupResultSet(rs)
	w.Flush()

	if w.Err() != nil {
		log.Error("Write failed", zap.Error(w.Err()))
	}
}
