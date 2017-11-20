package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

//go:generate protoc -I$GOPATH/src -I. --plugin=protoc-gen-yarpc=$GOPATH/bin/protoc-gen-yarpc --yarpc_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. storage.proto predicate.proto
//go:generate tmpl -data=@batch_cursor.gen.go.tmpldata batch_cursor.gen.go.tmpl
//go:generate tmpl -data=@batch_cursor.gen.go.tmpldata response_writer.gen.go.tmpl

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

const (
	batchSize  = 1000
	frameCount = 50
	writeSize  = 64 << 10 // 64k
)

func (r *rpcService) Read(req *ReadRequest, stream Storage_ReadServer) error {
	// TODO(sgc): implement frameWriter that handles the details of streaming frames

	if r.loggingEnabled {
		r.Logger.Info("request",
			zap.String("database", req.Database),
			zap.String("predicate", PredicateToExprString(req.Predicate)),
			zap.Uint64("series_limit", req.SeriesLimit),
			zap.Uint64("series_offset", req.SeriesOffset),
			zap.Uint64("points_limit", req.PointsLimit),
			zap.Int64("start", req.TimestampRange.Start),
			zap.Int64("end", req.TimestampRange.End),
			zap.Bool("desc", req.Descending),
			zap.String("grouping", strings.Join(req.Grouping, ",")),
		)
	}

	if req.PointsLimit == 0 {
		req.PointsLimit = math.MaxUint64
	}

	// TODO(sgc): use yarpc stream.Context() once implemented
	rs, err := r.Store.Read(context.Background(), req)
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

	return nil
}
