package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

//go:generate protoc -I$GOPATH/src -I. --plugin=protoc-gen-yarpc=$GOPATH/bin/protoc-gen-yarpc --yarpc_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. storage.proto predicate.proto
//go:generate tmpl -data=@batch_cursor.gen.go.tmpldata batch_cursor.gen.go.tmpl

type rpcService struct {
	loggingEnabled bool

	Store  *Store
	Logger zap.Logger
}

func (r *rpcService) Capabilities(context.Context, *types.Empty) (*CapabilitiesResponse, error) {
	return nil, errors.New("not implemented")
}

func (r *rpcService) Hints(context.Context, *types.Empty) (*HintsResponse, error) {
	return nil, errors.New("not implemented")
}

func (r *rpcService) Read(req *ReadRequest, stream Storage_ReadServer) error {
	const batchSize = 1000
	const frameCount = 50

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
		stream.Send(&ReadResponse{})
		return nil
	}

	b := 0
	res := &ReadResponse{Frames: make([]ReadResponse_Frame, 0, frameCount)}

	for rs.Next() {
		if len(res.Frames) >= frameCount {
			// TODO(sgc): if last frame is a series, strip it
			if err = stream.Send(res); err != nil {
				r.Logger.Error("stream.Send failed", zap.Error(err))
				rs.Close()
				return nil
			}

			for i := range res.Frames {
				res.Frames[i].Data = nil
			}
			res.Frames = res.Frames[:0]
		}

		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		ss := len(res.Frames)

		next := rs.Tags()
		sf := ReadResponse_SeriesFrame{Tags: make([]Tag, len(next))}
		for i, t := range next {
			sf.Tags[i] = Tag(t)
		}
		res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_Series{&sf}})

		switch cur := cur.(type) {
		case tsdb.IntegerBatchCursor:
			frame := &ReadResponse_IntegerPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]int64, 0, batchSize)}
			res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_IntegerPoints{frame}})

			for {
				ts, vs := cur.Next()
				if len(ts) == 0 {
					break
				}

				frame.Timestamps = append(frame.Timestamps, ts...)
				frame.Values = append(frame.Values, vs...)

				b += len(ts)
				if b >= batchSize {
					frame = &ReadResponse_IntegerPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]int64, 0, batchSize)}
					res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_IntegerPoints{frame}})
					b = 0
				}
			}

		case tsdb.FloatBatchCursor:
			frame := &ReadResponse_FloatPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]float64, 0, batchSize)}
			res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_FloatPoints{frame}})

			for {
				ts, vs := cur.Next()
				if len(ts) == 0 {
					break
				}

				frame.Timestamps = append(frame.Timestamps, ts...)
				frame.Values = append(frame.Values, vs...)

				b += len(ts)
				if b >= batchSize {
					frame = &ReadResponse_FloatPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]float64, 0, batchSize)}
					res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_FloatPoints{frame}})
					b = 0
				}
			}

		case tsdb.UnsignedBatchCursor:
			frame := &ReadResponse_UnsignedPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]uint64, 0, batchSize)}
			res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_UnsignedPoints{frame}})

			for {
				ts, vs := cur.Next()
				if len(ts) == 0 {
					break
				}

				frame.Timestamps = append(frame.Timestamps, ts...)
				frame.Values = append(frame.Values, vs...)

				b += len(ts)
				if b >= batchSize {
					frame = &ReadResponse_UnsignedPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]uint64, 0, batchSize)}
					res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_UnsignedPoints{frame}})
					b = 0
				}
			}

		case tsdb.BooleanBatchCursor:
			frame := &ReadResponse_BooleanPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]bool, 0, batchSize)}
			res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_BooleanPoints{frame}})

			for {
				ts, vs := cur.Next()
				if len(ts) == 0 {
					break
				}

				frame.Timestamps = append(frame.Timestamps, ts...)
				frame.Values = append(frame.Values, vs...)

				b += len(ts)
				if b >= batchSize {
					frame = &ReadResponse_BooleanPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]bool, 0, batchSize)}
					res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_BooleanPoints{frame}})
					b = 0
				}
			}

		case tsdb.StringBatchCursor:
			frame := &ReadResponse_StringPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]string, 0, batchSize)}
			res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_StringPoints{frame}})

			for {
				ts, vs := cur.Next()
				if len(ts) == 0 {
					break
				}

				frame.Timestamps = append(frame.Timestamps, ts...)
				frame.Values = append(frame.Values, vs...)

				b += len(ts)
				if b >= batchSize {
					frame = &ReadResponse_StringPointsFrame{Timestamps: make([]int64, 0, batchSize), Values: make([]string, 0, batchSize)}
					res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_StringPoints{frame}})
					b = 0
				}
			}

		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}

		cur.Close()

		if len(res.Frames) == ss+1 {
			// no points collected, so strip series
			res.Frames = res.Frames[:ss]
		}
	}

	if len(res.Frames) > 0 {
		stream.Send(res)
	}

	return nil
}
