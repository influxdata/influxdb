package storage

import (
	"context"
	"encoding/binary"

	"math"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

//go:generate protoc -I$GOPATH/src -I. --plugin=protoc-gen-yarpc=$GOPATH/bin/protoc-gen-yarpc --yarpc_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. storage.proto predicate.proto

type rpcService struct {
	Store *Store

	Logger zap.Logger
}

func (r *rpcService) Capabilities(context.Context, *types.Empty) (*CapabilitiesResponse, error) {
	panic("implement me")
}

func (r *rpcService) Hints(context.Context, *types.Empty) (*HintsResponse, error) {
	panic("implement me")
}

func (r *rpcService) Read(req *ReadRequest, stream Storage_ReadServer) error {
	const BatchSize = 5000
	const FrameCount = 10

	r.Logger.Info("request",
		zap.String("predicate", PredicateToExprString(req.Predicate)),
		zap.Uint64("series limit", req.SeriesLimit),
		zap.Uint64("series offset", req.SeriesOffset),
		zap.Uint64("points limit", req.PointsLimit),
		zap.Int64("start", req.TimestampRange.Start),
		zap.Int64("end", req.TimestampRange.End),
	)

	rs, err := r.Store.Read(req)
	if err != nil {
		r.Logger.Error("Store.Read failed", zap.Error(err))
		return err
	}

	if rs == nil {
		stream.Send(&ReadResponse{})
		return nil
	}

	lim := req.PointsLimit
	if lim == 0 {
		lim = math.MaxUint64
	}

	pointCount := uint64(0)
	b := 0
	var lastTags models.Tags
	var res ReadResponse
	res.Frames = make([]ReadResponse_Frame, 0, FrameCount)
	ss := 0

	for rs.Next() {
		if len(res.Frames) >= FrameCount {
			// TODO(sgc): if last frame is a series, strip it
			err = stream.Send(&res)
			if err != nil {
				r.Logger.Error("stream.Send failed", zap.Error(err))
				rs.Close()
				break
			}
			res.Frames = make([]ReadResponse_Frame, 0, FrameCount)
		}

		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		if next := rs.Tags(); !next.Equal(lastTags) {
			sf := ReadResponse_SeriesFrame{Name: rs.SeriesKey()}
			sf.Tags = make([]Tag, len(next))
			for i, t := range next {
				sf.Tags[i] = Tag(t)
			}

			lastTags = next
			if pointCount == 0 {
				// no points collected, so strip series
				res.Frames = res.Frames[:ss]
			} else {
				ss = len(res.Frames)
			}

			res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_Series{&sf}})

			pointCount = 0
		}

		switch cur := cur.(type) {
		case tsdb.IntegerCursor:
			frame := &ReadResponse_IntegerPointsFrame{Timestamps: make([]int64, 0, BatchSize), Values: make([]int64, 0, BatchSize)}
			res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_IntegerPoints{frame}})

			for {
				ts, v := cur.Next()
				if ts == tsdb.EOF {
					break
				}
				pointCount++
				if pointCount > lim {
					break
				}

				frame.Timestamps = append(frame.Timestamps, ts)
				frame.Values = append(frame.Values, v)

				b++
				if b >= BatchSize {
					frame = &ReadResponse_IntegerPointsFrame{Timestamps: make([]int64, 0, BatchSize), Values: make([]int64, 0, BatchSize)}
					res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_IntegerPoints{frame}})
					b = 0
				}
			}

		case tsdb.FloatCursor:
			frame := &ReadResponse_FloatPointsFrame{Timestamps: make([]int64, 0, BatchSize), Values: make([]float64, 0, BatchSize)}
			res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_FloatPoints{frame}})

			for {
				ts, v := cur.Next()
				if ts == tsdb.EOF {
					break
				}
				pointCount++
				if pointCount > lim {
					break
				}

				frame.Timestamps = append(frame.Timestamps, ts)
				frame.Values = append(frame.Values, v)

				b++
				if b >= BatchSize {
					frame = &ReadResponse_FloatPointsFrame{Timestamps: make([]int64, 0, BatchSize), Values: make([]float64, 0, BatchSize)}
					res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_FloatPoints{frame}})
					b = 0
				}
			}
		default:

		}

		cur.Close()
	}

	if len(res.Frames) > 0 {
		stream.Send(&res)
	}

	return nil
}

type integerPoints struct {
	c   uint32
	buf []byte
	d   []byte
}

func newIntegerPoints(sz int) *integerPoints {
	i := &integerPoints{buf: make([]byte, sz*16+4)}
	i.Reset()
	return i
}

func (i *integerPoints) Write(t, v int64) uint32 {
	binary.BigEndian.PutUint64(i.d, uint64(t))
	binary.BigEndian.PutUint64(i.d[8:], uint64(v))
	i.d = i.d[16:]
	i.c++
	return i.c
}

func (i *integerPoints) Buf() []byte {
	if i.c == 0 {
		return nil
	}

	binary.BigEndian.PutUint32(i.buf[:4], i.c)
	return i.buf[:i.c*16+4]
}

func (i *integerPoints) Reset() {
	i.c = 0
	i.d = i.buf[4:]
}
