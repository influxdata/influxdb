package storage

import (
	"context"
	"encoding/binary"

	"github.com/gogo/protobuf/types"
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

	rs, err := r.Store.Read(*req)
	if err != nil {
		r.Logger.Error("Store.Read failed", zap.Error(err))
		return err
	}

	lim := int64(req.Limit)
	i := int64(0)
	b := 0
	var lastKey string
	var res ReadResponse
	res.Frames = make([]ReadResponse_Frame, FrameCount)

LIMIT:
	for rs.Next() {
		if len(res.Frames) >= FrameCount {
			stream.Send(&res)
			res.Frames = make([]ReadResponse_Frame, FrameCount)
		}

		cur := rs.Cursor()
		if cur.SeriesKey() != lastKey {
			lastKey = cur.SeriesKey()
			res.Frames = append(res.Frames, ReadResponse_Frame{&ReadResponse_Frame_Series{&ReadResponse_SeriesFrame{lastKey}}})
			if err != nil {
				r.Logger.Error("stream.Send failed", zap.Error(err))
			}
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
				i++
				if i > lim {
					break LIMIT
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
				i++
				if i > lim {
					break LIMIT
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
	}

	if len(res.Frames) >= FrameCount {
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
