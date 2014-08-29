package engine

import "github.com/influxdb/influxdb/protocol"

// See documentation of Stream
type StreamQuery interface {
	// interface that query the state
	HasPoint() bool
	// Returns a series with one point only
	Next() *protocol.Series
	Closed() bool
}

// See documentation of Stream
type StreamUpdate interface {
	// interface that updates the state
	Close()
	Yield(*protocol.Series)
}

// A stream keep track of the state of a stream of points. A stream is
// defined as an ordered stream of points that have the same set of
// fields. For simplicity this will be the stream of points for one
// time series. This will be extended to include stream of points from
// multiple series that are merged at the shard level and multiple
// streams are merged together again at the coordinator level.
type Stream struct {
	s      *protocol.Series
	closed bool
}

func NewStream() *Stream {
	return &Stream{closed: false}
}

func (stream *Stream) Yield(s *protocol.Series) {
	if stream.s != nil {
		stream.s.Points = append(stream.s.Points, s.Points...)
		return
	}
	stream.s = s
}

func (stream *Stream) Close() {
	stream.closed = true
}

func (stream *Stream) HasPoint() bool {
	return stream.s != nil && len(stream.s.Points) > 0
}

func (stream *Stream) Next() *protocol.Series {
	var p []*protocol.Point
	p, stream.s.Points = stream.s.Points[:1:1], stream.s.Points[1:]
	s := &protocol.Series{
		Name:   stream.s.Name,
		Fields: stream.s.Fields,
		Points: p,
	}
	if len(stream.s.Points) == 0 {
		stream.s = nil
	}
	return s
}

func (stream *Stream) Closed() bool {
	return stream.closed
}
