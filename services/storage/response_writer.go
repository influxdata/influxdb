package storage

import (
	"github.com/influxdata/influxdb/models"
	"go.uber.org/zap"
)

type responseWriter struct {
	stream Storage_ReadServer
	res    *ReadResponse
	logger *zap.Logger
	err    error

	// current series
	sf *ReadResponse_SeriesFrame
	ss int
	sz int // estimated size in bytes for pending write

	vc int // total value count

	buffer struct {
		Float    []*ReadResponse_Frame_FloatPoints
		Integer  []*ReadResponse_Frame_IntegerPoints
		Unsigned []*ReadResponse_Frame_UnsignedPoints
		Boolean  []*ReadResponse_Frame_BooleanPoints
		String   []*ReadResponse_Frame_StringPoints
		Series   []*ReadResponse_Frame_Series
	}
}

func (w *responseWriter) getSeriesFrame(next models.Tags) *ReadResponse_Frame_Series {
	var res *ReadResponse_Frame_Series
	if len(w.buffer.Series) > 0 {
		i := len(w.buffer.Series) - 1
		res = w.buffer.Series[i]
		w.buffer.Series[i] = nil
		w.buffer.Series = w.buffer.Series[:i]
	} else {
		res = &ReadResponse_Frame_Series{&ReadResponse_SeriesFrame{}}
	}

	if cap(res.Series.Tags) < len(next) {
		res.Series.Tags = make([]Tag, len(next))
	} else if len(res.Series.Tags) != len(next) {
		res.Series.Tags = res.Series.Tags[:len(next)]
	}

	return res
}

func (w *responseWriter) putSeriesFrame(f *ReadResponse_Frame_Series) {
	tags := f.Series.Tags
	for i := range tags {
		tags[i].Key = nil
		tags[i].Value = nil
	}
	w.buffer.Series = append(w.buffer.Series, f)
}

func (w *responseWriter) startSeries(next models.Tags) {
	w.ss = len(w.res.Frames)

	f := w.getSeriesFrame(next)
	w.sf = f.Series
	for i, t := range next {
		w.sf.Tags[i] = Tag(t)
	}
	w.res.Frames = append(w.res.Frames, ReadResponse_Frame{f})
	w.sz += w.sf.Size()
}

func (w *responseWriter) flushFrames() {
	if w.err != nil || w.sz == 0 {
		return
	}

	w.sz = 0

	if w.err = w.stream.Send(w.res); w.err != nil {
		w.logger.Error("stream.Send failed", zap.Error(w.err))
		return
	}

	for i := range w.res.Frames {
		d := w.res.Frames[i].Data
		w.res.Frames[i].Data = nil
		switch p := d.(type) {
		case *ReadResponse_Frame_FloatPoints:
			w.putFloatPointsFrame(p)
		case *ReadResponse_Frame_IntegerPoints:
			w.putIntegerPointsFrame(p)
		case *ReadResponse_Frame_UnsignedPoints:
			w.putUnsignedPointsFrame(p)
		case *ReadResponse_Frame_BooleanPoints:
			w.putBooleanPointsFrame(p)
		case *ReadResponse_Frame_StringPoints:
			w.putStringPointsFrame(p)
		case *ReadResponse_Frame_Series:
			w.putSeriesFrame(p)
		}
	}
	w.res.Frames = w.res.Frames[:0]
}
