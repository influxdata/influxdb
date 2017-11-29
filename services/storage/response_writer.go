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
}

func (w *responseWriter) startSeries(next models.Tags) {
	w.ss = len(w.res.Frames)

	w.sf = &ReadResponse_SeriesFrame{Tags: make([]Tag, len(next))}
	for i, t := range next {
		w.sf.Tags[i] = Tag(t)
	}
	w.res.Frames = append(w.res.Frames, ReadResponse_Frame{&ReadResponse_Frame_Series{w.sf}})
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
		w.res.Frames[i].Data = nil
	}
	w.res.Frames = w.res.Frames[:0]
}
