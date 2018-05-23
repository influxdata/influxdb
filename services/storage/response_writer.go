package storage

import (
	"fmt"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
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
		Group    []*ReadResponse_Frame_Group
	}

	hints HintFlags
}

func (w *responseWriter) getGroupFrame(keys, partitionKey [][]byte) *ReadResponse_Frame_Group {
	var res *ReadResponse_Frame_Group
	if len(w.buffer.Group) > 0 {
		i := len(w.buffer.Group) - 1
		res = w.buffer.Group[i]
		w.buffer.Group[i] = nil
		w.buffer.Group = w.buffer.Group[:i]
	} else {
		res = &ReadResponse_Frame_Group{&ReadResponse_GroupFrame{}}
	}

	if cap(res.Group.TagKeys) < len(keys) {
		res.Group.TagKeys = make([][]byte, len(keys))
	} else if len(res.Group.TagKeys) != len(keys) {
		res.Group.TagKeys = res.Group.TagKeys[:len(keys)]
	}

	if cap(res.Group.PartitionKeyVals) < len(partitionKey) {
		res.Group.PartitionKeyVals = make([][]byte, len(partitionKey))
	} else if len(res.Group.PartitionKeyVals) != len(partitionKey) {
		res.Group.PartitionKeyVals = res.Group.PartitionKeyVals[:len(partitionKey)]
	}

	return res
}

func (w *responseWriter) putGroupFrame(f *ReadResponse_Frame_Group) {
	for i := range f.Group.TagKeys {
		f.Group.TagKeys[i] = nil
	}
	for i := range f.Group.PartitionKeyVals {
		f.Group.PartitionKeyVals[i] = nil
	}
	w.buffer.Group = append(w.buffer.Group, f)
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

func (w *responseWriter) startGroup(keys, partitionKey [][]byte) {
	f := w.getGroupFrame(keys, partitionKey)
	copy(f.Group.TagKeys, keys)
	copy(f.Group.PartitionKeyVals, partitionKey)
	w.res.Frames = append(w.res.Frames, ReadResponse_Frame{f})
	w.sz += f.Size()
}

func (w *responseWriter) startSeries(next models.Tags) {
	if w.hints.NoSeries() {
		return
	}

	w.ss = len(w.res.Frames)

	f := w.getSeriesFrame(next)
	w.sf = f.Series
	for i, t := range next {
		w.sf.Tags[i] = Tag(t)
	}
	w.res.Frames = append(w.res.Frames, ReadResponse_Frame{f})
	w.sz += w.sf.Size()
}

func (w *responseWriter) streamCursor(cur tsdb.Cursor) {
	switch {
	case w.hints.NoSeries():
		// skip
	case w.hints.NoPoints():
		switch cur := cur.(type) {
		case tsdb.IntegerBatchCursor:
			w.streamIntegerSeries(cur)
		case tsdb.FloatBatchCursor:
			w.streamFloatSeries(cur)
		case tsdb.UnsignedBatchCursor:
			w.streamUnsignedSeries(cur)
		case tsdb.BooleanBatchCursor:
			w.streamBooleanSeries(cur)
		case tsdb.StringBatchCursor:
			w.streamStringSeries(cur)
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}

	default:
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
	}
	cur.Close()
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
		case *ReadResponse_Frame_Group:
			w.putGroupFrame(p)
		}
	}
	w.res.Frames = w.res.Frames[:0]
}
