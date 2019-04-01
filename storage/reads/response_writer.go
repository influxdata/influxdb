package reads

import (
	"fmt"

	"google.golang.org/grpc/metadata"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type ResponseStream interface {
	Send(*datatypes.ReadResponse) error
	// SetTrailer sets the trailer metadata which will be sent with the RPC status.
	// When called more than once, all the provided metadata will be merged.
	SetTrailer(metadata.MD)
}

const (
	batchSize  = 1000
	frameCount = 50
	writeSize  = 64 << 10 // 64k
)

type ResponseWriter struct {
	stream ResponseStream
	res    *datatypes.ReadResponse
	err    error

	// current series
	sf *datatypes.ReadResponse_SeriesFrame
	ss int // pointer to current series frame; used to skip writing if no points
	sz int // estimated size in bytes for pending write

	vc int // total value count

	buffer struct {
		Float    []*datatypes.ReadResponse_Frame_FloatPoints
		Integer  []*datatypes.ReadResponse_Frame_IntegerPoints
		Unsigned []*datatypes.ReadResponse_Frame_UnsignedPoints
		Boolean  []*datatypes.ReadResponse_Frame_BooleanPoints
		String   []*datatypes.ReadResponse_Frame_StringPoints
		Series   []*datatypes.ReadResponse_Frame_Series
		Group    []*datatypes.ReadResponse_Frame_Group
	}

	hints datatypes.HintFlags
}

func NewResponseWriter(stream ResponseStream, hints datatypes.HintFlags) *ResponseWriter {
	rw := &ResponseWriter{stream: stream,
		res: &datatypes.ReadResponse{
			Frames: make([]datatypes.ReadResponse_Frame, 0, frameCount),
		},
		hints: hints,
	}

	return rw
}

// WrittenN returns the number of values written to the response stream.
func (w *ResponseWriter) WrittenN() int { return w.vc }

func (w *ResponseWriter) WriteResultSet(rs ResultSet) error {
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

	stats := rs.Stats()
	w.stream.SetTrailer(metadata.Pairs(
		"scanned-bytes", fmt.Sprint(stats.ScannedBytes),
		"scanned-values", fmt.Sprint(stats.ScannedValues)))

	return nil
}

func (w *ResponseWriter) WriteGroupResultSet(rs GroupResultSet) error {
	stats := cursors.CursorStats{}
	gc := rs.Next()
	for gc != nil {
		w.startGroup(gc.Keys(), gc.PartitionKeyVals())
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
			stats.Add(gc.Stats())
		}
		gc.Close()
		gc = rs.Next()
	}

	w.stream.SetTrailer(metadata.Pairs(
		"scanned-bytes", fmt.Sprint(stats.ScannedBytes),
		"scanned-values", fmt.Sprint(stats.ScannedValues)))

	return nil
}

func (w *ResponseWriter) Err() error { return w.err }

func (w *ResponseWriter) getGroupFrame(keys, partitionKey [][]byte) *datatypes.ReadResponse_Frame_Group {
	var res *datatypes.ReadResponse_Frame_Group
	if len(w.buffer.Group) > 0 {
		i := len(w.buffer.Group) - 1
		res = w.buffer.Group[i]
		w.buffer.Group[i] = nil
		w.buffer.Group = w.buffer.Group[:i]
	} else {
		res = &datatypes.ReadResponse_Frame_Group{Group: &datatypes.ReadResponse_GroupFrame{}}
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

func (w *ResponseWriter) putGroupFrame(f *datatypes.ReadResponse_Frame_Group) {
	for i := range f.Group.TagKeys {
		f.Group.TagKeys[i] = nil
	}
	for i := range f.Group.PartitionKeyVals {
		f.Group.PartitionKeyVals[i] = nil
	}
	w.buffer.Group = append(w.buffer.Group, f)
}

func (w *ResponseWriter) getSeriesFrame(next models.Tags) *datatypes.ReadResponse_Frame_Series {
	var res *datatypes.ReadResponse_Frame_Series
	if len(w.buffer.Series) > 0 {
		i := len(w.buffer.Series) - 1
		res = w.buffer.Series[i]
		w.buffer.Series[i] = nil
		w.buffer.Series = w.buffer.Series[:i]
	} else {
		res = &datatypes.ReadResponse_Frame_Series{Series: &datatypes.ReadResponse_SeriesFrame{}}
	}

	if cap(res.Series.Tags) < len(next) {
		res.Series.Tags = make([]datatypes.Tag, len(next))
	} else if len(res.Series.Tags) != len(next) {
		res.Series.Tags = res.Series.Tags[:len(next)]
	}

	return res
}

func (w *ResponseWriter) putSeriesFrame(f *datatypes.ReadResponse_Frame_Series) {
	tags := f.Series.Tags
	for i := range tags {
		tags[i].Key = nil
		tags[i].Value = nil
	}
	w.buffer.Series = append(w.buffer.Series, f)
}

func (w *ResponseWriter) startGroup(keys, partitionKey [][]byte) {
	f := w.getGroupFrame(keys, partitionKey)
	copy(f.Group.TagKeys, keys)
	copy(f.Group.PartitionKeyVals, partitionKey)
	w.res.Frames = append(w.res.Frames, datatypes.ReadResponse_Frame{Data: f})
	w.sz += f.Size()
}

func (w *ResponseWriter) startSeries(next models.Tags) {
	if w.hints.NoSeries() {
		return
	}

	w.ss = len(w.res.Frames)

	f := w.getSeriesFrame(next)
	w.sf = f.Series
	for i, t := range next {
		w.sf.Tags[i] = datatypes.Tag{
			Key:   t.Key,
			Value: t.Value,
		}
	}
	w.res.Frames = append(w.res.Frames, datatypes.ReadResponse_Frame{Data: f})
	w.sz += w.sf.Size()
}

func (w *ResponseWriter) streamCursor(cur cursors.Cursor) {
	switch {
	case w.hints.NoSeries():
		// skip
	case w.hints.NoPoints():
		switch cur := cur.(type) {
		case cursors.IntegerArrayCursor:
			w.streamIntegerArraySeries(cur)
		case cursors.FloatArrayCursor:
			w.streamFloatArraySeries(cur)
		case cursors.UnsignedArrayCursor:
			w.streamUnsignedArraySeries(cur)
		case cursors.BooleanArrayCursor:
			w.streamBooleanArraySeries(cur)
		case cursors.StringArrayCursor:
			w.streamStringArraySeries(cur)
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}

	default:
		switch cur := cur.(type) {
		case cursors.IntegerArrayCursor:
			w.streamIntegerArrayPoints(cur)
		case cursors.FloatArrayCursor:
			w.streamFloatArrayPoints(cur)
		case cursors.UnsignedArrayCursor:
			w.streamUnsignedArrayPoints(cur)
		case cursors.BooleanArrayCursor:
			w.streamBooleanArrayPoints(cur)
		case cursors.StringArrayCursor:
			w.streamStringArrayPoints(cur)
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}
	}
	cur.Close()
}

func (w *ResponseWriter) Flush() {
	if w.err != nil || w.sz == 0 {
		return
	}

	w.sz = 0

	if w.err = w.stream.Send(w.res); w.err != nil {
		return
	}

	for i := range w.res.Frames {
		d := w.res.Frames[i].Data
		w.res.Frames[i].Data = nil
		switch p := d.(type) {
		case *datatypes.ReadResponse_Frame_FloatPoints:
			w.putFloatPointsFrame(p)
		case *datatypes.ReadResponse_Frame_IntegerPoints:
			w.putIntegerPointsFrame(p)
		case *datatypes.ReadResponse_Frame_UnsignedPoints:
			w.putUnsignedPointsFrame(p)
		case *datatypes.ReadResponse_Frame_BooleanPoints:
			w.putBooleanPointsFrame(p)
		case *datatypes.ReadResponse_Frame_StringPoints:
			w.putStringPointsFrame(p)
		case *datatypes.ReadResponse_Frame_Series:
			w.putSeriesFrame(p)
		case *datatypes.ReadResponse_Frame_Group:
			w.putGroupFrame(p)
		}
	}
	w.res.Frames = w.res.Frames[:0]
}
