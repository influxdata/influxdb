package binary

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/tlv"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

type Writer struct {
	w           *bufio.Writer
	buf         []byte
	db, rp      string
	duration    time.Duration
	err         error
	bw          *bucketWriter
	state       writeState
	wroteHeader bool

	msg struct {
		bucketHeader BucketHeader
		bucketFooter BucketFooter
		seriesHeader SeriesHeader
		seriesFooter SeriesFooter
	}

	stats struct {
		series int
		counts [8]struct {
			series, values int
		}
	}
}

type writeState int

const (
	writeHeader writeState = iota
	writeBucket
	writeSeries
	writeSeriesHeader
	writePoints
)

func NewWriter(w io.Writer, database, rp string, duration time.Duration) *Writer {
	var wr *bufio.Writer
	if wr, _ = w.(*bufio.Writer); wr == nil {
		wr = bufio.NewWriter(w)
	}
	return &Writer{w: wr, db: database, rp: rp, duration: duration}
}

func (w *Writer) WriteStats(o io.Writer) {
	fmt.Fprintf(o, "total series: %d\n", w.stats.series)

	for i := 0; i < 5; i++ {
		ft := FieldType(i)
		fmt.Fprintf(o, "%s unique series: %d\n", ft, w.stats.counts[i].series)
		fmt.Fprintf(o, "%s total values : %d\n", ft, w.stats.counts[i].values)
	}
}

func (w *Writer) NewBucket(start, end int64) (format.BucketWriter, error) {
	if w.state == writeHeader {
		w.writeHeader()
	}

	if w.err != nil {
		return nil, w.err
	}

	if w.state != writeBucket {
		panic(fmt.Sprintf("writer state: got=%v, exp=%v", w.state, writeBucket))
	}

	w.bw = &bucketWriter{w: w, start: start, end: end}
	w.writeBucketHeader(start, end)

	return w.bw, w.err
}

func (w *Writer) Close() error {
	if w.err == ErrWriteAfterClose {
		return nil
	}
	if w.err != nil {
		return w.err
	}

	w.err = ErrWriteAfterClose

	return nil
}

func (w *Writer) writeHeader() {
	w.state = writeBucket
	w.wroteHeader = true

	w.write(Magic[:])

	h := Header{
		Version:         Version0,
		Database:        w.db,
		RetentionPolicy: w.rp,
		ShardDuration:   w.duration,
	}
	w.writeTypeMessage(HeaderType, &h)
}

func (w *Writer) writeBucketHeader(start, end int64) {
	w.state = writeSeries
	w.msg.bucketHeader.Start = start
	w.msg.bucketHeader.End = end
	w.writeTypeMessage(BucketHeaderType, &w.msg.bucketHeader)
}

func (w *Writer) writeBucketFooter() {
	w.state = writeBucket
	w.writeTypeMessage(BucketFooterType, &w.msg.bucketFooter)
}

func (w *Writer) writeSeriesHeader(key, field []byte, ft FieldType) {
	w.state = writePoints
	w.stats.series++
	w.stats.counts[ft&7].series++

	w.msg.seriesHeader.SeriesKey = key
	w.msg.seriesHeader.Field = field
	w.msg.seriesHeader.FieldType = ft
	w.writeTypeMessage(SeriesHeaderType, &w.msg.seriesHeader)
}

func (w *Writer) writeSeriesFooter(ft FieldType, count int) {
	w.stats.counts[ft&7].values += count
	w.writeTypeMessage(SeriesFooterType, &w.msg.seriesFooter)
}

func (w *Writer) write(p []byte) {
	if w.err != nil {
		return
	}
	_, w.err = w.w.Write(p)
}

func (w *Writer) writeTypeMessage(typ MessageType, msg message) {
	if w.err != nil {
		return
	}

	// ensure size
	n := msg.Size()
	if n > cap(w.buf) {
		w.buf = make([]byte, n)
	} else {
		w.buf = w.buf[:n]
	}

	_, w.err = msg.MarshalTo(w.buf)
	w.writeTypeBytes(typ, w.buf)
}

func (w *Writer) writeTypeBytes(typ MessageType, b []byte) {
	if w.err != nil {
		return
	}
	w.err = tlv.WriteTLV(w.w, byte(typ), w.buf)
}

type bucketWriter struct {
	w          *Writer
	err        error
	start, end int64
	key        []byte
	field      []byte
	n          int
	closed     bool
}

func (bw *bucketWriter) Err() error {
	if bw.w.err != nil {
		return bw.w.err
	}
	return bw.err
}

func (bw *bucketWriter) hasErr() bool {
	return bw.w.err != nil || bw.err != nil
}

func (bw *bucketWriter) BeginSeries(name, field []byte, typ influxql.DataType, tags models.Tags) {
	if bw.hasErr() {
		return
	}

	if bw.w.state != writeSeries {
		panic(fmt.Sprintf("writer state: got=%v, exp=%v", bw.w.state, writeSeries))
	}
	bw.w.state = writeSeriesHeader

	bw.key = models.AppendMakeKey(bw.key[:0], name, tags)
	bw.field = field
}

func (bw *bucketWriter) EndSeries() {
	if bw.hasErr() {
		return
	}

	if bw.w.state != writePoints && bw.w.state != writeSeriesHeader {
		panic(fmt.Sprintf("writer state: got=%v, exp=%v,%v", bw.w.state, writeSeriesHeader, writePoints))
	}
	if bw.w.state == writePoints {
		bw.w.writeSeriesFooter(IntegerFieldType, bw.n)
	}
	bw.w.state = writeSeries
}

func (bw *bucketWriter) WriteIntegerCursor(cur tsdb.IntegerArrayCursor) {
	if bw.hasErr() {
		return
	}

	if bw.w.state == writeSeriesHeader {
		bw.w.writeSeriesHeader(bw.key, bw.field, IntegerFieldType)
	}

	if bw.w.state != writePoints {
		panic(fmt.Sprintf("writer state: got=%v, exp=%v", bw.w.state, writePoints))
	}

	var msg IntegerPoints
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}

		bw.n += a.Len()
		msg.Timestamps = a.Timestamps
		msg.Values = a.Values
		bw.w.writeTypeMessage(IntegerPointsType, &msg)
	}
}

func (bw *bucketWriter) WriteFloatCursor(cur tsdb.FloatArrayCursor) {
	if bw.hasErr() {
		return
	}

	if bw.w.state == writeSeriesHeader {
		bw.w.writeSeriesHeader(bw.key, bw.field, FloatFieldType)
	}

	if bw.w.state != writePoints {
		panic(fmt.Sprintf("writer state: got=%v, exp=%v", bw.w.state, writePoints))
	}

	var msg FloatPoints
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}

		bw.n += a.Len()
		msg.Timestamps = a.Timestamps
		msg.Values = a.Values
		bw.w.writeTypeMessage(FloatPointsType, &msg)
	}
}

func (bw *bucketWriter) WriteUnsignedCursor(cur tsdb.UnsignedArrayCursor) {
	if bw.hasErr() {
		return
	}

	if bw.w.state == writeSeriesHeader {
		bw.w.writeSeriesHeader(bw.key, bw.field, UnsignedFieldType)
	}

	if bw.w.state != writePoints {
		panic(fmt.Sprintf("writer state: got=%v, exp=%v", bw.w.state, writePoints))
	}

	var msg UnsignedPoints
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}

		bw.n += a.Len()
		msg.Timestamps = a.Timestamps
		msg.Values = a.Values
		bw.w.writeTypeMessage(UnsignedPointsType, &msg)
	}
}

func (bw *bucketWriter) WriteBooleanCursor(cur tsdb.BooleanArrayCursor) {
	if bw.hasErr() {
		return
	}

	if bw.w.state == writeSeriesHeader {
		bw.w.writeSeriesHeader(bw.key, bw.field, BooleanFieldType)
	}

	if bw.w.state != writePoints {
		panic(fmt.Sprintf("writer state: got=%v, exp=%v", bw.w.state, writePoints))
	}

	var msg BooleanPoints
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}

		bw.n += a.Len()
		msg.Timestamps = a.Timestamps
		msg.Values = a.Values
		bw.w.writeTypeMessage(BooleanPointsType, &msg)
	}
}

func (bw *bucketWriter) WriteStringCursor(cur tsdb.StringArrayCursor) {
	if bw.hasErr() {
		return
	}

	if bw.w.state == writeSeriesHeader {
		bw.w.writeSeriesHeader(bw.key, bw.field, StringFieldType)
	}

	if bw.w.state != writePoints {
		panic(fmt.Sprintf("writer state: got=%v, exp=%v", bw.w.state, writePoints))
	}

	var msg StringPoints
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}

		bw.n += a.Len()
		msg.Timestamps = a.Timestamps
		msg.Values = a.Values
		bw.w.writeTypeMessage(StringPointsType, &msg)
	}
}

func (bw *bucketWriter) Close() error {
	if bw.closed {
		return nil
	}

	bw.closed = true

	if bw.hasErr() {
		return bw.Err()
	}

	bw.w.bw = nil
	bw.w.writeBucketFooter()
	bw.err = ErrWriteBucketAfterClose

	return bw.w.w.Flush()
}
