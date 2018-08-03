package line

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/escape"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

type Writer struct {
	w   *bufio.Writer
	key []byte
	err error
}

func NewWriter(w io.Writer) *Writer {
	var wr *bufio.Writer
	if wr, _ = w.(*bufio.Writer); wr == nil {
		wr = bufio.NewWriter(w)
	}
	return &Writer{
		w: wr,
	}
}

func (w *Writer) NewBucket(start, end int64) (format.BucketWriter, error) {
	fmt.Fprintf(w.w, "# new shard group start: %s -> end: %s\n", time.Unix(0, start).UTC(), time.Unix(0, end).UTC())
	return w, nil
}

func (w *Writer) Close() error { return w.w.Flush() }
func (w *Writer) Err() error   { return w.err }

func (w *Writer) BeginSeries(name, field []byte, typ influxql.DataType, tags models.Tags) {
	if w.err != nil {
		return
	}

	w.key = models.AppendMakeKey(w.key[:0], name, tags)
	w.key = append(w.key, ' ')
	w.key = append(w.key, escape.Bytes(field)...)
	w.key = append(w.key, '=')
}

func (w *Writer) EndSeries() {}

func (w *Writer) WriteIntegerCursor(cur tsdb.IntegerArrayCursor) {
	if w.err != nil {
		return
	}

	buf := w.key
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
		for i := range a.Timestamps {
			buf = buf[:len(w.key)] // Re-slice buf to be "<series_key> <field>=".

			buf = strconv.AppendInt(buf, a.Values[i], 10)
			buf = append(buf, 'i')
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}

func (w *Writer) WriteFloatCursor(cur tsdb.FloatArrayCursor) {
	if w.err != nil {
		return
	}

	buf := w.key
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
		for i := range a.Timestamps {
			buf = buf[:len(w.key)] // Re-slice buf to be "<series_key> <field>=".

			buf = strconv.AppendFloat(buf, a.Values[i], 'g', -1, 64)
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}

func (w *Writer) WriteUnsignedCursor(cur tsdb.UnsignedArrayCursor) {
	if w.err != nil {
		return
	}

	buf := w.key
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
		for i := range a.Timestamps {
			buf = buf[:len(w.key)] // Re-slice buf to be "<series_key> <field>=".

			buf = strconv.AppendUint(buf, a.Values[i], 10)
			buf = append(buf, 'u')
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}

func (w *Writer) WriteBooleanCursor(cur tsdb.BooleanArrayCursor) {
	if w.err != nil {
		return
	}

	buf := w.key
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
		for i := range a.Timestamps {
			buf = buf[:len(w.key)] // Re-slice buf to be "<series_key> <field>=".

			buf = strconv.AppendBool(buf, a.Values[i])
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}

func (w *Writer) WriteStringCursor(cur tsdb.StringArrayCursor) {
	if w.err != nil {
		return
	}

	buf := w.key
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
		for i := range a.Timestamps {
			buf = buf[:len(w.key)] // Re-slice buf to be "<series_key> <field>=".

			buf = append(buf, '"')
			buf = append(buf, models.EscapeStringField(a.Values[i])...)
			buf = append(buf, '"')
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}
