package format

import (
	"fmt"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/storage"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

var (
	// Discard is a Writer where all write calls succeed. The source data is also read completely, which can be useful
	// for testing performance.
	Discard Writer = &devNull{true}

	// DevNull is a Writer where all write calls succeed, however, no source data is read.
	DevNull Writer = &devNull{}
)

type Writer interface {
	NewBucket(start, end int64) (BucketWriter, error)
	Close() error
}

type BucketWriter interface {
	Err() error
	BeginSeries(name, field []byte, typ influxql.DataType, tags models.Tags)
	EndSeries()

	WriteIntegerCursor(cur tsdb.IntegerArrayCursor)
	WriteFloatCursor(cur tsdb.FloatArrayCursor)
	WriteUnsignedCursor(cur tsdb.UnsignedArrayCursor)
	WriteBooleanCursor(cur tsdb.BooleanArrayCursor)
	WriteStringCursor(cur tsdb.StringArrayCursor)
	Close() error
}

// WriteBucket reads data from rs covering the time range [start, end) and streams to w.
// The ResultSet must guarantee series+field keys are produced in ascending lexicographical order and values in
// ascending time order.
func WriteBucket(w Writer, start, end int64, rs *storage.ResultSet) error {
	bw, err := w.NewBucket(start, end)
	if err != nil {
		return err
	}
	defer bw.Close()

	for rs.Next() {
		bw.BeginSeries(rs.Name(), rs.Field(), rs.FieldType(), rs.Tags())

		ci := rs.CursorIterator()
		for ci.Next() {
			cur := ci.Cursor()
			switch c := cur.(type) {
			case tsdb.IntegerArrayCursor:
				bw.WriteIntegerCursor(c)
			case tsdb.FloatArrayCursor:
				bw.WriteFloatCursor(c)
			case tsdb.UnsignedArrayCursor:
				bw.WriteUnsignedCursor(c)
			case tsdb.BooleanArrayCursor:
				bw.WriteBooleanCursor(c)
			case tsdb.StringArrayCursor:
				bw.WriteStringCursor(c)
			case nil:
				// no data for series key + field combination in this shard
				continue
			default:
				panic(fmt.Sprintf("unreachable: %T", c))

			}
			cur.Close()
		}

		bw.EndSeries()

		if bw.Err() != nil {
			return bw.Err()
		}
	}
	return nil
}

type devNull struct {
	r bool
}

func (w *devNull) NewBucket(start, end int64) (BucketWriter, error) {
	return w, nil
}
func (w *devNull) BeginSeries(name, field []byte, typ influxql.DataType, tags models.Tags) {}
func (w *devNull) EndSeries()                                                              {}

func (w *devNull) Err() error   { return nil }
func (w *devNull) Close() error { return nil }

func (w *devNull) WriteIntegerCursor(cur tsdb.IntegerArrayCursor) {
	if !w.r {
		return
	}
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
	}
}

func (w *devNull) WriteFloatCursor(cur tsdb.FloatArrayCursor) {
	if !w.r {
		return
	}
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
	}
}

func (w *devNull) WriteUnsignedCursor(cur tsdb.UnsignedArrayCursor) {
	if !w.r {
		return
	}
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
	}
}

func (w *devNull) WriteBooleanCursor(cur tsdb.BooleanArrayCursor) {
	if !w.r {
		return
	}
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
	}
}

func (w *devNull) WriteStringCursor(cur tsdb.StringArrayCursor) {
	if !w.r {
		return
	}
	for {
		a := cur.Next()
		if a.Len() == 0 {
			break
		}
	}
}
