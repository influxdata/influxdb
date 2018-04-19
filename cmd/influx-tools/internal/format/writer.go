package format

import (
	"fmt"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/storage"
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

	WriteIntegerCursor(cur tsdb.IntegerBatchCursor)
	WriteFloatCursor(cur tsdb.FloatBatchCursor)
	WriteUnsignedCursor(cur tsdb.UnsignedBatchCursor)
	WriteBooleanCursor(cur tsdb.BooleanBatchCursor)
	WriteStringCursor(cur tsdb.StringBatchCursor)
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
			case tsdb.IntegerBatchCursor:
				bw.WriteIntegerCursor(c)
			case tsdb.FloatBatchCursor:
				bw.WriteFloatCursor(c)
			case tsdb.UnsignedBatchCursor:
				bw.WriteUnsignedCursor(c)
			case tsdb.BooleanBatchCursor:
				bw.WriteBooleanCursor(c)
			case tsdb.StringBatchCursor:
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

func (w *devNull) WriteIntegerCursor(cur tsdb.IntegerBatchCursor) {
	if !w.r {
		return
	}
	for {
		ts, _ := cur.Next()
		if len(ts) == 0 {
			break
		}
	}
}

func (w *devNull) WriteFloatCursor(cur tsdb.FloatBatchCursor) {
	if !w.r {
		return
	}
	for {
		ts, _ := cur.Next()
		if len(ts) == 0 {
			break
		}
	}
}

func (w *devNull) WriteUnsignedCursor(cur tsdb.UnsignedBatchCursor) {
	if !w.r {
		return
	}
	for {
		ts, _ := cur.Next()
		if len(ts) == 0 {
			break
		}
	}
}

func (w *devNull) WriteBooleanCursor(cur tsdb.BooleanBatchCursor) {
	if !w.r {
		return
	}
	for {
		ts, _ := cur.Next()
		if len(ts) == 0 {
			break
		}
	}
}

func (w *devNull) WriteStringCursor(cur tsdb.StringBatchCursor) {
	if !w.r {
		return
	}
	for {
		ts, _ := cur.Next()
		if len(ts) == 0 {
			break
		}
	}
}
