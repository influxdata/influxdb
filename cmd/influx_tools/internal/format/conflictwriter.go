package format

import (
	"bytes"
	"fmt"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// ConflictWriter is a Writer that redirects conflicting data to an alternate output.
type ConflictWriter struct {
	w  Writer
	c  Writer
	bw aggregateBucketWriter
}

// NewConflictWriter returns a Writer that redirects invalid point data to the conflict Writer.
func NewConflictWriter(w, conflict Writer) *ConflictWriter {
	return &ConflictWriter{w: w, c: conflict}
}

func (cw *ConflictWriter) NewBucket(start, end int64) (bw BucketWriter, err error) {
	cw.bw.w, err = cw.w.NewBucket(start, end)
	if err != nil {
		return nil, err
	}

	cw.bw.c, err = cw.c.NewBucket(start, end)
	if err != nil {
		cw.bw.w.Close()
		return nil, err
	}
	return &cw.bw, nil
}

func (cw *ConflictWriter) Close() error {
	// we care if either error and prioritize the conflict writer lower.
	cerr := cw.c.Close()
	if err := cw.w.Close(); err != nil {
		return err
	}

	return cerr
}

type bucketState int

const (
	beginSeriesBucketState bucketState = iota
	writeBucketState
	writeConflictsBucketState
)

type aggregateBucketWriter struct {
	w BucketWriter
	c BucketWriter

	state bucketState

	// current series
	name  []byte
	field []byte
	typ   influxql.DataType
	tags  models.Tags
	mf    map[string]influxql.DataType
}

func (bw *aggregateBucketWriter) Err() error {
	switch {
	case bw.w.Err() != nil:
		return bw.w.Err()
	case bw.c.Err() != nil:
		return bw.c.Err()
	default:
		return nil
	}
}

func (bw *aggregateBucketWriter) BeginSeries(name, field []byte, typ influxql.DataType, tags models.Tags) {
	bw.w.BeginSeries(name, field, typ, tags)

	if !bytes.Equal(bw.name, name) {
		// new measurement
		bw.name = append(bw.name[:0], name...)
		bw.mf = make(map[string]influxql.DataType)
	}

	bw.field = append(bw.field[:0], field...)
	bw.tags = tags

	var ok bool
	bw.typ, ok = bw.mf[string(field)]
	if !ok {
		bw.mf[string(field)] = typ
		bw.typ = typ
	}

	bw.state = writeBucketState
}

func (bw *aggregateBucketWriter) EndSeries() {
	switch {
	case bw.state == writeBucketState:
		bw.w.EndSeries()
	case bw.state == writeConflictsBucketState:
		bw.w.EndSeries()
		bw.c.EndSeries()
	default:
		panic(fmt.Sprintf("ConflictWriter state: got=%v, exp=%v,%v", bw.state, writeBucketState, writeConflictsBucketState))
	}
	bw.state = beginSeriesBucketState
}

func (bw *aggregateBucketWriter) conflictState(other influxql.DataType) {
	if bw.state == writeBucketState {
		bw.c.BeginSeries(bw.name, bw.field, bw.typ, bw.tags)
		bw.state = writeConflictsBucketState
	}
}

func (bw *aggregateBucketWriter) WriteIntegerCursor(cur tsdb.IntegerArrayCursor) {
	if bw.typ == influxql.Integer {
		bw.w.WriteIntegerCursor(cur)
	} else {
		bw.conflictState(influxql.Integer)
		bw.c.WriteIntegerCursor(cur)
	}
}

func (bw *aggregateBucketWriter) WriteFloatCursor(cur tsdb.FloatArrayCursor) {
	if bw.typ == influxql.Float {
		bw.w.WriteFloatCursor(cur)
	} else {
		bw.conflictState(influxql.Float)
		bw.c.WriteFloatCursor(cur)
	}
}

func (bw *aggregateBucketWriter) WriteUnsignedCursor(cur tsdb.UnsignedArrayCursor) {
	if bw.typ == influxql.Unsigned {
		bw.w.WriteUnsignedCursor(cur)
	} else {
		bw.conflictState(influxql.Unsigned)
		bw.c.WriteUnsignedCursor(cur)
	}
}

func (bw *aggregateBucketWriter) WriteBooleanCursor(cur tsdb.BooleanArrayCursor) {
	if bw.typ == influxql.Boolean {
		bw.w.WriteBooleanCursor(cur)
	} else {
		bw.conflictState(influxql.Boolean)
		bw.c.WriteBooleanCursor(cur)
	}
}

func (bw *aggregateBucketWriter) WriteStringCursor(cur tsdb.StringArrayCursor) {
	if bw.typ == influxql.String {
		bw.w.WriteStringCursor(cur)
	} else {
		bw.conflictState(influxql.String)
		bw.c.WriteStringCursor(cur)
	}
}

func (bw *aggregateBucketWriter) Close() error {
	// we care if either error and prioritize the conflict writer lower.
	cerr := bw.c.Close()
	if err := bw.w.Close(); err != nil {
		return err
	}

	return cerr
}
