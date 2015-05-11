package client

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	flat "github.com/influxdb/influxdb/client/influxdb"
)

// BatchBuilder is a helper for building a flatbuffer of InfluxDB data points.
type BatchBuilder struct {
	// builder is the flatbuffer Builder that we're wrapping.
	builder *flatbuffers.Builder
	// The remaining members hold offsets, into the flatbuffer, of pieces of
	// data that have been written.
	database     flatbuffers.UOffsetT
	retention    flatbuffers.UOffsetT
	timestamp    int64
	precision    flatbuffers.UOffsetT
	tags         flatbuffers.UOffsetT
	fields       []flatbuffers.UOffsetT
	fieldsOffset flatbuffers.UOffsetT
	points       []flatbuffers.UOffsetT
	pointsOffset flatbuffers.UOffsetT
	batch        flatbuffers.UOffsetT
}

const (
	// bufsz is the default size for the builder's buffer.
	bufsz = 0
)

// NewBatchBuilder constructs a new BatchBuilder.
func NewBatchBuilder() *BatchBuilder {
	return &BatchBuilder{
		builder: flatbuffers.NewBuilder(bufsz),
	}
}

// Reset clears the flatbuffer and starts fresh.
func (b *BatchBuilder) Reset() {
	b.builder = flatbuffers.NewBuilder(bufsz)
	b.database = 0
	b.retention = 0
	b.timestamp = 0
	b.precision = 0
	b.tags = 0
	b.fields = b.fields[:0]
	b.points = b.points[:0]
	b.batch = 0
}

// Database adds the database name to the flatbuffer.
func (b *BatchBuilder) Database(s string) {
	b.database = b.builder.CreateString(s)
}

// RetentionPolicy adds the retention policy name to the flatbuffer.
func (b *BatchBuilder) RetentionPolicy(s string) {
	b.retention = b.builder.CreateString(s)
}

// Timestamp sets the timestamp.
func (b *BatchBuilder) Timestamp(t int64) {
	b.timestamp = t
}

// Precision sets the timestamp precision.
func (b *BatchBuilder) Precision(s string) {
	b.precision = b.builder.CreateString(s)
}

// Tags adds tags (keys & values) to the flatbuffer.
func (b *BatchBuilder) Tags(tags map[string]string) flatbuffers.UOffsetT {
	var tagOffsets []flatbuffers.UOffsetT
	for k, v := range tags {
		key := b.builder.CreateString(k)
		val := b.builder.CreateString(v)
		flat.KeyValStrStart(b.builder)
		flat.KeyValStrAddKey(b.builder, key)
		flat.KeyValStrAddVal(b.builder, val)
		tagOffsets = append(tagOffsets, flat.KeyValStrEnd(b.builder))
	}

	// Doesn't matter which *StartTagsVector function we call here.
	flat.PointStartTagsVector(b.builder, len(tagOffsets))
	for _, tagOffset := range tagOffsets {
		b.builder.PrependUOffsetT(tagOffset)
	}
	b.tags = b.builder.EndVector(len(tagOffsets))

	return b.tags
}

// ResetFields clears out the current fields.
// Should be called after finishing each point.
func (b *BatchBuilder) ResetFields() {
	b.fields = b.fields[:0]
}

// AddFieldDouble adds a double value with the specified name to the flatbuffer.
func (b *BatchBuilder) AddFieldDouble(name string, val float64) {
	k := b.builder.CreateString(name)
	flat.DoubleStart(b.builder)
	flat.DoubleAddVal(b.builder, val)
	v := flat.DoubleEnd(b.builder)

	flat.KeyValStart(b.builder)
	flat.KeyValAddKey(b.builder, k)
	flat.KeyValAddValType(b.builder, flat.VariantDouble)
	flat.KeyValAddVal(b.builder, v)
	b.fields = append(b.fields, flat.KeyValEnd(b.builder))
}

// AddFieldInt64 adds an int64 value with the specified name to the flatbuffer.
func (b *BatchBuilder) AddFieldInt64(name string, val int64) {
	k := b.builder.CreateString(name)
	flat.LongStart(b.builder)
	flat.LongAddVal(b.builder, val)
	v := flat.LongEnd(b.builder)

	flat.KeyValStart(b.builder)
	flat.KeyValAddKey(b.builder, k)
	flat.KeyValAddValType(b.builder, flat.VariantLong)
	flat.KeyValAddVal(b.builder, v)
	b.fields = append(b.fields, flat.KeyValEnd(b.builder))
}

// EndFields adds all the fields created using AddField* methods to a vector in the flatbuffer.
func (b *BatchBuilder) EndFields() flatbuffers.UOffsetT {
	flat.PointStartFieldsVector(b.builder, len(b.fields))
	for _, f := range b.fields {
		b.builder.PrependUOffsetT(f)
	}
	b.fieldsOffset = b.builder.EndVector(len(b.fields))
	return b.fieldsOffset
}

// AddPoint adds a point to the flatbuffer.
func (b *BatchBuilder) AddPoint(name string, tags map[string]string, timestamp int64, precision string) {
	n := b.builder.CreateString(name)

	var t flatbuffers.UOffsetT
	if tags != nil {
		t = b.Tags(tags)
	}

	var p flatbuffers.UOffsetT
	if precision != "" {
		p = b.builder.CreateString(precision)
	}

	flat.PointStart(b.builder)
	flat.PointAddName(b.builder, n)
	if tags != nil {
		flat.PointAddTags(b.builder, t)
	}
	if timestamp != 0 {
		flat.PointAddTimestamp(b.builder, timestamp)
	}
	flat.PointAddFields(b.builder, b.fieldsOffset)
	b.ResetFields()
	if precision != "" {
		flat.PointAddPrecision(b.builder, p)
	}
	b.points = append(b.points, flat.PointEnd(b.builder))
}

// EndPoints adds all the points created using AddPoint to a vector in the flatbuffer.
func (b *BatchBuilder) EndPoints() flatbuffers.UOffsetT {
	flat.BatchPointsStartPointsVector(b.builder, len(b.points))
	for _, p := range b.points {
		b.builder.PrependUOffsetT(p)
	}
	return b.builder.EndVector(len(b.points))
}

// Finish writes the BatchPoints to the flatbuffer.
// Call this once all fields, points, tags, etc. have been added.
func (b *BatchBuilder) Finish() {
	points := b.EndPoints()
	flat.BatchPointsStart(b.builder)
	flat.BatchPointsAddDatabase(b.builder, b.database)
	flat.BatchPointsAddRetentionPolicy(b.builder, b.retention)
	flat.BatchPointsAddTags(b.builder, b.tags)
	flat.BatchPointsAddPoints(b.builder, points)
	b.batch = flat.BatchPointsEnd(b.builder)
	b.builder.Finish(b.batch)
}

// Bytes returns the underling flatbuffer []byte.
func (b *BatchBuilder) Bytes() []byte { return b.builder.Bytes }

// Batch returns the BatchPoints object.
func (b *BatchBuilder) Batch() *flat.BatchPoints {
	return flat.GetRootAsBatchPoints(b.builder.Bytes, b.builder.Head())
}

func GetValFloat64(kv *flat.KeyVal) (float64, error) {
	if kv.ValType() != flat.VariantDouble {
		return 0.0, fmt.Errorf("value is not a float64: %#v", kv)
	}
	var t flatbuffers.Table
	if ok := kv.Val(&t); !ok {
		return 0.0, fmt.Errorf("invalid buffer")
	}
	var f flat.Double
	f.Init(t.Bytes, t.Pos)
	return f.Val(), nil
}
