// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ipc // import "github.com/apache/arrow/go/arrow/ipc"

import (
	"sort"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/influxdata/influxdb/rpc/flight/ipc/internal/flatbuf"
	"golang.org/x/xerrors"
)

const (
	currentMetadataVersion = MetadataV4

	// ARROW-109: We set this number arbitrarily to help catch user mistakes. For
	// deeply nested schemas, it is expected the user will indicate explicitly the
	// maximum allowed recursion depth
	kMaxNestingDepth = 64
)

type startVecFunc func(b *flatbuffers.Builder, n int) flatbuffers.UOffsetT

type fieldMetadata struct {
	Len    int64
	Nulls  int64
	Offset int64
}

type bufferMetadata struct {
	Offset int64 // relative offset into the memory page to the starting byte of the buffer
	Len    int64 // absolute length in bytes of the buffer
}

func unitToFB(unit arrow.TimeUnit) flatbuf.TimeUnit {
	switch unit {
	case arrow.Second:
		return flatbuf.TimeUnitSECOND
	case arrow.Millisecond:
		return flatbuf.TimeUnitMILLISECOND
	case arrow.Microsecond:
		return flatbuf.TimeUnitMICROSECOND
	case arrow.Nanosecond:
		return flatbuf.TimeUnitNANOSECOND
	default:
		panic(xerrors.Errorf("arrow/ipc: invalid arrow.TimeUnit(%d) value", unit))
	}
}

func fieldToFB(b *flatbuffers.Builder, field arrow.Field, memo *dictMemo) flatbuffers.UOffsetT {
	var visitor = fieldVisitor{b: b, memo: memo, meta: make(map[string]string)}
	return visitor.result(field)
}

type fieldVisitor struct {
	b      *flatbuffers.Builder
	memo   *dictMemo
	dtype  flatbuf.Type
	offset flatbuffers.UOffsetT
	kids   []flatbuffers.UOffsetT
	meta   map[string]string
}

func (fv *fieldVisitor) visit(field arrow.Field) {
	dt := field.Type
	switch dt := dt.(type) {
	case *arrow.NullType:
		fv.dtype = flatbuf.TypeNull
		flatbuf.NullStart(fv.b)
		fv.offset = flatbuf.NullEnd(fv.b)

	case *arrow.BooleanType:
		fv.dtype = flatbuf.TypeBool
		flatbuf.BoolStart(fv.b)
		fv.offset = flatbuf.BoolEnd(fv.b)

	case *arrow.Uint8Type:
		fv.dtype = flatbuf.TypeInt
		fv.offset = intToFB(fv.b, int32(dt.BitWidth()), false)

	case *arrow.Uint16Type:
		fv.dtype = flatbuf.TypeInt
		fv.offset = intToFB(fv.b, int32(dt.BitWidth()), false)

	case *arrow.Uint32Type:
		fv.dtype = flatbuf.TypeInt
		fv.offset = intToFB(fv.b, int32(dt.BitWidth()), false)

	case *arrow.Uint64Type:
		fv.dtype = flatbuf.TypeInt
		fv.offset = intToFB(fv.b, int32(dt.BitWidth()), false)

	case *arrow.Int8Type:
		fv.dtype = flatbuf.TypeInt
		fv.offset = intToFB(fv.b, int32(dt.BitWidth()), true)

	case *arrow.Int16Type:
		fv.dtype = flatbuf.TypeInt
		fv.offset = intToFB(fv.b, int32(dt.BitWidth()), true)

	case *arrow.Int32Type:
		fv.dtype = flatbuf.TypeInt
		fv.offset = intToFB(fv.b, int32(dt.BitWidth()), true)

	case *arrow.Int64Type:
		fv.dtype = flatbuf.TypeInt
		fv.offset = intToFB(fv.b, int32(dt.BitWidth()), true)

	case *arrow.Float16Type:
		fv.dtype = flatbuf.TypeFloatingPoint
		fv.offset = floatToFB(fv.b, int32(dt.BitWidth()))

	case *arrow.Float32Type:
		fv.dtype = flatbuf.TypeFloatingPoint
		fv.offset = floatToFB(fv.b, int32(dt.BitWidth()))

	case *arrow.Float64Type:
		fv.dtype = flatbuf.TypeFloatingPoint
		fv.offset = floatToFB(fv.b, int32(dt.BitWidth()))

	case *arrow.Decimal128Type:
		fv.dtype = flatbuf.TypeDecimal
		flatbuf.DecimalStart(fv.b)
		flatbuf.DecimalAddPrecision(fv.b, dt.Precision)
		flatbuf.DecimalAddScale(fv.b, dt.Scale)
		fv.offset = flatbuf.DecimalEnd(fv.b)

	case *arrow.FixedSizeBinaryType:
		fv.dtype = flatbuf.TypeFixedSizeBinary
		flatbuf.FixedSizeBinaryStart(fv.b)
		flatbuf.FixedSizeBinaryAddByteWidth(fv.b, int32(dt.ByteWidth))
		fv.offset = flatbuf.FixedSizeBinaryEnd(fv.b)

	case *arrow.BinaryType:
		fv.dtype = flatbuf.TypeBinary
		flatbuf.BinaryStart(fv.b)
		fv.offset = flatbuf.BinaryEnd(fv.b)

	case *arrow.StringType:
		fv.dtype = flatbuf.TypeUtf8
		flatbuf.Utf8Start(fv.b)
		fv.offset = flatbuf.Utf8End(fv.b)

	case *arrow.Date32Type:
		fv.dtype = flatbuf.TypeDate
		flatbuf.DateStart(fv.b)
		flatbuf.DateAddUnit(fv.b, flatbuf.DateUnitDAY)
		fv.offset = flatbuf.DateEnd(fv.b)

	case *arrow.Date64Type:
		fv.dtype = flatbuf.TypeDate
		flatbuf.DateStart(fv.b)
		flatbuf.DateAddUnit(fv.b, flatbuf.DateUnitMILLISECOND)
		fv.offset = flatbuf.DateEnd(fv.b)

	case *arrow.Time32Type:
		fv.dtype = flatbuf.TypeTime
		flatbuf.TimeStart(fv.b)
		flatbuf.TimeAddUnit(fv.b, unitToFB(dt.Unit))
		flatbuf.TimeAddBitWidth(fv.b, 32)
		fv.offset = flatbuf.TimeEnd(fv.b)

	case *arrow.Time64Type:
		fv.dtype = flatbuf.TypeTime
		flatbuf.TimeStart(fv.b)
		flatbuf.TimeAddUnit(fv.b, unitToFB(dt.Unit))
		flatbuf.TimeAddBitWidth(fv.b, 64)
		fv.offset = flatbuf.TimeEnd(fv.b)

	case *arrow.TimestampType:
		fv.dtype = flatbuf.TypeTimestamp
		unit := unitToFB(dt.Unit)
		var tz flatbuffers.UOffsetT
		if dt.TimeZone != "" {
			tz = fv.b.CreateString(dt.TimeZone)
		}
		flatbuf.TimestampStart(fv.b)
		flatbuf.TimestampAddUnit(fv.b, unit)
		flatbuf.TimestampAddTimezone(fv.b, tz)
		fv.offset = flatbuf.TimestampEnd(fv.b)

	case *arrow.StructType:
		fv.dtype = flatbuf.TypeStruct_
		offsets := make([]flatbuffers.UOffsetT, len(dt.Fields()))
		for i, field := range dt.Fields() {
			offsets[i] = fieldToFB(fv.b, field, fv.memo)
		}
		flatbuf.Struct_Start(fv.b)
		for i := len(offsets) - 1; i >= 0; i-- {
			fv.b.PrependUOffsetT(offsets[i])
		}
		fv.offset = flatbuf.Struct_End(fv.b)
		fv.kids = append(fv.kids, offsets...)

	case *arrow.ListType:
		fv.dtype = flatbuf.TypeList
		fv.kids = append(fv.kids, fieldToFB(fv.b, arrow.Field{Name: "item", Type: dt.Elem(), Nullable: field.Nullable}, fv.memo))
		flatbuf.ListStart(fv.b)
		fv.offset = flatbuf.ListEnd(fv.b)

	case *arrow.FixedSizeListType:
		fv.dtype = flatbuf.TypeFixedSizeList
		fv.kids = append(fv.kids, fieldToFB(fv.b, arrow.Field{Name: "item", Type: dt.Elem(), Nullable: field.Nullable}, fv.memo))
		flatbuf.FixedSizeListStart(fv.b)
		flatbuf.FixedSizeListAddListSize(fv.b, dt.Len())
		fv.offset = flatbuf.FixedSizeListEnd(fv.b)

	case *arrow.MonthIntervalType:
		fv.dtype = flatbuf.TypeInterval
		flatbuf.IntervalStart(fv.b)
		flatbuf.IntervalAddUnit(fv.b, flatbuf.IntervalUnitYEAR_MONTH)
		fv.offset = flatbuf.IntervalEnd(fv.b)

	case *arrow.DayTimeIntervalType:
		fv.dtype = flatbuf.TypeInterval
		flatbuf.IntervalStart(fv.b)
		flatbuf.IntervalAddUnit(fv.b, flatbuf.IntervalUnitDAY_TIME)
		fv.offset = flatbuf.IntervalEnd(fv.b)

	case *arrow.DurationType:
		fv.dtype = flatbuf.TypeDuration
		unit := unitToFB(dt.Unit)
		flatbuf.DurationStart(fv.b)
		flatbuf.DurationAddUnit(fv.b, unit)
		fv.offset = flatbuf.DurationEnd(fv.b)

	default:
		err := xerrors.Errorf("arrow/ipc: invalid data type %v", dt)
		panic(err) // FIXME(sbinet): implement all data-types.
	}
}

func (fv *fieldVisitor) result(field arrow.Field) flatbuffers.UOffsetT {
	nameFB := fv.b.CreateString(field.Name)

	fv.visit(field)

	flatbuf.FieldStartChildrenVector(fv.b, len(fv.kids))
	for i := len(fv.kids) - 1; i >= 0; i-- {
		fv.b.PrependUOffsetT(fv.kids[i])
	}
	kidsFB := fv.b.EndVector(len(fv.kids))

	var dictFB flatbuffers.UOffsetT
	if field.Type.ID() == arrow.DICTIONARY {
		panic("not implemented") // FIXME(sbinet)
	}

	var (
		metaFB flatbuffers.UOffsetT
		kvs    []flatbuffers.UOffsetT
	)
	for i, k := range field.Metadata.Keys() {
		v := field.Metadata.Values()[i]
		kk := fv.b.CreateString(k)
		vv := fv.b.CreateString(v)
		flatbuf.KeyValueStart(fv.b)
		flatbuf.KeyValueAddKey(fv.b, kk)
		flatbuf.KeyValueAddValue(fv.b, vv)
		kvs = append(kvs, flatbuf.KeyValueEnd(fv.b))
	}
	{
		keys := make([]string, 0, len(fv.meta))
		for k := range fv.meta {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := fv.meta[k]
			kk := fv.b.CreateString(k)
			vv := fv.b.CreateString(v)
			flatbuf.KeyValueStart(fv.b)
			flatbuf.KeyValueAddKey(fv.b, kk)
			flatbuf.KeyValueAddValue(fv.b, vv)
			kvs = append(kvs, flatbuf.KeyValueEnd(fv.b))
		}
	}
	if len(kvs) > 0 {
		flatbuf.FieldStartCustomMetadataVector(fv.b, len(kvs))
		for i := len(kvs) - 1; i >= 0; i-- {
			fv.b.PrependUOffsetT(kvs[i])
		}
		metaFB = fv.b.EndVector(len(kvs))
	}

	flatbuf.FieldStart(fv.b)
	flatbuf.FieldAddName(fv.b, nameFB)
	flatbuf.FieldAddNullable(fv.b, field.Nullable)
	flatbuf.FieldAddTypeType(fv.b, fv.dtype)
	flatbuf.FieldAddType(fv.b, fv.offset)
	flatbuf.FieldAddDictionary(fv.b, dictFB)
	flatbuf.FieldAddChildren(fv.b, kidsFB)
	flatbuf.FieldAddCustomMetadata(fv.b, metaFB)

	offset := flatbuf.FieldEnd(fv.b)

	return offset
}

func intToFB(b *flatbuffers.Builder, bw int32, isSigned bool) flatbuffers.UOffsetT {
	flatbuf.IntStart(b)
	flatbuf.IntAddBitWidth(b, bw)
	flatbuf.IntAddIsSigned(b, isSigned)
	return flatbuf.IntEnd(b)
}

func floatToFB(b *flatbuffers.Builder, bw int32) flatbuffers.UOffsetT {
	switch bw {
	case 16:
		flatbuf.FloatingPointStart(b)
		flatbuf.FloatingPointAddPrecision(b, flatbuf.PrecisionHALF)
		return flatbuf.FloatingPointEnd(b)
	case 32:
		flatbuf.FloatingPointStart(b)
		flatbuf.FloatingPointAddPrecision(b, flatbuf.PrecisionSINGLE)
		return flatbuf.FloatingPointEnd(b)
	case 64:
		flatbuf.FloatingPointStart(b)
		flatbuf.FloatingPointAddPrecision(b, flatbuf.PrecisionDOUBLE)
		return flatbuf.FloatingPointEnd(b)
	default:
		panic(xerrors.Errorf("arrow/ipc: invalid floating point precision %d-bits", bw))
	}
}

func metadataToFB(b *flatbuffers.Builder, meta arrow.Metadata, start startVecFunc) flatbuffers.UOffsetT {
	if meta.Len() == 0 {
		return 0
	}

	n := meta.Len()
	kvs := make([]flatbuffers.UOffsetT, n)
	for i := range kvs {
		k := b.CreateString(meta.Keys()[i])
		v := b.CreateString(meta.Values()[i])
		flatbuf.KeyValueStart(b)
		flatbuf.KeyValueAddKey(b, k)
		flatbuf.KeyValueAddValue(b, v)
		kvs[i] = flatbuf.KeyValueEnd(b)
	}

	start(b, n)
	for i := n - 1; i >= 0; i-- {
		b.PrependUOffsetT(kvs[i])
	}
	return b.EndVector(n)
}

func schemaToFB(b *flatbuffers.Builder, schema *arrow.Schema, memo *dictMemo) flatbuffers.UOffsetT {
	fields := make([]flatbuffers.UOffsetT, len(schema.Fields()))
	for i, field := range schema.Fields() {
		fields[i] = fieldToFB(b, field, memo)
	}

	flatbuf.SchemaStartFieldsVector(b, len(fields))
	for i := len(fields) - 1; i >= 0; i-- {
		b.PrependUOffsetT(fields[i])
	}
	fieldsFB := b.EndVector(len(fields))

	metaFB := metadataToFB(b, schema.Metadata(), flatbuf.SchemaStartCustomMetadataVector)

	flatbuf.SchemaStart(b)
	flatbuf.SchemaAddEndianness(b, flatbuf.EndiannessLittle)
	flatbuf.SchemaAddFields(b, fieldsFB)
	flatbuf.SchemaAddCustomMetadata(b, metaFB)
	offset := flatbuf.SchemaEnd(b)

	return offset
}

// payloadsFromSchema returns a slice of payloads corresponding to the given schema.
// Callers of payloadsFromSchema will need to call Release after use.
func payloadsFromSchema(schema *arrow.Schema, mem memory.Allocator, memo *dictMemo) payloads {
	dict := newMemo()

	ps := make(payloads, 1, dict.Len()+1)
	ps[0].meta = writeSchemaMessage(schema, mem, &dict)

	// append dictionaries.
	if dict.Len() > 0 {
		panic("payloads-from-schema: not-implemented")
		//		for id, arr := range dict.id2dict {
		//			// GetSchemaPayloads: writer.cc:535
		//		}
	}

	if memo != nil {
		*memo = dict
	}

	return ps
}

func writeFBBuilder(b *flatbuffers.Builder, mem memory.Allocator) *memory.Buffer {
	raw := b.FinishedBytes()
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(len(raw))
	copy(buf.Bytes(), raw)
	return buf
}

func writeMessageFB(b *flatbuffers.Builder, mem memory.Allocator, hdrType flatbuf.MessageHeader, hdr flatbuffers.UOffsetT, bodyLen int64) *memory.Buffer {

	flatbuf.MessageStart(b)
	flatbuf.MessageAddVersion(b, int16(currentMetadataVersion))
	flatbuf.MessageAddHeaderType(b, hdrType)
	flatbuf.MessageAddHeader(b, hdr)
	flatbuf.MessageAddBodyLength(b, bodyLen)
	msg := flatbuf.MessageEnd(b)
	b.Finish(msg)

	return writeFBBuilder(b, mem)
}

func writeSchemaMessage(schema *arrow.Schema, mem memory.Allocator, dict *dictMemo) *memory.Buffer {
	b := flatbuffers.NewBuilder(1024)
	schemaFB := schemaToFB(b, schema, dict)
	return writeMessageFB(b, mem, flatbuf.MessageHeaderSchema, schemaFB, 0)
}

func writeRecordMessage(mem memory.Allocator, size, bodyLength int64, fields []fieldMetadata, meta []bufferMetadata) *memory.Buffer {
	b := flatbuffers.NewBuilder(0)
	recFB := recordToFB(b, size, bodyLength, fields, meta)
	return writeMessageFB(b, mem, flatbuf.MessageHeaderRecordBatch, recFB, bodyLength)
}

func recordToFB(b *flatbuffers.Builder, size, bodyLength int64, fields []fieldMetadata, meta []bufferMetadata) flatbuffers.UOffsetT {
	fieldsFB := writeFieldNodes(b, fields, flatbuf.RecordBatchStartNodesVector)
	metaFB := writeBuffers(b, meta, flatbuf.RecordBatchStartBuffersVector)

	flatbuf.RecordBatchStart(b)
	flatbuf.RecordBatchAddLength(b, size)
	flatbuf.RecordBatchAddNodes(b, fieldsFB)
	flatbuf.RecordBatchAddBuffers(b, metaFB)
	return flatbuf.RecordBatchEnd(b)
}

func writeFieldNodes(b *flatbuffers.Builder, fields []fieldMetadata, start startVecFunc) flatbuffers.UOffsetT {

	start(b, len(fields))
	for i := len(fields) - 1; i >= 0; i-- {
		field := fields[i]
		if field.Offset != 0 {
			panic(xerrors.Errorf("arrow/ipc: field metadata for IPC must have offset 0"))
		}
		flatbuf.CreateFieldNode(b, field.Len, field.Nulls)
	}

	return b.EndVector(len(fields))
}

func writeBuffers(b *flatbuffers.Builder, buffers []bufferMetadata, start startVecFunc) flatbuffers.UOffsetT {
	start(b, len(buffers))
	for i := len(buffers) - 1; i >= 0; i-- {
		buffer := buffers[i]
		flatbuf.CreateBuffer(b, buffer.Offset, buffer.Len)
	}
	return b.EndVector(len(buffers))
}
