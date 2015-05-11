// automatically generated, do not modify

package influxdb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type BatchPoints struct {
	_tab flatbuffers.Table
}

func GetRootAsBatchPoints(buf []byte, offset flatbuffers.UOffsetT) *BatchPoints {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &BatchPoints{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *BatchPoints) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *BatchPoints) Database() string {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.String(o + rcv._tab.Pos)
	}
	return ""
}

func (rcv *BatchPoints) RetentionPolicy() string {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.String(o + rcv._tab.Pos)
	}
	return ""
}

func (rcv *BatchPoints) Timestamp() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *BatchPoints) Precision() string {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.String(o + rcv._tab.Pos)
	}
	return ""
}

func (rcv *BatchPoints) Tags(obj *KeyValStr, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		if obj == nil {
			obj = new(KeyValStr)
		}
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *BatchPoints) TagsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *BatchPoints) Points(obj *Point, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		if obj == nil {
			obj = new(Point)
		}
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *BatchPoints) PointsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func BatchPointsStart(builder *flatbuffers.Builder) { builder.StartObject(6) }
func BatchPointsAddDatabase(builder *flatbuffers.Builder, Database flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(Database), 0)
}
func BatchPointsAddRetentionPolicy(builder *flatbuffers.Builder, RetentionPolicy flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(RetentionPolicy), 0)
}
func BatchPointsAddTimestamp(builder *flatbuffers.Builder, Timestamp int64) {
	builder.PrependInt64Slot(2, Timestamp, 0)
}
func BatchPointsAddPrecision(builder *flatbuffers.Builder, Precision flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(Precision), 0)
}
func BatchPointsAddTags(builder *flatbuffers.Builder, Tags flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(Tags), 0)
}
func BatchPointsStartTagsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func BatchPointsAddPoints(builder *flatbuffers.Builder, Points flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(5, flatbuffers.UOffsetT(Points), 0)
}
func BatchPointsStartPointsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func BatchPointsEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
