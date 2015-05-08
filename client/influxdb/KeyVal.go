// automatically generated, do not modify

package influxdb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type KeyVal struct {
	_tab flatbuffers.Table
}

func (rcv *KeyVal) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *KeyVal) Key() string {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.String(o + rcv._tab.Pos)
	}
	return ""
}

func (rcv *KeyVal) ValType() byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetByte(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *KeyVal) Val(obj *flatbuffers.Table) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		rcv._tab.Union(obj, o)
		return true
	}
	return false
}

func KeyValStart(builder *flatbuffers.Builder) { builder.StartObject(3) }
func KeyValAddKey(builder *flatbuffers.Builder, Key flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(Key), 0)
}
func KeyValAddValType(builder *flatbuffers.Builder, ValType byte) {
	builder.PrependByteSlot(1, ValType, 0)
}
func KeyValAddVal(builder *flatbuffers.Builder, Val flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(Val), 0)
}
func KeyValEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
