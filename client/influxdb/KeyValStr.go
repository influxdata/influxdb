// automatically generated, do not modify

package influxdb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type KeyValStr struct {
	_tab flatbuffers.Table
}

func (rcv *KeyValStr) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *KeyValStr) Key() string {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.String(o + rcv._tab.Pos)
	}
	return ""
}

func (rcv *KeyValStr) Val() string {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.String(o + rcv._tab.Pos)
	}
	return ""
}

func KeyValStrStart(builder *flatbuffers.Builder) { builder.StartObject(2) }
func KeyValStrAddKey(builder *flatbuffers.Builder, Key flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(Key), 0)
}
func KeyValStrAddVal(builder *flatbuffers.Builder, Val flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(Val), 0)
}
func KeyValStrEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
