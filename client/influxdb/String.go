// automatically generated, do not modify

package influxdb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type String struct {
	_tab flatbuffers.Table
}

func (rcv *String) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *String) Val() string {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.String(o + rcv._tab.Pos)
	}
	return ""
}

func StringStart(builder *flatbuffers.Builder) { builder.StartObject(1) }
func StringAddVal(builder *flatbuffers.Builder, Val flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(Val), 0)
}
func StringEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
