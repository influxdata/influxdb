// automatically generated, do not modify

package influxdb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Bool struct {
	_tab flatbuffers.Table
}

func (rcv *Bool) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Bool) Val() byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetByte(o + rcv._tab.Pos)
	}
	return 0
}

func BoolStart(builder *flatbuffers.Builder)                    { builder.StartObject(1) }
func BoolAddVal(builder *flatbuffers.Builder, Val byte)         { builder.PrependByteSlot(0, Val, 0) }
func BoolEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
