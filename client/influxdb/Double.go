// automatically generated, do not modify

package influxdb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Double struct {
	_tab flatbuffers.Table
}

func (rcv *Double) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Double) Val() float64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetFloat64(o + rcv._tab.Pos)
	}
	return 0
}

func DoubleStart(builder *flatbuffers.Builder)                    { builder.StartObject(1) }
func DoubleAddVal(builder *flatbuffers.Builder, Val float64)      { builder.PrependFloat64Slot(0, Val, 0) }
func DoubleEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
