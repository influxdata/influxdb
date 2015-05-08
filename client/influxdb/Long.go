// automatically generated, do not modify

package influxdb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Long struct {
	_tab flatbuffers.Table
}

func (rcv *Long) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Long) Val() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func LongStart(builder *flatbuffers.Builder)                    { builder.StartObject(1) }
func LongAddVal(builder *flatbuffers.Builder, Val int64)        { builder.PrependInt64Slot(0, Val, 0) }
func LongEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
