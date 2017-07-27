package storage

import (
	"encoding/binary"
	"io"
)

type frameEncoder struct {
	buf []byte
}

func (e *frameEncoder) EncodeSeries(v ReadResponse_SeriesFrame) []byte {
	sz := v.Size()
	e.buf[0] = byte(FrameTypeSeries)
	binary.BigEndian.PutUint32(e.buf[1:5], uint32(sz))
	v.MarshalTo(e.buf[5:])
	return e.buf[:sz+5]
}

func (e *frameEncoder) EncodeIntegerPoints(v ReadResponse_IntegerPointsFrame) []byte {
	sz := v.Size()
	e.buf[0] = byte(FrameTypePoints)
	binary.BigEndian.PutUint32(e.buf[1:5], uint32(sz))
	v.MarshalTo(e.buf[5:])
	return e.buf[:sz+5]
}

type FrameDecoder struct {
	buf []byte
}

func (f *FrameDecoder) SetBuf(b []byte) {
	f.buf = b
}

func (f *FrameDecoder) Next() (ReadResponse_FrameType, []byte, error) {
	if len(f.buf) == 0 {
		return 0, nil, io.EOF
	}

	t := ReadResponse_FrameType(f.buf[0])
	l := binary.BigEndian.Uint32(f.buf[1:5])
	var buf []byte
	buf, f.buf = f.buf[5:l+5], f.buf[l+5:]
	return t, buf, nil
}
