package storage

import (
	"encoding/binary"
	"io"
)

type IntegerPointsDecoder struct {
	buf  []byte
	d    []byte
	t, v int64
}

func (d *IntegerPointsDecoder) SetBuf(b []byte) {
	d.buf = b
	d.d = b[4:]
	c := int(binary.BigEndian.Uint32(b[:4]))
	if len(d.d)*16 < c {
		panic(io.ErrShortBuffer)
	}
}

func (d *IntegerPointsDecoder) Next() bool {
	if len(d.d) < 16 {
		return false
	}
	d.t, d.d = int64(binary.BigEndian.Uint64(d.d)), d.d[8:]
	d.v, d.d = int64(binary.BigEndian.Uint64(d.d)), d.d[8:]
	return true
}

func (d *IntegerPointsDecoder) Read() (int64, int64) {
	return d.t, d.v
}
