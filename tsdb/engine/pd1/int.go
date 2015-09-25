package pd1

import (
	"encoding/binary"
	"fmt"

	"github.com/jwilder/encoding/simple8b"
)

type Int64Encoder interface {
	Write(v int64)
	Bytes() ([]byte, error)
}

type Int64Decoder interface {
	Next() bool
	Read() int64
}

type int64Encoder struct {
	values []uint64
}

func NewInt64Encoder() Int64Encoder {
	return &int64Encoder{}
}

func (e *int64Encoder) Write(v int64) {
	e.values = append(e.values, ZigZagEncode(v))
}

func (e *int64Encoder) Bytes() ([]byte, error) {
	for _, v := range e.values {
		// Value is too large to encode using packed format
		if v > simple8b.MaxValue {
			return e.encodeUncompressed()
		}
	}

	return e.encodePacked()
}

func (e *int64Encoder) encodePacked() ([]byte, error) {
	encoded, err := simple8b.Encode(e.values)
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1+len(encoded)*8+4)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(EncodingPacked) << 4

	binary.BigEndian.PutUint32(b[1:5], uint32(len(e.values)))

	for i, v := range encoded {
		binary.BigEndian.PutUint64(b[5+i*8:5+i*8+8], v)
	}
	return b, nil
}

func (e *int64Encoder) encodeUncompressed() ([]byte, error) {
	b := make([]byte, 1+len(e.values)*8)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(EncodingUncompressed) << 4

	for i, v := range e.values {
		binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], uint64(v))
	}
	return b, nil
}

type int64Decoder struct {
	values []uint64
	v      int64
	buf    []uint64
	vbuf   []uint64
}

func NewInt64Decoder(b []byte) Int64Decoder {
	d := &int64Decoder{
		buf:  make([]uint64, 240),
		vbuf: make([]uint64, 1),
	}
	d.decode(b)
	return d
}

func (d *int64Decoder) SetBytes(b []byte) {
	d.decode(b)
}

func (d *int64Decoder) Next() bool {
	if len(d.values) == 0 {
		return false
	}
	d.v = ZigZagDecode(d.values[0])
	d.values = d.values[1:]
	return true
}

func (d *int64Decoder) Read() int64 {
	return d.v
}

func (d *int64Decoder) decode(b []byte) {
	if len(b) == 0 {
		return
	}

	// Encoding type is stored in the 4 high bits of the first byte
	encoding := b[0] >> 4
	switch encoding {
	case EncodingUncompressed:
		d.decodeUncompressed(b[1:])
	case EncodingPacked:
		d.decodePacked(b[1:])
	default:
		panic(fmt.Sprintf("unknown encoding %v", encoding))
	}
}

func (d *int64Decoder) decodePacked(b []byte) {
	if len(b) == 0 {
		return
	}

	count := binary.BigEndian.Uint32(b[:4])

	if count == 0 {
		return
	}

	d.values = make([]uint64, count)
	b = b[4:]
	j := 0
	for i := 0; i < len(b); i += 8 {
		d.vbuf[0] = binary.BigEndian.Uint64(b[i : i+8])
		n, _ := simple8b.Decode(d.buf, d.vbuf)
		copy(d.values[j:], d.buf[:n])
		j += n
	}
}

func (d *int64Decoder) decodeUncompressed(b []byte) {
	d.values = make([]uint64, len(b)/8)
	for i := range d.values {
		d.values[i] = binary.BigEndian.Uint64(b[i*8 : i*8+8])
	}
}
