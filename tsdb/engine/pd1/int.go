package pd1

import (
	"encoding/binary"
	"fmt"

	"github.com/jwilder/encoding/simple8b"
)

type int64Encoder struct {
	values []int64
}

func NewInt64Encoder() *int64Encoder {
	return &int64Encoder{}
}

func (e *int64Encoder) Write(v int64) {
	e.values = append(e.values, v)
}

func (e *int64Encoder) Bytes() ([]byte, error) {
	enc := simple8b.NewEncoder()

	for _, v := range e.values {
		n := ZigZagEncode(v)
		// Value is too large to encode using packed format
		if n > simple8b.MaxValue {
			return e.encodeUncompressed()
		}
		enc.Write(n)
	}

	b, err := enc.Bytes()
	if err != nil {
		return nil, err
	}

	return append([]byte{EncodingPacked << 4}, b...), nil
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
	values []int64
	v      int64
}

func NewInt64Decoder(b []byte) *int64Decoder {
	d := &int64Decoder{}
	d.decode(b)
	return d
}

func (d *int64Decoder) Next() bool {
	if len(d.values) == 0 {
		return false
	}
	d.v = d.values[0]
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
	dec := simple8b.NewDecoder(b)
	for dec.Next() {
		d.values = append(d.values, ZigZagDecode(dec.Read()))
	}
}

func (d *int64Decoder) decodeUncompressed(b []byte) {
	d.values = make([]int64, len(b)/8)
	for i := range d.values {
		d.values[i] = int64(binary.BigEndian.Uint64(b[i*8 : i*8+8]))
	}
}
