package tsm1

// Int64 encoding uses two different strategies depending on the range of values in
// the uncompressed data.  Encoded values are first encoding used zig zag encoding.
// This interleaves postiive and negative integers across a range of positive integers.
//
// For example, [-2,-1,0,1] becomes [3,1,0,2]. See
// https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers
// for more information.
//
// If all the zig zag encoded values less than 1 << 60 - 1, they are compressed using
// simple8b encoding.  If any values is larger than 1 << 60 - 1, the values are stored uncompressed.
//
// Each encoded byte slice, contains a 1 byte header followed by multiple 8 byte packed integers
// or 8 byte uncompressed integers.  The 4 high bits of the first byte indicate the encoding type
// for the remaining bytes.
//
// There are currently two encoding types that can be used with room for 16 total.  These additional
// encoding slots are reserved for future use.  One improvement to be made is to use a patched
// encoding such as PFOR if only a small number of values exceed the max compressed value range.  This
// should improve compression ratios with very large integers near the ends of the int64 range.

import (
	"encoding/binary"
	"fmt"

	"github.com/jwilder/encoding/simple8b"
)

const (
	// intUncompressed is an uncompressed format using 8 bytes per point
	intUncompressed = 0
	// intCompressedSimple is a bit-packed format using simple8b encoding
	intCompressedSimple = 1
)

// Int64Encoder encoders int64 into byte slices
type Int64Encoder interface {
	Write(v int64)
	Bytes() ([]byte, error)
}

// Int64Decoder decodes a byte slice into int64s
type Int64Decoder interface {
	Next() bool
	Read() int64
	Error() error
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
	encoded, err := simple8b.EncodeAll(e.values)
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1+len(encoded)*8)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(intCompressedSimple) << 4

	for i, v := range encoded {
		binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], v)
	}
	return b, nil
}

func (e *int64Encoder) encodeUncompressed() ([]byte, error) {
	b := make([]byte, 1+len(e.values)*8)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(intUncompressed) << 4

	for i, v := range e.values {
		binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], v)
	}
	return b, nil
}

type int64Decoder struct {
	values []uint64
	bytes  []byte
	i      int
	n      int

	encoding byte
	err      error
}

func NewInt64Decoder(b []byte) Int64Decoder {
	d := &int64Decoder{
		// 240 is the maximum number of values that can be encoded into a single uint64 using simple8b
		values: make([]uint64, 240),
	}

	d.SetBytes(b)
	return d
}

func (d *int64Decoder) SetBytes(b []byte) {
	if len(b) > 0 {
		d.encoding = b[0] >> 4
		d.bytes = b[1:]
	}
	d.i = 0
	d.n = 0
}

func (d *int64Decoder) Next() bool {
	if d.i >= d.n && len(d.bytes) == 0 {
		return false
	}

	d.i += 1

	if d.i >= d.n {
		switch d.encoding {
		case intUncompressed:
			d.decodeUncompressed()
		case intCompressedSimple:
			d.decodePacked()
		default:
			d.err = fmt.Errorf("unknown encoding %v", d.encoding)
		}
	}
	return d.i < d.n
}

func (d *int64Decoder) Error() error {
	return d.err
}

func (d *int64Decoder) Read() int64 {
	return ZigZagDecode(d.values[d.i])
}

func (d *int64Decoder) decodePacked() {
	if len(d.bytes) == 0 {
		return
	}

	v := binary.BigEndian.Uint64(d.bytes[0:8])
	n, err := simple8b.Decode(d.values, v)
	if err != nil {
		// Should never happen, only error that could be returned is if the the value to be decoded was not
		// actually encoded by simple8b encoder.
		d.err = fmt.Errorf("failed to decode value %v: %v", v, err)
	}

	d.n = n
	d.i = 0
	d.bytes = d.bytes[8:]
}

func (d *int64Decoder) decodeUncompressed() {
	d.values[0] = binary.BigEndian.Uint64(d.bytes[0:8])
	d.i = 0
	d.n = 1
	d.bytes = d.bytes[8:]
}
