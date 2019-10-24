package tsm1

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/encoding/simple8b"
)

// IntegerArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capactity to b.
//
// IntegerArrayEncodeAll implements batch oriented versions of the three integer
// encoding types we support: uncompressed, simple8b and RLE.
//
// Important: IntegerArrayEncodeAll modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into IntegerArrayEncodeAll.
func IntegerArrayEncodeAll(src []int64, b []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil // Nothing to do
	}

	var max = uint64(0)

	// To prevent an allocation of the entire block we're encoding reuse the
	// src slice to store the encoded deltas.
	deltas := reintepretInt64ToUint64Slice(src)
	for i := len(deltas) - 1; i > 0; i-- {
		deltas[i] = deltas[i] - deltas[i-1]
		deltas[i] = ZigZagEncode(int64(deltas[i]))
		if deltas[i] > max {
			max = deltas[i]
		}
	}

	deltas[0] = ZigZagEncode(int64(deltas[0]))

	if len(deltas) > 2 {
		var rle = true
		for i := 2; i < len(deltas); i++ {
			if deltas[1] != deltas[i] {
				rle = false
				break
			}
		}

		if rle {
			// Large varints can take up to 10 bytes.  We're storing 3 + 1
			// type byte.
			if len(b) < 31 && cap(b) >= 31 {
				b = b[:31]
			} else if len(b) < 31 {
				b = append(b, make([]byte, 31-len(b))...)
			}

			// 4 high bits used for the encoding type
			b[0] = byte(intCompressedRLE) << 4

			i := 1
			// The first value
			binary.BigEndian.PutUint64(b[i:], deltas[0])
			i += 8
			// The first delta
			i += binary.PutUvarint(b[i:], deltas[1])
			// The number of times the delta is repeated
			i += binary.PutUvarint(b[i:], uint64(len(deltas)-1))

			return b[:i], nil
		}
	}

	if max > simple8b.MaxValue { // There is an encoded value that's too big to simple8b encode.
		// Encode uncompressed.
		sz := 1 + len(deltas)*8
		if len(b) < sz && cap(b) >= sz {
			b = b[:sz]
		} else if len(b) < sz {
			b = append(b, make([]byte, sz-len(b))...)
		}

		// 4 high bits of first byte store the encoding type for the block
		b[0] = byte(intUncompressed) << 4
		for i, v := range deltas {
			binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], uint64(v))
		}
		return b[:sz], nil
	}

	// Encode with simple8b - fist value is written unencoded using 8 bytes.
	encoded, err := simple8b.EncodeAll(deltas[1:])
	if err != nil {
		return nil, err
	}

	sz := 1 + (len(encoded)+1)*8
	if len(b) < sz && cap(b) >= sz {
		b = b[:sz]
	} else if len(b) < sz {
		b = append(b, make([]byte, sz-len(b))...)
	}

	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(intCompressedSimple) << 4

	// Write the first value since it's not part of the encoded values
	binary.BigEndian.PutUint64(b[1:9], deltas[0])

	// Write the encoded values
	for i, v := range encoded {
		binary.BigEndian.PutUint64(b[9+i*8:9+i*8+8], v)
	}
	return b, nil
}

// UnsignedArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capactity to b.
//
// UnsignedArrayEncodeAll implements batch oriented versions of the three integer
// encoding types we support: uncompressed, simple8b and RLE.
//
// Important: IntegerArrayEncodeAll modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into IntegerArrayEncodeAll.
func UnsignedArrayEncodeAll(src []uint64, b []byte) ([]byte, error) {
	srcint := reintepretUint64ToInt64Slice(src)
	return IntegerArrayEncodeAll(srcint, b)
}

var (
	integerBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		integerBatchDecodeAllUncompressed,
		integerBatchDecodeAllSimple,
		integerBatchDecodeAllRLE,
		integerBatchDecodeAllInvalid,
	}
)

func IntegerArrayDecodeAll(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	return integerBatchDecoderFunc[encoding&3](b, dst)
}

func UnsignedArrayDecodeAll(b []byte, dst []uint64) ([]uint64, error) {
	if len(b) == 0 {
		return []uint64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	res, err := integerBatchDecoderFunc[encoding&3](b, reintepretUint64ToInt64Slice(dst))
	return reintepretInt64ToUint64Slice(res), err
}

func integerBatchDecodeAllUncompressed(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	prev := int64(0)
	for i := range dst {
		prev += ZigZagDecode(binary.BigEndian.Uint64(b[i*8:]))
		dst[i] = prev
	}

	return dst, nil
}

func integerBatchDecodeAllSimple(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: not enough data to decode packed value")
	}

	count, err := simple8b.CountBytes(b[8:])
	if err != nil {
		return []int64{}, err
	}

	count += 1
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	// first value
	dst[0] = ZigZagDecode(binary.BigEndian.Uint64(b))

	// decode compressed values
	buf := reintepretInt64ToUint64Slice(dst)
	n, err := simple8b.DecodeBytesBigEndian(buf[1:], b[8:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	// calculate prefix sum
	prev := dst[0]
	for i := 1; i < len(dst); i++ {
		prev += ZigZagDecode(uint64(dst[i]))
		dst[i] = prev
	}

	return dst, nil
}

func integerBatchDecodeAllRLE(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: not enough data to decode RLE starting value")
	}

	var k, n int

	// Next 8 bytes is the starting value
	first := ZigZagDecode(binary.BigEndian.Uint64(b[k : k+8]))
	k += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: invalid RLE delta value")
	}
	k += n

	delta := ZigZagDecode(value)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: invalid RLE repeat value")
	}

	count += 1

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	if delta == 0 {
		for i := range dst {
			dst[i] = first
		}
	} else {
		acc := first
		for i := range dst {
			dst[i] = acc
			acc += delta
		}
	}

	return dst, nil
}

func integerBatchDecodeAllInvalid(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("unknown encoding %v", b[0]>>4)
}

func reintepretInt64ToUint64Slice(src []int64) []uint64 {
	return *(*[]uint64)(unsafe.Pointer(&src))
}

func reintepretUint64ToInt64Slice(src []uint64) []int64 {
	return *(*[]int64)(unsafe.Pointer(&src))
}
