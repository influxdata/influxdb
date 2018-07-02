package tsm1

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/encoding/simple8b"
)

var (
	integerBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		integerBatchDecodeAllUncompressed,
		integerBatchDecodeAllSimple,
		integerBatchDecodeAllRLE,
		integerBatchDecodeAllInvalid,
	}
)

func IntegerBatchDecodeAll(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	return integerBatchDecoderFunc[encoding&3](b, dst)
}

func UnsignedBatchDecodeAll(b []byte, dst []uint64) ([]uint64, error) {
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
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: expected multiple of 8 bytes")
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
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: not enough data to decode packed value")
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
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: unexpected number of values decoded; got=%d, exp=%d", n, count-1)
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
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: not enough data to decode RLE starting value")
	}

	var k, n int

	// Next 8 bytes is the starting value
	first := ZigZagDecode(binary.BigEndian.Uint64(b[k : k+8]))
	k += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: invalid RLE delta value")
	}
	k += n

	delta := ZigZagDecode(value)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: invalid RLE repeat value")
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
