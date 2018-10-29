package tsm1

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/encoding/simple8b"
)

// TimeArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// TimeArrayEncodeAll implements batch oriented versions of the three integer
// encoding types we support: uncompressed, simple8b and RLE.
//
// Timestamp values to be encoded should be sorted before encoding.  When encoded,
// the values are first delta-encoded.  The first value is the starting timestamp,
// subsequent values are the difference from the prior value.
//
// Important: TimeArrayEncodeAll modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into TimeArrayEncodeAll.
func TimeArrayEncodeAll(src []int64, b []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil // Nothing to do
	}

	var max, div = uint64(0), uint64(1e12)

	// To prevent an allocation of the entire block we're encoding reuse the
	// src slice to store the encoded deltas.
	deltas := reintepretInt64ToUint64Slice(src)

	if len(deltas) > 1 {
		for i := len(deltas) - 1; i > 0; i-- {
			deltas[i] = deltas[i] - deltas[i-1]
			if deltas[i] > max {
				max = deltas[i]
			}
		}

		var rle = true
		for i := 2; i < len(deltas); i++ {
			if deltas[1] != deltas[i] {
				rle = false
				break
			}
		}

		// Deltas are the same - encode with RLE
		if rle {
			// Large varints can take up to 10 bytes.  We're storing 3 + 1
			// type byte.
			if len(b) < 31 && cap(b) >= 31 {
				b = b[:31]
			} else if len(b) < 31 {
				b = append(b, make([]byte, 31-len(b))...)
			}

			// 4 high bits used for the encoding type
			b[0] = byte(timeCompressedRLE) << 4

			i := 1
			// The first value
			binary.BigEndian.PutUint64(b[i:], deltas[0])
			i += 8

			// The first delta, checking the divisor
			// given all deltas are the same, we can do a single check for the divisor
			v := deltas[1]
			for div > 1 && v%div != 0 {
				div /= 10
			}

			if div > 1 {
				// 4 low bits are the log10 divisor
				b[0] |= byte(math.Log10(float64(div)))
				i += binary.PutUvarint(b[i:], deltas[1]/div)
			} else {
				i += binary.PutUvarint(b[i:], deltas[1])
			}

			// The number of times the delta is repeated
			i += binary.PutUvarint(b[i:], uint64(len(deltas)))

			return b[:i], nil
		}
	}

	// We can't compress this time-range, the deltas exceed 1 << 60
	if max > simple8b.MaxValue {
		// Encode uncompressed.
		sz := 1 + len(deltas)*8
		if len(b) < sz && cap(b) >= sz {
			b = b[:sz]
		} else if len(b) < sz {
			b = append(b, make([]byte, sz-len(b))...)
		}

		// 4 high bits of first byte store the encoding type for the block
		b[0] = byte(timeUncompressed) << 4
		for i, v := range deltas {
			binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], v)
		}
		return b[:sz], nil
	}

	// find divisor only if we're compressing with simple8b
	for i := 1; i < len(deltas) && div > 1; i++ {
		// If our value is divisible by 10, break.  Otherwise, try the next smallest divisor.
		v := deltas[i]
		for div > 1 && v%div != 0 {
			div /= 10
		}
	}

	// Only apply the divisor if it's greater than 1 since division is expensive.
	if div > 1 {
		for i := 1; i < len(deltas); i++ {
			deltas[i] /= div
		}
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
	b[0] = byte(timeCompressedPackedSimple) << 4
	// 4 low bits are the log10 divisor
	b[0] |= byte(math.Log10(float64(div)))

	// Write the first value since it's not part of the encoded values
	binary.BigEndian.PutUint64(b[1:9], deltas[0])

	// Write the encoded values
	for i, v := range encoded {
		binary.BigEndian.PutUint64(b[9+i*8:9+i*8+8], v)
	}
	return b[:sz], nil
}

var (
	timeBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		timeBatchDecodeAllUncompressed,
		timeBatchDecodeAllSimple,
		timeBatchDecodeAllRLE,
		timeBatchDecodeAllInvalid,
	}
)

func TimeArrayDecodeAll(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > timeCompressedRLE {
		encoding = 3 // timeBatchDecodeAllInvalid
	}

	return timeBatchDecoderFunc[encoding&3](b, dst)
}

func timeBatchDecodeAllUncompressed(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	prev := uint64(0)
	for i := range dst {
		prev += binary.BigEndian.Uint64(b[i*8:])
		dst[i] = int64(prev)
	}

	return dst, nil
}

func timeBatchDecodeAllSimple(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: not enough data to decode packed timestamps")
	}

	div := uint64(math.Pow10(int(b[0] & 0xF))) // multiplier

	count, err := simple8b.CountBytes(b[9:])
	if err != nil {
		return []int64{}, err
	}

	count += 1

	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	buf := *(*[]uint64)(unsafe.Pointer(&dst))

	// first value
	buf[0] = binary.BigEndian.Uint64(b[1:9])
	n, err := simple8b.DecodeBytesBigEndian(buf[1:], b[9:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	// Compute the prefix sum and scale the deltas back up
	last := buf[0]
	if div > 1 {
		for i := 1; i < len(buf); i++ {
			dgap := buf[i] * div
			buf[i] = last + dgap
			last = buf[i]
		}
	} else {
		for i := 1; i < len(buf); i++ {
			buf[i] += last
			last = buf[i]
		}
	}

	return dst, nil
}

func timeBatchDecodeAllRLE(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: not enough data to decode RLE starting value")
	}

	var k, n int

	// Lower 4 bits hold the 10 based exponent so we can scale the values back up
	mod := int64(math.Pow10(int(b[k] & 0xF)))
	k++

	// Next 8 bytes is the starting timestamp
	first := binary.BigEndian.Uint64(b[k:])
	k += 8

	// Next 1-10 bytes is our (scaled down by factor of 10) run length delta
	delta, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: invalid run length in decodeRLE")
	}
	k += n

	// Scale the delta back up
	delta *= uint64(mod)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("TimeDecoder: invalid repeat value in decodeRLE")
	}

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	acc := first
	for i := range dst {
		dst[i] = int64(acc)
		acc += delta
	}

	return dst, nil
}

func timeBatchDecodeAllInvalid(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("unknown encoding %v", b[0]>>4)
}
