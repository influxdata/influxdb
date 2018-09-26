package tsm1

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/encoding/simple8b"
)

var (
	timeBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		timeBatchDecodeAllUncompressed,
		timeBatchDecodeAllSimple,
		timeBatchDecodeAllRLE,
		timeBatchDecodeAllInvalid,
	}
)

func TimeBatchDecodeAll(b []byte, dst []int64) ([]int64, error) {
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
		return []int64{}, fmt.Errorf("TimeBatchDecodeAll: expected multiple of 8 bytes")
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
		return []int64{}, fmt.Errorf("TimeBatchDecodeAll: not enough data to decode packed timestamps")
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
		return []int64{}, fmt.Errorf("TimeBatchDecodeAll: unexpected number of values decoded; got=%d, exp=%d", n, count-1)
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
		return []int64{}, fmt.Errorf("TimeBatchDecodeAll: not enough data to decode RLE starting value")
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
		return []int64{}, fmt.Errorf("TimeBatchDecodeAll: invalid run length in decodeRLE")
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
