package tsm1

import (
	"encoding/binary"
	"fmt"
)

// BooleanArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capactity to b.
func BooleanArrayEncodeAll(src []bool, b []byte) ([]byte, error) {
	sz := 1 + 8 + ((len(src) + 7) / 8) // Header + Num bools + bool data.
	if len(b) < sz && cap(b) > sz {
		b = b[:sz]
	} else if len(b) < sz {
		b = append(b, make([]byte, sz)...)
	}

	// Store the encoding type in the 4 high bits of the first byte
	b[0] = byte(booleanCompressedBitPacked) << 4
	n := uint64(8) // Current bit in current byte.

	// Encode the number of booleans written.
	i := binary.PutUvarint(b[n>>3:], uint64(len(src)))
	n += uint64(i * 8)

	for _, v := range src {
		if v {
			b[n>>3] |= 128 >> (n & 7) // Set current bit on current byte.
		} else {
			b[n>>3] &^= 128 >> (n & 7) // Clear current bit on current byte.
		}
		n++
	}

	length := n >> 3
	if n&7 > 0 {
		length++ // Add an extra byte to capture overflowing bits.
	}
	return b[:length], nil
}

func BooleanArrayDecodeAll(b []byte, dst []bool) ([]bool, error) {
	if len(b) == 0 {
		return nil, nil
	}

	// First byte stores the encoding type, only have 1 bit-packet format
	// currently ignore for now.
	b = b[1:]
	val, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, fmt.Errorf("BooleanBatchDecoder: invalid count")
	}

	count := int(val)

	b = b[n:]
	if min := len(b) * 8; min < count {
		// Shouldn't happen - TSM file was truncated/corrupted
		count = min
	}

	if cap(dst) < count {
		dst = make([]bool, count)
	} else {
		dst = dst[:count]
	}

	j := 0
	for _, v := range b {
		for i := byte(128); i > 0 && j < len(dst); i >>= 1 {
			dst[j] = v&i != 0
			j++
		}
	}
	return dst, nil
}
