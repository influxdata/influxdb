package tsm1

import (
	"encoding/binary"
	"fmt"
)

func BooleanBatchDecodeAll(b []byte, dst []bool) ([]bool, error) {
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
