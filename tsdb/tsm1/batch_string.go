package tsm1

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/golang/snappy"
)

var (
	errStringBatchDecodeInvalidStringLength = fmt.Errorf("StringBatchDecodeAll: invalid encoded string length")
	errStringBatchDecodeLengthOverflow      = fmt.Errorf("StringBatchDecodeAll: length overflow")
	errStringBatchDecodeShortBuffer         = fmt.Errorf("StringBatchDecodeAll: short buffer")
)

func StringBatchDecodeAll(b []byte, dst []string) ([]string, error) {
	// First byte stores the encoding type, only have snappy format
	// currently so ignore for now.
	if len(b) > 0 {
		var err error
		// it is important that to note that `snappy.Decode` always returns
		// a newly allocated slice as the final strings reference this slice
		// directly.
		b, err = snappy.Decode(nil, b[1:])
		if err != nil {
			return []string{}, fmt.Errorf("failed to decode string block: %v", err.Error())
		}
	} else {
		return []string{}, nil
	}

	var (
		i, l int
	)

	sz := cap(dst)
	if sz == 0 {
		sz = 64
		dst = make([]string, sz)
	} else {
		dst = dst[:sz]
	}

	j := 0

	for i < len(b) {
		length, n := binary.Uvarint(b[i:])
		if n <= 0 {
			return []string{}, errStringBatchDecodeInvalidStringLength
		}

		// The length of this string plus the length of the variable byte encoded length
		l = int(length) + n

		lower := i + n
		upper := lower + int(length)
		if upper < lower {
			return []string{}, errStringBatchDecodeLengthOverflow
		}
		if upper > len(b) {
			return []string{}, errStringBatchDecodeShortBuffer
		}

		// NOTE: this optimization is critical for performance and to reduce
		// allocations. This is just as "safe" as string.Builder, which
		// returns a string mapped to the original byte slice
		s := b[lower:upper]
		val := *(*string)(unsafe.Pointer(&s))
		if j < len(dst) {
			dst[j] = val
		} else {
			dst = append(dst, val) // force a resize
			dst = dst[:cap(dst)]
		}
		i += l
		j++
	}

	return dst[:j], nil
}
