package tsm1

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/golang/snappy"
)

var (
	errStringBatchDecodeInvalidStringLength = fmt.Errorf("StringArrayDecodeAll: invalid encoded string length")
	errStringBatchDecodeLengthOverflow      = fmt.Errorf("StringArrayDecodeAll: length overflow")
	errStringBatchDecodeShortBuffer         = fmt.Errorf("StringArrayDecodeAll: short buffer")
)

// StringArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capactity to b.
//
// Currently only the string compression scheme used snappy.
func StringArrayEncodeAll(src []string, b []byte) ([]byte, error) {
	// As a heuristic assume 2 bytes per string (upto 127 byte length).
	sz := 2 + (len(src) * 2) // First 2 bytes is assuming empty input (snappy expects 1 byte of data).
	if len(b) < sz && cap(b) < sz {
		// Not enough capacity, need to grow buffer.
		b = append(b[:0], make([]byte, sz-len(b))...)
	}
	b = b[:sz]

	// Shortcut to snappy encoding nothing.
	if len(src) == 0 {
		b[0] = stringCompressedSnappy << 4
		return b[:2], nil
	}

	n := 0 // Number of bytes written to buffer.
	for _, v := range src {
		sz := 10          // Maximum needed size for this string.
		rem := len(b) - n // Bytes available in b.
		if rem < sz && cap(b) >= n+sz {
			b = b[:n+sz] // Enough capacity in b to expand.
		} else if rem < sz {
			b = append(b, make([]byte, sz-rem)...) // Need to grow b.
		}
		n += binary.PutUvarint(b[n:], uint64(len(v)))
		b = append(b[:n], v...)
		n += len(v)
	}

	// Ensure there is room to add the header byte.
	if n == len(b) {
		b = append(b, byte(stringCompressedSnappy<<4))
	} else {
		b[n] = byte(stringCompressedSnappy << 4)
	}
	n++

	// Grow b to include the compressed data. That way we don't need to allocate
	// a slice only to throw it away.
	sz = snappy.MaxEncodedLen(n - 1) // Don't need to consider header
	rem := len(b) - n                // Bytes available in b.
	if rem < sz && cap(b) >= n+sz {
		b = b[:n+sz] // Enough capacity in b to just expand.
	} else if rem < sz {
		b = append(b, make([]byte, sz-rem)...) // Need to grow b.
	}
	res := snappy.Encode(b[n:], b[:n-1]) // Don't encode header byte.

	return b[n-1 : n+len(res)], nil // Include header byte in returned data.
}

func StringArrayDecodeAll(b []byte, dst []string) ([]string, error) {
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
