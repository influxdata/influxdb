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
	srcSz := 2 + len(src)*binary.MaxVarintLen32 // strings should't be longer than 64kb
	for i := range src {
		srcSz += len(src[i])
	}

	// determine the maximum possible length needed for the buffer, which
	// includes the compressed size
	var compressedSz = 0
	if len(src) > 0 {
		compressedSz = snappy.MaxEncodedLen(srcSz) + 1 /* header */
	}
	totSz := srcSz + compressedSz

	if cap(b) < totSz {
		b = make([]byte, totSz)
	} else {
		b = b[:totSz]
	}

	// Shortcut to snappy encoding nothing.
	if len(src) == 0 {
		b[0] = stringCompressedSnappy << 4
		return b[:2], nil
	}

	// write the data to be compressed *after* the space needed for snappy
	// compression. The compressed data is at the start of the allocated buffer,
	// ensuring the entire capacity is returned and available for subsequent use.
	dta := b[compressedSz:]
	n := 0
	for i := range src {
		n += binary.PutUvarint(dta[n:], uint64(len(src[i])))
		n += copy(dta[n:], src[i])
	}
	dta = dta[:n]

	dst := b[:compressedSz]
	dst[0] = stringCompressedSnappy << 4
	res := snappy.Encode(dst[1:], dta)
	return dst[:len(res)+1], nil
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
