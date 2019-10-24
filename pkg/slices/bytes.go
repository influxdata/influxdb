package slices

import "bytes"

// BytesToStrings converts a slice of []byte into a slice of strings.
func BytesToStrings(a [][]byte) []string {
	s := make([]string, 0, len(a))
	for _, v := range a {
		s = append(s, string(v))
	}
	return s
}

// CopyChunkedByteSlices deep-copies a [][]byte to a new [][]byte that is backed by a small number of []byte "chunks".
func CopyChunkedByteSlices(src [][]byte, chunkSize int) [][]byte {
	dst := make([][]byte, len(src))

	for chunkBegin := 0; chunkBegin < len(src); chunkBegin += chunkSize {
		chunkEnd := len(src)
		if chunkEnd-chunkBegin > chunkSize {
			chunkEnd = chunkBegin + chunkSize
		}

		chunkByteSize := 0
		for j := chunkBegin; j < chunkEnd; j++ {
			chunkByteSize += len(src[j])
		}

		chunk := make([]byte, chunkByteSize)
		offset := 0
		for j := chunkBegin; j < chunkEnd; j++ {
			copy(chunk[offset:offset+len(src[j])], src[j])
			dst[j] = chunk[offset : offset+len(src[j]) : offset+len(src[j])]
			offset += len(src[j])
		}
	}

	return dst
}

// CompareSlice returns an integer comparing two slices of byte slices
// lexicographically.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func CompareSlice(a, b [][]byte) int {
	i := 0
	for i < len(a) && i < len(b) {
		if v := bytes.Compare(a[i], b[i]); v == 0 {
			i++
			continue
		} else {
			return v
		}
	}

	if i < len(b) {
		// b is longer, so assume a is less
		return -1
	} else if i < len(a) {
		// a is longer, so assume b is less
		return 1
	} else {
		return 0
	}
}
