package slices

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
