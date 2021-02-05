package radix

// bufferSize is the size of the buffer and the largest slice that can be
// contained in it.
const bufferSize = 4096

// buffer is a type that amoritizes allocations into larger ones, handing out
// small subslices to make copies.
type buffer []byte

// Copy returns a copy of the passed in byte slice allocated using the byte
// slice in the buffer.
func (b *buffer) Copy(x []byte) []byte {
	// if we can never have enough room, just return a copy
	if len(x) > bufferSize {
		out := make([]byte, len(x))
		copy(out, x)
		return out
	}

	// if we don't have enough room, reallocate the buf first
	if len(x) > len(*b) {
		*b = make([]byte, bufferSize)
	}

	// create a copy and hand out a slice
	copy(*b, x)
	out := (*b)[:len(x):len(x)]
	*b = (*b)[len(x):]
	return out
}
