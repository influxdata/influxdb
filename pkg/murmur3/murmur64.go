package murmur3

// NOTE(edd): This code is adapted from Sébastien Paolacci's
// implementation of murmur3's 64-bit hash. It was adapted so that
// the direct murmur3.Hash64 implementation could be used directly
// instead of through the hash.Hash interface.

import (
	"hash"
)

// Make sure interfaces are correctly implemented.
var (
	_ hash.Hash   = new(Hash64)
	_ hash.Hash64 = new(Hash64)
)

// Hash64 is half a Hash128.
type Hash64 Hash128

// BlockSize returns the hash's underlying block size.
func (h *Hash64) BlockSize() int { return 1 }

// Reset resets the Hash to its initial state.
func (h *Hash64) Reset() {
	(*Hash128)(h).Reset()
}

// Size returns the size of the hash.
func (h *Hash64) Size() int { return 8 }

// Write (via the embedded io.Writer interface) adds more data to the running
// hash.
func (h *Hash64) Write(p []byte) (n int, err error) {
	return (*Hash128)(h).Write(p)
}

// Sum appends the current hash to b and returns the resulting slice.
func (h *Hash64) Sum(b []byte) []byte {
	h1 := h.Sum64()
	return append(b,
		byte(h1>>56), byte(h1>>48), byte(h1>>40), byte(h1>>32),
		byte(h1>>24), byte(h1>>16), byte(h1>>8), byte(h1))
}

// Sum64 returns the 64-bit current hash.
func (h *Hash64) Sum64() uint64 {
	h1, _ := (*Hash128)(h).Sum128()
	return h1
}

// Sum64 returns the MurmurHash3 sum of data. It is equivalent to the
// following sequence (without the extra burden and the extra allocation):
//     hasher := New128()
//     hasher.Write(data)
//     return hasher.Sum64()
func Sum64(data []byte) uint64 {
	h1, _ := Sum128(data)
	return h1
}
