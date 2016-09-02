package murmur3

// NOTE(benbjohnson): This code is copied from Sébastien Paolacci's
// implementation of murmur3's 32-bit hash. It was copied so that
// the direct murmur3.Hash32 implementation could be used directly
// instead of through the hash.Hash interface.

// http://code.google.com/p/guava-libraries/source/browse/guava/src/com/google/common/hash/Murmur3_32HashFunction.java

import (
	"hash"
	"unsafe"
)

// Make sure interfaces are correctly implemented.
var (
	_ hash.Hash   = new(Hash32)
	_ hash.Hash32 = new(Hash32)
)

const (
	c1_32 uint32 = 0xcc9e2d51
	c2_32 uint32 = 0x1b873593
)

// Hash32 represents a partial evaluation of a 32-bit hash.
type Hash32 struct {
	clen int      // Digested input cumulative length.
	tail []byte   // 0 to Size()-1 bytes view of `buf'.
	buf  [16]byte // Expected (but not required) to be Size() large.
	h1   uint32   // Unfinalized running hash.
}

func (h *Hash32) BlockSize() int { return 1 }

func (h *Hash32) Write(p []byte) (n int, err error) {
	n = len(p)
	h.clen += n

	if len(h.tail) > 0 {
		// Stick back pending bytes.
		nfree := h.Size() - len(h.tail) // nfree ∈ [1, h.Size()-1].
		if nfree < len(p) {
			// One full block can be formed.
			block := append(h.tail, p[:nfree]...)
			p = p[nfree:]
			_ = h.bmix(block) // No tail.
		} else {
			// Tail's buf is large enough to prevent reallocs.
			p = append(h.tail, p...)
		}
	}

	h.tail = h.bmix(p)

	// Keep own copy of the 0 to Size()-1 pending bytes.
	nn := copy(h.buf[:], h.tail)
	h.tail = h.buf[:nn]

	return n, nil
}

func (h *Hash32) Size() int { return 4 }

func (h *Hash32) Reset() {
	h.clen = 0
	h.tail = nil
	h.h1 = 0
}

func (h *Hash32) Sum(b []byte) []byte {
	v := h.Sum32()
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// Digest as many blocks as possible.
func (h *Hash32) bmix(p []byte) (tail []byte) {
	h1 := h.h1

	nblocks := len(p) / 4
	for i := 0; i < nblocks; i++ {
		k1 := *(*uint32)(unsafe.Pointer(&p[i*4]))

		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19) // rotl32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}
	h.h1 = h1
	return p[nblocks*h.Size():]
}

func (h *Hash32) Sum32() (h1 uint32) {

	h1 = h.h1

	var k1 uint32
	switch len(h.tail) & 3 {
	case 3:
		k1 ^= uint32(h.tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(h.tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(h.tail[0])
		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	}

	h1 ^= uint32(h.clen)

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

/*
func rotl32(x uint32, r byte) uint32 {
	return (x << r) | (x >> (32 - r))
}
*/

// Sum32 returns the MurmurHash3 sum of data. It is equivalent to the
// following sequence (without the extra burden and the extra allocation):
//     hasher := New32()
//     hasher.Write(data)
//     return hasher.Sum32()
func Sum32(data []byte) uint32 {

	var h1 uint32 = 0

	nblocks := len(data) / 4
	var p uintptr
	if len(data) > 0 {
		p = uintptr(unsafe.Pointer(&data[0]))
	}
	p1 := p + uintptr(4*nblocks)
	for ; p < p1; p += 4 {
		k1 := *(*uint32)(unsafe.Pointer(p))

		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19) // rotl32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}

	tail := data[nblocks*4:]

	var k1 uint32
	switch len(tail) & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	}

	h1 ^= uint32(len(data))

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}
