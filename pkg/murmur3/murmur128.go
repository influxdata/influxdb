package murmur3

// NOTE(edd): This code is adapted from Sébastien Paolacci's
// implementation of murmur3's 128-bit hash. It was adapted so that
// the direct murmur3.Hash128 implementation could be used directly
// instead of through the hash.Hash interface.

import (
	"hash"
	"unsafe"
)

const (
	c1_128 = 0x87c37b91114253d5
	c2_128 = 0x4cf5ad432745937f
)

// Make sure interfaces are correctly implemented.
var (
	_ hash.Hash = new(Hash128)
)

// Hash128 represents a partial evaluation of a 128-bit hash.
//
// Hash128 should also be used when 64-bit hashes are required. In such a case,
// the first part of the running hash may be used.
type Hash128 struct {
	clen int      // Digested input cumulative length.
	tail []byte   // 0 to Size()-1 bytes view of `buf'.
	buf  [16]byte // Expected (but not required) to be Size() large.

	h1 uint64 // Unfinalized running hash part 1.
	h2 uint64 // Unfinalized running hash part 2.
}

// BlockSize returns the hash's underlying block size.
func (h *Hash128) BlockSize() int { return 1 }

// Write (via the embedded io.Writer interface) adds more data to the running
// hash.
func (h *Hash128) Write(p []byte) (n int, err error) {
	n = len(p)
	h.clen += n

	if len(h.tail) > 0 {
		// Stick back pending bytes.
		nfree := h.Size() - len(h.tail) // nfree ∈ [1, h.Size()-1].
		if nfree < len(p) {
			// One full block can be formeh.
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

// Reset resets the Hash to its initial state.
func (h *Hash128) Reset() {
	h.clen = 0
	h.tail = nil
	h.h1, h.h2 = 0, 0
}

// Size returns the size of the hash.
func (h *Hash128) Size() int { return 16 }

// Sum appends the current hash to b and returns the resulting slice.
func (h *Hash128) Sum(b []byte) []byte {
	h1, h2 := h.Sum128()
	return append(b,
		byte(h1>>56), byte(h1>>48), byte(h1>>40), byte(h1>>32),
		byte(h1>>24), byte(h1>>16), byte(h1>>8), byte(h1),

		byte(h2>>56), byte(h2>>48), byte(h2>>40), byte(h2>>32),
		byte(h2>>24), byte(h2>>16), byte(h2>>8), byte(h2),
	)
}

func (h *Hash128) bmix(p []byte) (tail []byte) {
	h1, h2 := h.h1, h.h2

	nblocks := len(p) / 16
	for i := 0; i < nblocks; i++ {
		t := (*[2]uint64)(unsafe.Pointer(&p[i*16]))
		k1, k2 := t[0], t[1]

		k1 *= c1_128
		k1 = (k1 << 31) | (k1 >> 33) // rotl64(k1, 31)
		k1 *= c2_128
		h1 ^= k1

		h1 = (h1 << 27) | (h1 >> 37) // rotl64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2_128
		k2 = (k2 << 33) | (k2 >> 31) // rotl64(k2, 33)
		k2 *= c1_128
		h2 ^= k2

		h2 = (h2 << 31) | (h2 >> 33) // rotl64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}
	h.h1, h.h2 = h1, h2
	return p[nblocks*h.Size():]
}

// Sum128 returns the 128-bit current hash.
func (h *Hash128) Sum128() (h1, h2 uint64) {

	h1, h2 = h.h1, h.h2

	var k1, k2 uint64
	switch len(h.tail) & 15 {
	case 15:
		k2 ^= uint64(h.tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= uint64(h.tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= uint64(h.tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= uint64(h.tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= uint64(h.tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= uint64(h.tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= uint64(h.tail[8]) << 0

		k2 *= c2_128
		k2 = (k2 << 33) | (k2 >> 31) // rotl64(k2, 33)
		k2 *= c1_128
		h2 ^= k2

		fallthrough

	case 8:
		k1 ^= uint64(h.tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= uint64(h.tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= uint64(h.tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= uint64(h.tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= uint64(h.tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= uint64(h.tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint64(h.tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint64(h.tail[0]) << 0
		k1 *= c1_128
		k1 = (k1 << 31) | (k1 >> 33) // rotl64(k1, 31)
		k1 *= c2_128
		h1 ^= k1
	}

	h1 ^= uint64(h.clen)
	h2 ^= uint64(h.clen)

	h1 += h2
	h2 += h1

	h1 = fmix64(h1)
	h2 = fmix64(h2)

	h1 += h2
	h2 += h1

	return h1, h2
}

func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}

/*
func rotl64(x uint64, r byte) uint64 {
	return (x << r) | (x >> (64 - r))
}
*/

// Sum128 returns the MurmurHash3 sum of data. It is equivalent to the
// following sequence (without the extra burden and the extra allocation):
//     hasher := New128()
//     hasher.Write(data)
//     return hasher.Sum128()
func Sum128(data []byte) (h1 uint64, h2 uint64) {
	h := new(Hash128)
	h.tail = h.bmix(data)
	h.clen = len(data)
	return h.Sum128()
}
