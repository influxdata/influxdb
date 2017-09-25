package bloom

// NOTE:
// This package implements a limited bloom filter implementation based on
// Will Fitzgerald's bloom & bitset packages. It uses a zero-allocation xxhash
// implementation, rather than murmur3. It's implemented locally to support
// zero-copy memory-mapped slices.
//
// This also optimizes the filter by always using a bitset size with a power of 2.

import (
	"fmt"
	"math"

	"github.com/cespare/xxhash"
)

// Filter represents a bloom filter.
type Filter struct {
	k    uint64
	b    []byte
	mask uint64
}

// NewFilter returns a new instance of Filter using m bits and k hash functions.
// If m is not a power of two then it is rounded to the next highest power of 2.
func NewFilter(m uint64, k uint64) *Filter {
	m = pow2(m)
	return &Filter{k: k, b: make([]byte, m>>3), mask: m - 1}
}

// NewFilterBuffer returns a new instance of a filter using a backing buffer.
// The buffer length MUST be a power of 2.
func NewFilterBuffer(buf []byte, k uint64) (*Filter, error) {
	m := pow2(uint64(len(buf)) * 8)
	if m != uint64(len(buf))*8 {
		return nil, fmt.Errorf("bloom.Filter: buffer bit count must a power of two: %d/%d", len(buf)*8, m)
	}
	return &Filter{k: k, b: buf, mask: m - 1}, nil
}

// Len returns the number of bits used in the filter.
func (f *Filter) Len() uint { return uint(len(f.b)) }

// K returns the number of hash functions used in the filter.
func (f *Filter) K() uint64 { return f.k }

// Bytes returns the underlying backing slice.
func (f *Filter) Bytes() []byte { return f.b }

// Clone returns a copy of f.
func (f *Filter) Clone() *Filter {
	other := &Filter{k: f.k, b: make([]byte, len(f.b)), mask: f.mask}
	copy(other.b, f.b)
	return other
}

// Insert inserts data to the filter.
func (f *Filter) Insert(v []byte) {
	h := f.hash(v)
	for i := uint64(0); i < f.k; i++ {
		loc := f.location(h, i)
		f.b[loc>>3] |= 1 << (loc & 7)
	}
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) Contains(v []byte) bool {
	h := f.hash(v)
	for i := uint64(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// Merge performs an in-place union of other into f.
// Returns an error if m or k of the filters differs.
func (f *Filter) Merge(other *Filter) error {
	if other == nil {
		return nil
	}

	// Ensure m & k fields match.
	if len(f.b) != len(other.b) {
		return fmt.Errorf("bloom.Filter.Merge(): m mismatch: %d <> %d", len(f.b), len(other.b))
	} else if f.k != other.k {
		return fmt.Errorf("bloom.Filter.Merge(): k mismatch: %d <> %d", f.b, other.b)
	}

	// Perform union of each byte.
	for i := range f.b {
		f.b[i] |= other.b[i]
	}

	return nil
}

// location returns the ith hashed location using two hash values.
func (f *Filter) location(h [2]uint64, i uint64) uint {
	return uint((h[0] + h[1]*i) & f.mask)
}

// hash returns two 64-bit hashes based on the output of xxhash.
func (f *Filter) hash(data []byte) [2]uint64 {
	v1 := xxhash.Sum64(data)
	var v2 uint64
	if len(data) > 0 {
		b := data[len(data)-1] // We'll put the original byte back.
		data[len(data)-1] = byte(0)
		v2 = xxhash.Sum64(data)
		data[len(data)-1] = b
	}
	return [2]uint64{v1, v2}
}

// Estimate returns an estimated bit count and hash count given the element count and false positive rate.
func Estimate(n uint64, p float64) (m uint64, k uint64) {
	m = uint64(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k = uint64(math.Ceil(math.Log(2) * float64(m) / float64(n)))
	return m, k
}

// pow2 returns the number that is the next highest power of 2.
// Returns v if it is a power of 2.
func pow2(v uint64) uint64 {
	for i := uint64(8); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}
