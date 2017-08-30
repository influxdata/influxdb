package bloom

// NOTE:
// This package implements a limited bloom filter implementation based on
// Will Fitzgerald's bloom & bitset packages. It's implemented locally to
// support zero-copy memory-mapped slices.
//
// This also optimizes the filter by always using a bitset size with a power of 2.

import (
	"fmt"
	"math"

	"github.com/influxdata/influxdb/pkg/pool"
	"github.com/spaolacci/murmur3"
)

// Filter represents a bloom filter.
type Filter struct {
	k        uint64
	b        []byte
	mask     uint64
	hashPool *pool.Generic
}

// NewFilter returns a new instance of Filter using m bits and k hash functions.
// If m is not a power of two then it is rounded to the next highest power of 2.
func NewFilter(m uint64, k uint64) *Filter {
	m = pow2(m)

	return &Filter{
		k:    k,
		b:    make([]byte, m/8),
		mask: m - 1,
		hashPool: pool.NewGeneric(16, func(sz int) interface{} {
			return murmur3.New128()
		}),
	}
}

// NewFilterBuffer returns a new instance of a filter using a backing buffer.
// The buffer length MUST be a power of 2.
func NewFilterBuffer(buf []byte, k uint64) (*Filter, error) {
	m := pow2(uint64(len(buf)) * 8)
	if m != uint64(len(buf))*8 {
		return nil, fmt.Errorf("bloom.Filter: buffer bit count must a power of two: %d/%d", len(buf)*8, m)
	}

	return &Filter{
		k:    k,
		b:    buf,
		mask: m - 1,
		hashPool: pool.NewGeneric(16, func(sz int) interface{} {
			return murmur3.New128()
		}),
	}, nil
}

// Len returns the number of bits used in the filter.
func (f *Filter) Len() uint { return uint(len(f.b)) }

// K returns the number of hash functions used in the filter.
func (f *Filter) K() uint64 { return f.k }

// Bytes returns the underlying backing slice.
func (f *Filter) Bytes() []byte { return f.b }

// Clone returns a copy of f.
func (f *Filter) Clone() *Filter {
	other := &Filter{k: f.k, b: make([]byte, len(f.b)), mask: f.mask, hashPool: f.hashPool}
	copy(other.b, f.b)
	return other
}

// Insert inserts data to the filter.
func (f *Filter) Insert(v []byte) {
	h := f.hash(v)
	for i := uint64(0); i < f.k; i++ {
		loc := f.location(h, i)
		f.b[loc/8] |= 1 << (loc % 8)
	}
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) Contains(v []byte) bool {
	h := f.hash(v)
	for i := uint64(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc/8]&(1<<(loc%8)) == 0 {
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

// location returns the ith hashed location using the four base hash values.
func (f *Filter) location(h [4]uint64, i uint64) uint {
	return uint((h[i%2] + i*h[2+(((i+(i%2))%4)/2)]) & f.mask)
}

// hash returns a set of 4 based hashes.
func (f *Filter) hash(data []byte) [4]uint64 {
	h := f.hashPool.Get(0).(murmur3.Hash128)
	defer f.hashPool.Put(h)
	h.Reset()
	h.Write(data)
	v1, v2 := h.Sum128()
	h.Write([]byte{1})
	v3, v4 := h.Sum128()
	return [4]uint64{v1, v2, v3, v4}
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
