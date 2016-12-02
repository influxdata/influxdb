// Package hll contains a HyperLogLog++ implementation that is adapted (mostly
// copied) from an implementation provided by Clark DuVall
// github.com/clarkduvall/hyperloglog.
//
// The differences are that the implementation in this package:
//
//   * uses an AMD64 optimised xxhash algorithm instead of murmur;
//   * uses some AMD64 optimisations for things like clz;
//   * works with []byte rather than a Hash64 interface, to reduce allocations;
//   * implements encoding.BinaryMarshaler and encoding.BinaryUnmarshaler
//
// Based on some rough benchmarking, this implementation of HyperLogLog++ is
// around twice as fast as the github.com/clarkduvall/hyperloglog implementation.
package hll

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/cespare/xxhash"
	"github.com/dgryski/go-bits"
	"github.com/influxdata/influxdb/pkg/estimator"
)

// Current version of HLL implementation.
const version uint8 = 1

// Plus implements the Hyperloglog++ algorithm, described in the following
// paper: http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
//
// The HyperLogLog++ algorithm provides cardinality estimations.
type Plus struct {
	// hash function used to hash values to add to the sketch.
	hash func([]byte) uint64

	p  uint8 // precision.
	pp uint8 // p' (sparse) precision to be used when p ∈ [4..pp] and pp < 64.

	m  uint32 // Number of substream used for stochastic averaging of stream.
	mp uint32 // m' (sparse) number of substreams.

	alpha float64 // alpha is used for bias correction.

	sparse bool // Should we use a sparse sketch representation.
	tmpSet set

	denseList  []uint8         // The dense representation of the HLL.
	sparseList *compressedList // values that can be stored in the sparse represenation.
}

// NewPlus returns a new Plus with precision p. p must be between 4 and 18.
func NewPlus(p uint8) (*Plus, error) {
	if p > 18 || p < 4 {
		return nil, errors.New("precision must be between 4 and 18")
	}

	// p' = 25 is used in the Google paper.
	pp := uint8(25)

	hll := &Plus{
		hash:   xxhash.Sum64,
		p:      p,
		pp:     pp,
		m:      1 << p,
		mp:     1 << pp,
		tmpSet: set{},
		sparse: true,
	}
	hll.sparseList = newCompressedList(int(hll.m))

	// Determine alpha.
	switch hll.m {
	case 16:
		hll.alpha = 0.673
	case 32:
		hll.alpha = 0.697
	case 64:
		hll.alpha = 0.709
	default:
		hll.alpha = 0.7213 / (1 + 1.079/float64(hll.m))
	}

	return hll, nil
}

// NewDefaultPlus creates a new Plus with the default precision.
func NewDefaultPlus() *Plus {
	p, err := NewPlus(DefaultPrecision)
	if err != nil {
		panic(err)
	}
	return p
}

// MustNewPlus returns a new Plus with precision p. Panic on error.
func MustNewPlus(p uint8) *Plus {
	hll, err := NewPlus(p)
	if err != nil {
		panic(err)
	}
	return hll
}

// Add adds a new value to the HLL.
func (h *Plus) Add(v []byte) {
	x := h.hash(v)
	if h.sparse {
		h.tmpSet.add(h.encodeHash(x))

		if uint32(len(h.tmpSet))*100 > h.m {
			h.mergeSparse()
			if uint32(h.sparseList.Len()) > h.m {
				h.toNormal()
			}
		}
	} else {
		i := bextr(x, 64-h.p, h.p) // {x63,...,x64-p}
		w := x<<h.p | 1<<(h.p-1)   // {x63-p,...,x0}

		rho := uint8(bits.Clz(w)) + 1
		if rho > h.denseList[i] {
			h.denseList[i] = rho
		}
	}
}

// Count returns a cardinality estimate.
func (h *Plus) Count() uint64 {
	if h.sparse {
		h.mergeSparse()
		return uint64(h.linearCount(h.mp, h.mp-uint32(h.sparseList.count)))
	}

	est, zeros := h.e()
	if est <= 5.0*float64(h.m) {
		est -= h.estimateBias(est)
	}

	if zeros > 0 {
		lc := h.linearCount(h.m, zeros)
		if lc <= threshold[h.p-4] {
			return uint64(lc)
		}
	}
	return uint64(est)
}

// Merge takes another HyperLogLogPlus and combines it with HyperLogLogPlus h.
// If HyperLogLogPlus h is using the sparse representation, it will be converted
// to the normal representation.
func (h *Plus) Merge(s estimator.Sketch) error {
	other, ok := s.(*Plus)
	if !ok {
		return fmt.Errorf("wrong type for merging: %T", other)
	}

	if h.p != other.p {
		return errors.New("precisions must be equal")
	}

	if h.sparse {
		h.toNormal()
	}

	if other.sparse {
		for k := range other.tmpSet {
			i, r := other.decodeHash(k)
			if h.denseList[i] < r {
				h.denseList[i] = r
			}
		}

		for iter := other.sparseList.Iter(); iter.HasNext(); {
			i, r := other.decodeHash(iter.Next())
			if h.denseList[i] < r {
				h.denseList[i] = r
			}
		}
	} else {
		for i, v := range other.denseList {
			if v > h.denseList[i] {
				h.denseList[i] = v
			}
		}
	}
	return nil
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (h *Plus) MarshalBinary() (data []byte, err error) {
	// Marshal a version marker.
	data = append(data, version)

	// Marshal precision.
	data = append(data, byte(h.p))

	if h.sparse {
		// It's using the sparse representation.
		data = append(data, byte(1))

		// Add the tmp_set
		tsdata, err := h.tmpSet.MarshalBinary()
		if err != nil {
			return nil, err
		}
		data = append(data, tsdata...)

		// Add the sparse representation
		sdata, err := h.sparseList.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return append(data, sdata...), nil
	}

	// It's using the dense representation.
	data = append(data, byte(0))

	// Add the dense sketch representation.
	sz := len(h.denseList)
	data = append(data, []byte{
		byte(sz >> 24),
		byte(sz >> 16),
		byte(sz >> 8),
		byte(sz),
	}...)

	// Marshal each element in the list.
	for i := 0; i < len(h.denseList); i++ {
		data = append(data, byte(h.denseList[i]))
	}

	return data, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (h *Plus) UnmarshalBinary(data []byte) error {
	// Unmarshal version. We may need this in the future if we make
	// non-compatible changes.
	_ = data[0]

	// Unmarshal precision.
	p := uint8(data[1])

	newh, err := NewPlus(p)
	if err != nil {
		return err
	}
	*h = *newh

	// h is now initialised with the correct precision. We just need to fill the
	// rest of the details out.
	if data[2] == byte(1) {
		// Using the sparse representation.
		h.sparse = true

		// Unmarshal the tmp_set.
		tssz := binary.BigEndian.Uint32(data[3:7])
		h.tmpSet = make(map[uint32]struct{}, tssz)

		// We need to unmarshal tssz values in total, and each value requires us
		// to read 4 bytes.
		tsLastByte := int((tssz * 4) + 7)
		for i := 7; i < tsLastByte; i += 4 {
			k := binary.BigEndian.Uint32(data[i : i+4])
			h.tmpSet[k] = struct{}{}
		}

		// Unmarshal the sparse representation.
		return h.sparseList.UnmarshalBinary(data[tsLastByte:])
	}

	// Using the dense representation.
	h.sparse = false
	dsz := int(binary.BigEndian.Uint32(data[3:7]))
	h.denseList = make([]uint8, 0, dsz)
	for i := 7; i < dsz+7; i++ {
		h.denseList = append(h.denseList, uint8(data[i]))
	}
	return nil
}

func (h *Plus) mergeSparse() {
	keys := make(uint64Slice, 0, len(h.tmpSet))
	for k := range h.tmpSet {
		keys = append(keys, k)
	}
	sort.Sort(keys)

	newList := newCompressedList(int(h.m))
	for iter, i := h.sparseList.Iter(), 0; iter.HasNext() || i < len(keys); {
		if !iter.HasNext() {
			newList.Append(keys[i])
			i++
			continue
		}

		if i >= len(keys) {
			newList.Append(iter.Next())
			continue
		}

		x1, x2 := iter.Peek(), keys[i]
		if x1 == x2 {
			newList.Append(iter.Next())
			i++
		} else if x1 > x2 {
			newList.Append(x2)
			i++
		} else {
			newList.Append(iter.Next())
		}
	}

	h.sparseList = newList
	h.tmpSet = set{}
}

// Convert from sparse representation to dense representation.
func (h *Plus) toNormal() {
	if len(h.tmpSet) > 0 {
		h.mergeSparse()
	}

	h.denseList = make([]uint8, h.m)
	for iter := h.sparseList.Iter(); iter.HasNext(); {
		i, r := h.decodeHash(iter.Next())
		if h.denseList[i] < r {
			h.denseList[i] = r
		}
	}

	h.sparse = false
	h.tmpSet = nil
	h.sparseList = nil
}

// Encode a hash to be used in the sparse representation.
func (h *Plus) encodeHash(x uint64) uint32 {
	idx := uint32(bextr(x, 64-h.pp, h.pp))
	if bextr(x, 64-h.pp, h.pp-h.p) == 0 {
		zeros := bits.Clz((bextr(x, 0, 64-h.pp)<<h.pp)|(1<<h.pp-1)) + 1
		return idx<<7 | uint32(zeros<<1) | 1
	}
	return idx << 1
}

// Decode a hash from the sparse representation.
func (h *Plus) decodeHash(k uint32) (uint32, uint8) {
	var r uint8
	if k&1 == 1 {
		r = uint8(bextr32(k, 1, 6)) + h.pp - h.p
	} else {
		// We can use the 64bit clz implementation and reduce the result
		// by 32 to get a clz for a 32bit word.
		r = uint8(bits.Clz(uint64(k<<(32-h.pp+h.p-1))) - 31) // -32 + 1
	}
	return h.getIndex(k), r
}

func (h *Plus) getIndex(k uint32) uint32 {
	if k&1 == 1 {
		return bextr32(k, 32-h.p, h.p)
	}
	return bextr32(k, h.pp-h.p+1, h.p)
}

func (h *Plus) linearCount(m uint32, v uint32) float64 {
	fm := float64(m)
	return fm * math.Log(fm/float64(v))
}

// E calculates the raw estimate. It also returns the number of zero registers
// which is useful for later on in a cardinality estimate.
func (h *Plus) e() (float64, uint32) {
	sum := 0.0
	var count uint32
	for _, val := range h.denseList {
		sum += 1.0 / float64(uint32(1)<<val)
		if val == 0 {
			count++
		}
	}
	return h.alpha * float64(h.m) * float64(h.m) / sum, count
}

// Estimates the bias using empirically determined values.
func (h *Plus) estimateBias(est float64) float64 {
	estTable, biasTable := rawEstimateData[h.p-4], biasData[h.p-4]

	if estTable[0] > est {
		return estTable[0] - biasTable[0]
	}

	lastEstimate := estTable[len(estTable)-1]
	if lastEstimate < est {
		return lastEstimate - biasTable[len(biasTable)-1]
	}

	var i int
	for i = 0; i < len(estTable) && estTable[i] < est; i++ {
	}

	e1, b1 := estTable[i-1], biasTable[i-1]
	e2, b2 := estTable[i], biasTable[i]

	c := (est - e1) / (e2 - e1)
	return b1*(1-c) + b2*c
}

type uint64Slice []uint32

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type set map[uint32]struct{}

func (s set) MarshalBinary() (data []byte, err error) {
	// 4 bytes for the size of the set, and 4 bytes for each key.
	// list.
	data = make([]byte, 0, 4+(4*len(s)))

	// Length of the set. We only need 32 bits because the size of the set
	// couldn't exceed that on 32 bit architectures.
	sl := len(s)
	data = append(data, []byte{
		byte(sl >> 24),
		byte(sl >> 16),
		byte(sl >> 8),
		byte(sl),
	}...)

	// Marshal each element in the set.
	for k := range s {
		data = append(data, []byte{
			byte(k >> 24),
			byte(k >> 16),
			byte(k >> 8),
			byte(k),
		}...)
	}

	return data, nil
}

func (s set) add(v uint32)      { s[v] = struct{}{} }
func (s set) has(v uint32) bool { _, ok := s[v]; return ok }

// bextr performs a bitfield extract on v. start should be the LSB of the field
// you wish to extract, and length the number of bits to extract.
//
// For example: start=0 and length=4 for the following 64-bit word would result
// in 1111 being returned.
//
// <snip 56 bits>00011110
// returns 1110
func bextr(v uint64, start, length uint8) uint64 {
	return (v >> start) & ((1 << length) - 1)
}

func bextr32(v uint32, start, length uint8) uint32 {
	return (v >> start) & ((1 << length) - 1)
}
