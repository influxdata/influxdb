package tsm1

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"unsafe"
)

// readerOffsets keeps track of offsets of keys for an indirectIndex.
type readerOffsets struct {
	offsets  []uint32
	prefixes []prefixEntry
	entry    prefixEntry
}

// prefixEntry keeps a prefix along with a prefix sum of the total number of
// keys with the given prefix.
type prefixEntry struct {
	pre   prefix
	total int // partial sums
}

// prefix is a byte prefix of a key that sorts the same way the key does.
type prefix [8]byte

const prefixSize = len(prefix{})

// comparePrefix is like bytes.Compare but for a prefix.
func comparePrefix(a, b prefix) int {
	au, bu := binary.BigEndian.Uint64(a[:8]), binary.BigEndian.Uint64(b[:8])
	if au == bu {
		return 0
	} else if au < bu {
		return -1
	}
	return 1
}

// keyPrefix returns a prefix that can be used with compare
// to sort the same way the bytes would.
func keyPrefix(key []byte) (pre prefix) {
	if len(key) >= prefixSize {
		return *(*prefix)(unsafe.Pointer(&key[0]))
	}
	copy(pre[:], key)
	return pre
}

// searchPrefix returns the index of the prefixEntry for the nth offset.
func (r *readerOffsets) searchPrefix(n int) int {
	i, j := 0, len(r.prefixes)
	for i < j {
		h := int(uint(i+j) >> 1)
		if n >= r.prefixes[h].total {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

// AddKey tracks the key in the readerOffsets at the given offset.
func (r *readerOffsets) AddKey(offset uint32, key []byte) {
	r.offsets = append(r.offsets, offset)
	pre := keyPrefix(key)
	if r.entry.pre != pre && r.entry.total != 0 {
		r.prefixes = append(r.prefixes, r.entry)
	}
	r.entry.pre = pre
	r.entry.total++
}

// done signals that we are done adding keys.
func (r *readerOffsets) Done() {
	r.prefixes = append(r.prefixes, r.entry)
}

// Iterator returns an iterator that can walk and seek around the keys cheaply.
func (r *readerOffsets) Iterator() readerOffsetsIterator {
	return readerOffsetsIterator{r: r, first: true}
}

//
// iterator stuff
//

// readerOffsetsIterator iterates over the keys in readerOffsets.
type readerOffsetsIterator struct {
	r     *readerOffsets
	first bool        // is this the first call to next?
	del   bool        // has delete been called?
	i     int         // index into offsets
	pi    int         // index into prefixes
	ks    rOIKeyState // current key state
}

// rOIKeyState keeps track of cached information for the current key.
type rOIKeyState struct {
	length uint16
	key    []byte
}

// Index returns the current pointed at index.
func (ri *readerOffsetsIterator) Index() int { return ri.i }

// setIndex sets the reader to the given index and clears any cached state.
func (ri *readerOffsetsIterator) setIndex(i, pi int) {
	ri.i, ri.pi, ri.ks = i, pi, rOIKeyState{}
}

// Length returns the length of the current pointed at key.
func (ri *readerOffsetsIterator) Length(b *faultBuffer) uint16 {
	if ri.ks.length == 0 {
		buf := b.access(ri.Offset(), 2)
		ri.ks.length = uint16(buf[0])<<8 | uint16(buf[1])
	}
	return ri.ks.length
}

// Key returns the current pointed at key.
func (ri *readerOffsetsIterator) Key(b *faultBuffer) []byte {
	if ri.ks.key == nil {
		ri.ks.key = b.access(ri.KeyOffset(), uint32(ri.Length(b)))
	}
	return ri.ks.key
}

// KeyOffset returns the offset of the current pointed at the key.
func (ri *readerOffsetsIterator) KeyOffset() uint32 {
	return ri.Offset() + 2
}

// EntryOffset returns the offset of the current pointed at entries (including type byte).
func (ri *readerOffsetsIterator) EntryOffset(b *faultBuffer) uint32 {
	return ri.Offset() + 2 + uint32(ri.Length(b))
}

// Prefix returns the current pointed at prefix.
func (ri *readerOffsetsIterator) Prefix() prefix {
	return ri.r.prefixes[ri.pi].pre
}

// Offset returns the current pointed at offset.
func (ri *readerOffsetsIterator) Offset() uint32 {
	return atomic.LoadUint32(&ri.r.offsets[ri.i]) &^ (1 << 31)
}

// Next advances the iterator and returns true if it points at a value.
func (ri *readerOffsetsIterator) Next() bool {
	if ri.i >= len(ri.r.offsets) {
		return false
	} else if ri.first {
		ri.first = false
		return true
	}

	ri.i++
	ri.ks = rOIKeyState{}

	for ri.pi < len(ri.r.prefixes) && ri.i >= ri.r.prefixes[ri.pi].total {
		ri.pi++
	}

	return ri.i < len(ri.r.offsets)
}

// Done should be called to finalize up any deletes. Must be called under a write lock.
func (ri *readerOffsetsIterator) Done() {
	if !ri.del {
		return
	}
	ri.del = false

	j, psub, pi := 0, 0, 0
	for i, v := range ri.r.offsets {
		for pi < len(ri.r.prefixes) && i >= ri.r.prefixes[pi].total {
			ri.r.prefixes[pi].total -= psub
			pi++
		}

		if v&(1<<31) > 0 {
			psub++
			continue
		}

		if i != j {
			ri.r.offsets[j] = ri.r.offsets[i]
		}
		j++
	}

	ri.r.offsets = ri.r.offsets[:j]
}

// Delete flags the entry to be deleted on the next call to Done. Is safe for
// concurrent use under a read lock, but Done must be called under a write lock.
func (ri *readerOffsetsIterator) Delete() {
	ri.del = true
	if offset := ri.Offset(); offset&(1<<31) == 0 {
		atomic.StoreUint32(&ri.r.offsets[ri.i], offset|(1<<31))
	}
}

// HasDeletes returns true if the iterator has any Delete calls.
func (ri *readerOffsetsIterator) HasDeletes() bool { return ri.del }

// Seek points the iterator at the smallest key greater than or equal to the
// given key, returning true if it was an exact match. It returns false for
// ok if the key does not exist.
func (ri *readerOffsetsIterator) Seek(key []byte, b *faultBuffer) (exact, ok bool) {
	ri.first = false

	pre, i, j, pi := keyPrefix(key), 0, len(ri.r.offsets), 0

	for i < j {
		h := int(uint(i+j) >> 1)
		pi = ri.r.searchPrefix(h)
		ri.setIndex(h, pi)

		switch ri.Compare(key, pre, b) {
		case -1:
			i = h + 1
		case 1:
			j = h
		default:
			return true, true
		}
	}

	ri.setIndex(i, pi)
	if ri.i >= len(ri.r.offsets) {
		return false, false
	}

	for ri.pi < len(ri.r.prefixes) && ri.i >= ri.r.prefixes[ri.pi].total {
		ri.pi++
	}

	return bytes.Equal(ri.Key(b), key), true
}

// SetIndex sets the iterator to point at the nth element.
func (ri *readerOffsetsIterator) SetIndex(n int) {
	ri.setIndex(n, ri.r.searchPrefix(n))
}

// Compare is like bytes.Compare with the pointed at key, but reduces the amount of faults.
func (ri *readerOffsetsIterator) Compare(key []byte, pre prefix, b *faultBuffer) int {
	if cmp := comparePrefix(ri.Prefix(), pre); cmp != 0 {
		return cmp
	}
	return bytes.Compare(ri.Key(b), key)
}
