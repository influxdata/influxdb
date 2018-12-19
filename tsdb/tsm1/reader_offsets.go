package tsm1

import (
	"bytes"
	"encoding/binary"
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

// prefix is an 8 byte prefix of a key that sorts the same way the key does.
type prefix [8]byte

// comparePrefix is like bytes.Compare but for a prefix.
func comparePrefix(a, b prefix) int {
	au, bu := binary.BigEndian.Uint64(a[:]), binary.BigEndian.Uint64(b[:])
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
	if len(key) >= 8 {
		return *(*[8]byte)(unsafe.Pointer(&key[0]))
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

// addKey tracks the key in the readerOffsets at the given offset.
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

// readerOffsetsIterator iterates over the keys in readerOffsets.
type readerOffsetsIterator struct {
	r     *readerOffsets
	first bool        // is this the first call to next?
	del   bool        // should the next call delete?
	oi    int         // index into offsets
	od    int         // undeleted index into offsets
	pi    int         // index into prefixes
	psub  int         // number of deleted entries
	ks    rOIKeyState // current key state
}

// rOIKeyState keeps track of cached information for the current key.
type rOIKeyState struct {
	length uint32 // high bit means set
	key    []byte
}

// newROIKeyState constructs a rOIKeyState for the given key, precaching it.
func newROIKeyState(key []byte) (ks rOIKeyState) {
	if key != nil {
		ks.length = uint32(len(key)) | 1<<31
		ks.key = key
	}
	return ks
}

// setIndex sets the reader to the given index and clears any cached state.
func (ri *readerOffsetsIterator) setIndex(i, pi int, key []byte) {
	ri.oi, ri.od, ri.pi, ri.ks = i, i, pi, newROIKeyState(key)
}

// Length returns the length of the current pointed at key.
func (ri *readerOffsetsIterator) Length(b *faultBuffer) uint16 {
	if ri.ks.length&1<<31 == 0 {
		ri.ks.length = uint32(binary.BigEndian.Uint16(b.access(ri.Offset(), 2))) | 1<<31
	}
	return uint16(ri.ks.length)
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

// prefix returns the current pointed at prefix.
func (ri *readerOffsetsIterator) Prefix() prefix { return ri.r.prefixes[ri.pi].pre }

// offset returns the current pointed at offset.
func (ri *readerOffsetsIterator) Offset() uint32 { return ri.r.offsets[ri.oi] }

// Next advances the iterator and returns true if it points at a value.
func (ri *readerOffsetsIterator) Next() bool {
	if ri.oi >= len(ri.r.offsets) {
		return false
	} else if ri.first {
		ri.first = false
		return true
	}

	if ri.del {
		ri.psub++
		ri.del = false
	} else {
		if ri.oi != ri.od {
			ri.r.offsets[ri.od] = ri.r.offsets[ri.oi]
		}
		ri.od++
	}

	ri.oi++
	ri.ks = rOIKeyState{}

	for ri.pi < len(ri.r.prefixes) && ri.oi >= ri.r.prefixes[ri.pi].total {
		ri.r.prefixes[ri.pi].total -= ri.psub
		ri.pi++
	}

	return ri.oi < len(ri.r.offsets)
}

// Done should be called to finalize up any deletes.
func (ri *readerOffsetsIterator) Done() {
	if ri.del { // if flagged to delete, pretend next was called
		ri.psub++
		ri.oi++
	}
	if ri.oi != ri.od {
		for ; ri.pi < len(ri.r.prefixes); ri.pi++ {
			ri.r.prefixes[ri.pi].total -= ri.psub
		}
		copy(ri.r.offsets[ri.od:], ri.r.offsets[ri.oi:])
		ri.r.offsets = ri.r.offsets[:len(ri.r.offsets)-(ri.oi-ri.od)]
	}
}

// Delete flags the entry to be skipped on the next call to Next.
// Should only be called with the write lock.
func (ri *readerOffsetsIterator) Delete() { ri.del = true }

// Seek points the iterator at the smallest key greater than or equal to the
// given key, returning true if it was an exact match. It returns false for
// ok if the key does not exist. Do not call Seek if you have ever called Delete.
func (ri *readerOffsetsIterator) Seek(key []byte, b *faultBuffer) (exact, ok bool) {
	if ri.del || ri.psub > 0 {
		panic("does not support Seek when we just deleted a key")
	}
	ri.first = false

	pre, i, j, pi := keyPrefix(key), 0, len(ri.r.offsets), 0

	for i < j {
		h := int(uint(i+j) >> 1)
		pi = ri.r.searchPrefix(h)
		ri.setIndex(h, pi, nil)

		switch ri.Compare(key, pre, b) {
		case -1:
			i = h + 1
		case 1:
			j = h
		default:
			return true, true
		}
	}

	ri.setIndex(i, pi, nil)
	if ri.oi >= len(ri.r.offsets) {
		return false, false
	}

	for ri.pi < len(ri.r.prefixes) && ri.oi >= ri.r.prefixes[ri.pi].total {
		ri.pi++
	}

	return bytes.Equal(ri.Key(b), key), true
}

// SeekTo sets the iterator to point at the nth element.
func (ri *readerOffsetsIterator) SeekTo(n int) { ri.setIndex(n, ri.r.searchPrefix(n), nil) }

// Compare is like bytes.Compare with the pointed at key, but reduces the amount of
// faults.
func (ri *readerOffsetsIterator) Compare(key []byte, pre prefix, b *faultBuffer) int {
	if cmp := comparePrefix(ri.Prefix(), pre); cmp != 0 {
		return cmp
	}
	return bytes.Compare(ri.Key(b), key)
}
