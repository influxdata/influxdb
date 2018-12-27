package tsm1

import (
	"fmt"
)

// TSMIndexIterator allows one to iterate over the TSM index.
type TSMIndexIterator struct {
	b    *faultBuffer
	n    int
	d    *indirectIndex
	iter *readerOffsetsIterator

	// if true, don't need to advance iter on the call to Next
	first  bool
	peeked bool

	ok  bool
	err error

	offset  uint32
	eoffset uint32

	// lazily loaded from offset and eoffset
	key     []byte
	typ     byte
	entries []IndexEntry
}

// Next advances the iterator and reports if it is still valid.
func (t *TSMIndexIterator) Next() bool {
	t.d.mu.RLock()
	if n := len(t.d.ro.offsets); t.n != n {
		t.err, t.ok = fmt.Errorf("Key count changed during iteration"), false
	}
	if !t.ok || t.err != nil {
		t.d.mu.RUnlock()
		return false
	}
	if !t.peeked && !t.first {
		t.ok = t.iter.Next()
	}
	if !t.ok {
		t.d.mu.RUnlock()
		return false
	}

	t.peeked = false
	t.first = false

	t.offset = t.iter.Offset()
	t.eoffset = t.iter.EntryOffset(t.b)
	t.d.mu.RUnlock()

	// reset lazy loaded state
	t.key = nil
	t.typ = 0
	t.entries = t.entries[:0]
	return true
}

// Peek reports the next key or nil if there is not one or an error happened.
func (t *TSMIndexIterator) Peek() []byte {
	if !t.ok || t.err != nil {
		return nil
	}
	if !t.peeked {
		t.ok = t.iter.Next()
		t.peeked = true
	}

	if !t.ok {
		return nil
	}

	return t.iter.Key(t.b)
}

// Key reports the current key.
func (t *TSMIndexIterator) Key() []byte {
	if t.key == nil {
		buf := t.b.access(t.offset, 0)
		t.key = readKey(buf)
		t.typ = buf[2+len(t.key)]
	}
	return t.key
}

// Type reports the current type.
func (t *TSMIndexIterator) Type() byte {
	if t.key == nil {
		buf := t.b.access(t.offset, 0)
		t.key = readKey(buf)
		t.typ = buf[2+len(t.key)]
	}
	return t.typ
}

// Entries reports the current list of entries.
func (t *TSMIndexIterator) Entries() []IndexEntry {
	if len(t.entries) == 0 {
		buf := t.b.access(t.eoffset, 0)
		t.entries, t.err = readEntries(buf, t.entries)
	}
	if t.err != nil {
		return nil
	}
	return t.entries
}

// Err reports if an error stopped the iteration.
func (t *TSMIndexIterator) Err() error {
	return t.err
}
