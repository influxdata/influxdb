package tsm1

import (
	"bytes"
	"container/heap"
)

type keyIterator struct {
	f   TSMFile
	c   int // current key index
	n   int // key count
	key []byte
	typ byte
}

func newKeyIterator(f TSMFile, seek []byte) *keyIterator {
	c, n := 0, f.KeyCount()
	if len(seek) > 0 {
		c = f.Seek(seek)
	}

	if c >= n {
		return nil
	}

	k := &keyIterator{f: f, c: c, n: n}
	k.next()

	return k
}

func (k *keyIterator) next() bool {
	if k.c < k.n {
		k.key, k.typ = k.f.KeyAt(k.c)
		k.c++
		return true
	}
	return false
}

type mergeKeyIterator struct {
	itrs keyIterators
	key  []byte
	typ  byte
}

func newMergeKeyIterator(files []TSMFile, seek []byte) *mergeKeyIterator {
	m := &mergeKeyIterator{}
	itrs := make(keyIterators, 0, len(files))
	for _, f := range files {
		if ki := newKeyIterator(f, seek); ki != nil {
			itrs = append(itrs, ki)
		}
	}
	m.itrs = itrs
	heap.Init(&m.itrs)

	return m
}

func (m *mergeKeyIterator) Next() bool {
	merging := len(m.itrs) > 1

RETRY:
	if len(m.itrs) == 0 {
		return false
	}

	key, typ := m.itrs[0].key, m.itrs[0].typ
	more := m.itrs[0].next()

	switch {
	case len(m.itrs) > 1:
		if !more {
			// remove iterator from heap
			heap.Pop(&m.itrs)
		} else {
			heap.Fix(&m.itrs, 0)
		}

	case len(m.itrs) == 1:
		if !more {
			m.itrs = nil
		}
	}

	if merging && bytes.Equal(m.key, key) {
		// same as previous key, keep iterating
		goto RETRY
	}

	m.key, m.typ = key, typ

	return true
}

func (m *mergeKeyIterator) Read() ([]byte, byte) { return m.key, m.typ }

type keyIterators []*keyIterator

func (k keyIterators) Len() int            { return len(k) }
func (k keyIterators) Less(i, j int) bool  { return bytes.Compare(k[i].key, k[j].key) == -1 }
func (k keyIterators) Swap(i, j int)       { k[i], k[j] = k[j], k[i] }
func (k *keyIterators) Push(x interface{}) { *k = append(*k, x.(*keyIterator)) }

func (k *keyIterators) Pop() interface{} {
	old := *k
	n := len(old)
	x := old[n-1]
	*k = old[:n-1]
	return x
}
