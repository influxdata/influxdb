package kv

import (
	"bytes"
	"sort"
)

// staticCursor implements the Cursor interface for a slice of
// static key value pairs.
type staticCursor struct {
	idx   int
	pairs []Pair
}

// Pair is a struct for key value pairs.
type Pair struct {
	Key   []byte
	Value []byte
}

// NewStaticCursor returns an instance of a StaticCursor. It
// destructively sorts the provided pairs to be in key ascending order.
func NewStaticCursor(pairs []Pair) Cursor {
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].Key, pairs[j].Key) < 0
	})
	return &staticCursor{
		pairs: pairs,
	}
}

// Seek searches the slice for the first key with the provided prefix.
func (c *staticCursor) Seek(prefix []byte) ([]byte, []byte) {
	// TODO: do binary search for prefix since pairs are ordered.
	for i, pair := range c.pairs {
		if bytes.HasPrefix(pair.Key, prefix) {
			c.idx = i
			return pair.Key, pair.Value
		}
	}

	return nil, nil
}

func (c *staticCursor) getValueAtIndex(delta int) ([]byte, []byte) {
	idx := c.idx + delta
	if idx < 0 {
		return nil, nil
	}

	if idx >= len(c.pairs) {
		return nil, nil
	}

	c.idx = idx

	pair := c.pairs[c.idx]

	return pair.Key, pair.Value
}

// First retrieves the first element in the cursor.
func (c *staticCursor) First() ([]byte, []byte) {
	return c.getValueAtIndex(-c.idx)
}

// Last retrieves the last element in the cursor.
func (c *staticCursor) Last() ([]byte, []byte) {
	return c.getValueAtIndex(len(c.pairs) - 1 - c.idx)
}

// Next retrieves the next entry in the cursor.
func (c *staticCursor) Next() ([]byte, []byte) {
	return c.getValueAtIndex(1)
}

// Prev retrieves the previous entry in the cursor.
func (c *staticCursor) Prev() ([]byte, []byte) {
	return c.getValueAtIndex(-1)
}
