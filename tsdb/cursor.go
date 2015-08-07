package tsdb

import (
	"bytes"
	"container/heap"
)

// MultiCursor returns a single cursor that combines the results of all cursors in order.
//
// If the same key is returned from multiple cursors then the first cursor
// specified will take precendence. A key will only be returned once from the
// returned cursor.
func MultiCursor(cursors ...Cursor) Cursor {
	return &multiCursor{cursors: cursors}
}

// multiCursor represents a cursor that combines multiple cursors into one.
type multiCursor struct {
	cursors []Cursor
	heap    cursorHeap
	prev    []byte
}

// Seek moves the cursor to a given key.
func (mc *multiCursor) Seek(seek []byte) (key, value []byte) {
	// Initialize heap.
	h := make(cursorHeap, 0, len(mc.cursors))
	for i, c := range mc.cursors {
		// Move cursor to position. Skip if it's empty.
		k, v := c.Seek(seek)
		if k == nil {
			continue
		}

		// Append cursor to heap.
		h = append(h, &cursorHeapItem{
			key:      k,
			value:    v,
			cursor:   c,
			priority: len(mc.cursors) - i,
		})
	}

	heap.Init(&h)
	mc.heap = h
	mc.prev = nil

	return mc.pop()
}

// Next returns the next key/value from the cursor.
func (mc *multiCursor) Next() (key, value []byte) { return mc.pop() }

// pop returns the next item from the heap.
// Reads the next key/value from item's cursor and puts it back on the heap.
func (mc *multiCursor) pop() (key, value []byte) {
	// Read items until we have a key that doesn't match the previously read one.
	// This is to perform deduplication when there's multiple items with the same key.
	// The highest priority cursor will be read first and then remaining keys will be dropped.
	for {
		// Return nil if there are no more items left.
		if len(mc.heap) == 0 {
			return nil, nil
		}

		// Read the next item from the heap.
		item := heap.Pop(&mc.heap).(*cursorHeapItem)

		// Save the key/value for return.
		key, value = item.key, item.value

		// Read the next item from the cursor. Push back to heap if one exists.
		if item.key, item.value = item.cursor.Next(); item.key != nil {
			heap.Push(&mc.heap, item)
		}

		// Skip if this key matches the previously returned one.
		if bytes.Equal(mc.prev, key) {
			continue
		}

		mc.prev = key
		return
	}
}

// cursorHeap represents a heap of cursorHeapItems.
type cursorHeap []*cursorHeapItem

func (h cursorHeap) Len() int      { return len(h) }
func (h cursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h cursorHeap) Less(i, j int) bool {
	if cmp := bytes.Compare(h[i].key, h[j].key); cmp == -1 {
		return true
	} else if cmp == 0 {
		return h[i].priority > h[j].priority
	}
	return false
}

func (h *cursorHeap) Push(x interface{}) {
	*h = append(*h, x.(*cursorHeapItem))
}

func (h *cursorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// cursorHeapItem is something we manage in a priority queue.
type cursorHeapItem struct {
	key      []byte
	value    []byte
	cursor   Cursor
	priority int
}
