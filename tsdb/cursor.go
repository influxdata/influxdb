package tsdb

import (
	"container/heap"
	"math"

	"github.com/influxdata/influxdb/influxql"
)

// EOF represents a "not found" key returned by a Cursor.
const EOF = int64(-1)

// Cursor represents an iterator over a series.
type Cursor interface {
	SeekTo(seek int64) (key int64, value interface{})
	Next() (key int64, value interface{})
	Ascending() bool
}

// MultiCursor returns a single cursor that combines the results of all cursors in order.
//
// If the same key is returned from multiple cursors then the first cursor
// specified will take precendence. A key will only be returned once from the
// returned cursor.
func MultiCursor(cursors ...Cursor) Cursor {
	return &multiCursor{
		cursors: cursors,
	}
}

// multiCursor represents a cursor that combines multiple cursors into one.
type multiCursor struct {
	cursors []Cursor
	heap    cursorHeap
	prev    int64 // previously read key
}

// Seek moves the cursor to a given key.
func (mc *multiCursor) SeekTo(seek int64) (int64, interface{}) {
	// Initialize heap.
	h := make(cursorHeap, 0, len(mc.cursors))
	for i, c := range mc.cursors {
		// Move cursor to position. Skip if it's empty.
		k, v := c.SeekTo(seek)
		if k == EOF {
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
	mc.prev = EOF

	return mc.pop()
}

// Ascending returns the direction of the first cursor.
func (mc *multiCursor) Ascending() bool {
	if len(mc.cursors) == 0 {
		return true
	}
	return mc.cursors[0].Ascending()
}

// Next returns the next key/value from the cursor.
func (mc *multiCursor) Next() (int64, interface{}) { return mc.pop() }

// pop returns the next item from the heap.
// Reads the next key/value from item's cursor and puts it back on the heap.
func (mc *multiCursor) pop() (key int64, value interface{}) {
	// Read items until we have a key that doesn't match the previously read one.
	// This is to perform deduplication when there's multiple items with the same key.
	// The highest priority cursor will be read first and then remaining keys will be dropped.
	for {
		// Return EOF marker if there are no more items left.
		if len(mc.heap) == 0 {
			return EOF, nil
		}

		// Read the next item from the heap.
		item := heap.Pop(&mc.heap).(*cursorHeapItem)

		// Save the key/value for return.
		key, value = item.key, item.value

		// Read the next item from the cursor. Push back to heap if one exists.
		if item.key, item.value = item.cursor.Next(); item.key != EOF {
			heap.Push(&mc.heap, item)
		}

		// Skip if this key matches the previously returned one.
		if key == mc.prev {
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
	// Use priority if the keys are the same.
	if h[i].key == h[j].key {
		return h[i].priority > h[j].priority
	}

	// Otherwise compare based on cursor direction.
	if h[i].cursor.Ascending() {
		return h[i].key < h[j].key
	}
	return h[i].key > h[j].key
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
	key      int64
	value    interface{}
	cursor   Cursor
	priority int
}

// bufCursor represents a buffered cursor that is initialized at a time.
// This cursor does not allow seeking after initial seek.
type bufCursor struct {
	cur Cursor
	buf *struct {
		key   int64
		value interface{}
	}
}

// newBufCursor returns a new instance of bufCursor that wraps cur.
func newBufCursor(cur Cursor, seek int64) *bufCursor {
	c := &bufCursor{cur: cur}

	// Limit min seek to zero.
	if seek < 0 {
		seek = 0
	}

	// Fill buffer, if seekable.
	k, v := cur.SeekTo(seek)
	if k != EOF {
		c.buf = &struct {
			key   int64
			value interface{}
		}{k, v}
	}

	return c
}

// SeekTo panics if called. Cursor can only be seeked on initialization.
func (c *bufCursor) SeekTo(seek int64) (key int64, value interface{}) { panic("unseekable") }

// Next returns the next key & value from the underlying cursor.
func (c *bufCursor) Next() (key int64, value interface{}) {
	if c.buf != nil {
		key, value = c.buf.key, c.buf.value
		c.buf = nil
		return
	}
	return c.cur.Next()
}

// Ascending returns true if the cursor traverses in ascending order.
func (c *bufCursor) Ascending() bool { return c.cur.Ascending() }

// FloatCursorIterator represents a wrapper for Cursor to produce an influxql.FloatIterator.
type FloatCursorIterator struct {
	cursor *bufCursor
	opt    influxql.IteratorOptions
	ref    *influxql.VarRef
	tags   influxql.Tags
	point  influxql.FloatPoint // reuseable point to emit
}

// NewFloatCursorIterator returns a new instance of FloatCursorIterator.
func NewFloatCursorIterator(name string, tagMap map[string]string, cur Cursor, opt influxql.IteratorOptions) *FloatCursorIterator {
	// Extract variable reference if available.
	var ref *influxql.VarRef
	if opt.Expr != nil {
		ref = opt.Expr.(*influxql.VarRef)
	}

	// Only allocate aux values if we have any requested.
	var aux []interface{}
	if len(opt.Aux) > 0 {
		aux = make([]interface{}, len(opt.Aux))
	}

	// Convert to influxql tags.
	tags := influxql.NewTags(tagMap)

	// Determine initial seek position based on sort direction.
	seek := opt.StartTime
	if !opt.Ascending {
		seek = opt.EndTime
	}

	return &FloatCursorIterator{
		point: influxql.FloatPoint{
			Name: name,
			Tags: tags.Subset(opt.Dimensions),
			Aux:  aux,
		},
		opt:    opt,
		ref:    ref,
		tags:   tags,
		cursor: newBufCursor(cur, seek),
	}
}

// Close closes the iterator.
func (itr *FloatCursorIterator) Close() error { return nil }

// Next returns the next point from the cursor.
func (itr *FloatCursorIterator) Next() *influxql.FloatPoint {
	for {
		// Read next key/value and emit nil if at the end.
		timestamp, value := itr.cursor.Next()
		if timestamp == EOF {
			return nil
		} else if itr.opt.Ascending && timestamp > itr.opt.EndTime {
			return nil
		} else if !itr.opt.Ascending && timestamp < itr.opt.StartTime {
			return nil
		}

		// Set timestamp on point.
		itr.point.Time = timestamp

		// Retrieve tags key/value map.
		tags := itr.tags.KeyValues()

		// If value is a map then extract all the fields.
		if m, ok := value.(map[string]interface{}); ok {
			// If filter fails then skip to the next value.
			if itr.opt.Condition != nil && !influxql.EvalBool(itr.opt.Condition, m) {
				continue
			}

			if itr.ref != nil {
				fv, ok := m[itr.ref.Val].(float64)
				if !ok {
					continue // read next point
				}
				itr.point.Value = fv
			} else {
				itr.point.Value = math.NaN()
			}

			// Read all auxilary fields.
			for i, name := range itr.opt.Aux {
				if v, ok := m[name]; ok {
					itr.point.Aux[i] = v
				} else if s, ok := tags[name]; ok {
					itr.point.Aux[i] = s
				} else {
					itr.point.Aux[i] = nil
				}
			}

			return &itr.point
		}

		// Otherwise expect value to be of an appropriate type.
		if itr.ref != nil {
			// If filter fails then skip to the next value.
			if itr.opt.Condition != nil && !influxql.EvalBool(itr.opt.Condition, map[string]interface{}{itr.ref.Val: value}) {
				continue
			}

			fv, ok := value.(float64)
			if !ok {
				continue // read next point
			}
			itr.point.Value = fv
		} else {
			itr.point.Value = math.NaN()
		}

		// Read all auxilary fields.
		for i, name := range itr.opt.Aux {
			if tagValue, ok := tags[name]; ok {
				itr.point.Aux[i] = tagValue
			} else {
				itr.point.Aux[i] = value
			}
		}

		return &itr.point
	}
}
