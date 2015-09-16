package tsdb

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"sort"
	"strings"

	"github.com/influxdb/influxdb/influxql"
)

// Direction represents a cursor navigation direction.
type Direction bool

const (
	// Forward indicates that a cursor will move forward over its values.
	Forward Direction = true
	// Reverse indicates that a cursor will move backwards over its values.
	Reverse Direction = false
)

func (d Direction) String() string {
	if d.Forward() {
		return "forward"
	}
	return "reverse"
}

// Forward returns true if direction is forward
func (d Direction) Forward() bool {
	return d == Forward
}

// Forward returns true if direction is reverse
func (d Direction) Reverse() bool {
	return d == Reverse
}

// MultiCursor returns a single cursor that combines the results of all cursors in order.
//
// If the same key is returned from multiple cursors then the first cursor
// specified will take precendence. A key will only be returned once from the
// returned cursor.
func MultiCursor(d Direction, cursors ...Cursor) Cursor {
	return &multiCursor{cursors: cursors, direction: d}
}

// multiCursor represents a cursor that combines multiple cursors into one.
type multiCursor struct {
	cursors   []Cursor
	heap      cursorHeap
	prev      []byte
	direction Direction
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

func (mc *multiCursor) Direction() Direction { return mc.direction }

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
	dir := -1
	if !h[i].cursor.Direction() {
		dir = 1
	}

	if cmp := bytes.Compare(h[i].key, h[j].key); cmp == dir {
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

// TagSetCursor is virtual cursor that iterates over mutiple series cursors, as though it were
// a single series.
type TagSetCursor struct {
	measurement string            // Measurement name
	tags        map[string]string // Tag key-value pairs
	cursors     []*seriesCursor   // Underlying series cursors.
	decoder     *FieldCodec       // decoder for the raw data bytes
	currentTags map[string]string // the current tags for the underlying series cursor in play

	// pointHeap is a min-heap, ordered by timestamp, that contains the next
	// point from each seriesCursor. Queries sometimes pull points from
	// thousands of series. This makes it reasonably efficient to find the
	// point with the next lowest timestamp among the thousands of series that
	// the query is pulling points from.
	// Performance profiling shows that this lookahead needs to be part
	// of the TagSetCursor type and not part of the the cursors type.
	pointHeap *pointHeap

	// Memomize the cursor's tagset-based key. Profiling shows that calculating this
	// is significant CPU cost, and it only needs to be done once.
	memokey string
}

// TagSetCursors represents a sortable slice of TagSetCursors.
type TagSetCursors []*TagSetCursor

func (a TagSetCursors) Len() int           { return len(a) }
func (a TagSetCursors) Less(i, j int) bool { return a[i].key() < a[j].key() }
func (a TagSetCursors) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (a TagSetCursors) Keys() []string {
	keys := []string{}
	for i := range a {
		keys = append(keys, a[i].key())
	}
	sort.Strings(keys)
	return keys
}

// NewTagSetCursor returns a instance of TagSetCursor.
func NewTagSetCursor(m string, t map[string]string, c []*seriesCursor, d *FieldCodec) *TagSetCursor {
	tsc := &TagSetCursor{
		measurement: m,
		tags:        t,
		cursors:     c,
		decoder:     d,
		pointHeap:   newPointHeap(),
	}

	return tsc
}

func (tsc *TagSetCursor) key() string {
	if tsc.memokey == "" {
		if len(tsc.tags) == 0 {
			tsc.memokey = tsc.measurement
		} else {
			tsc.memokey = strings.Join([]string{tsc.measurement, string(MarshalTags(tsc.tags))}, "|")
		}
	}
	return tsc.memokey
}

func (tsc *TagSetCursor) SeekTo(seek int64) {
	tsc.pointHeap = newPointHeap()

	// Prime the buffers.
	for i := 0; i < len(tsc.cursors); i++ {
		k, v := tsc.cursors[i].SeekTo(seek)
		if k == -1 {
			k, v = tsc.cursors[i].Next()
		}
		if k == -1 {
			continue
		}

		heap.Push(tsc.pointHeap, &pointHeapItem{
			timestamp: k,
			value:     v,
			cursor:    tsc.cursors[i],
		})
	}
}

// Next returns the next matching series-key, timestamp byte slice and meta tags for the tagset. Filtering
// is enforced on the values. If there is no matching value, then a nil result is returned.
func (tsc *TagSetCursor) Next(tmin, tmax int64, selectFields, whereFields []string) (int64, interface{}) {
	for {
		// If we're out of points, we're done.
		if tsc.pointHeap.Len() == 0 {
			return -1, nil
		}

		// Grab the next point with the lowest timestamp.
		p := heap.Pop(tsc.pointHeap).(*pointHeapItem)

		// We're done if the point is outside the query's time range [tmin:tmax).
		if p.timestamp != tmin && (p.timestamp < tmin || p.timestamp >= tmax) {
			return -1, nil
		}

		// Decode the raw point.
		value := tsc.decodeRawPoint(p, selectFields, whereFields)
		timestamp := p.timestamp

		// Keep track of the current tags for the series cursor so we can
		// respond with them if asked
		tsc.currentTags = p.cursor.tags

		// Advance the cursor
		nextKey, nextVal := p.cursor.Next()
		if nextKey != -1 {
			*p = pointHeapItem{
				timestamp: nextKey,
				value:     nextVal,
				cursor:    p.cursor,
			}
			heap.Push(tsc.pointHeap, p)
		}

		// Value didn't match, look for the next one.
		if value == nil {
			continue
		}

		return timestamp, value
	}
}

// Tags returns the current tags of the current cursor
// if there is no current currsor, it returns nil
func (tsc *TagSetCursor) Tags() map[string]string {
	return tsc.currentTags
}

// decodeRawPoint decodes raw point data into field names & values and does WHERE filtering.
func (tsc *TagSetCursor) decodeRawPoint(p *pointHeapItem, selectFields, whereFields []string) interface{} {
	if len(selectFields) > 1 {
		if fieldsWithNames, err := tsc.decoder.DecodeFieldsWithNames(p.value); err == nil {
			// if there's a where clause, make sure we don't need to filter this value
			if p.cursor.filter != nil && !influxql.EvalBool(p.cursor.filter, fieldsWithNames) {
				return nil
			}

			return fieldsWithNames
		}
	}

	// With only 1 field SELECTed, decoding all fields may be avoidable, which is faster.
	value, err := tsc.decoder.DecodeByName(selectFields[0], p.value)
	if err != nil {
		return nil
	}

	// If there's a WHERE clase, see if we need to filter
	if p.cursor.filter != nil {
		// See if the WHERE is only on this field or on one or more other fields.
		// If the latter, we'll have to decode everything
		if len(whereFields) == 1 && whereFields[0] == selectFields[0] {
			if !influxql.EvalBool(p.cursor.filter, map[string]interface{}{selectFields[0]: value}) {
				value = nil
			}
		} else { // Decode everything
			fieldsWithNames, err := tsc.decoder.DecodeFieldsWithNames(p.value)
			if err != nil || !influxql.EvalBool(p.cursor.filter, fieldsWithNames) {
				value = nil
			}
		}
	}

	return value
}

type pointHeapItem struct {
	timestamp int64
	value     []byte
	cursor    *seriesCursor // cursor whence pointHeapItem came
}

type pointHeap []*pointHeapItem

func newPointHeap() *pointHeap {
	q := make(pointHeap, 0)
	heap.Init(&q)
	return &q
}

func (pq pointHeap) Len() int { return len(pq) }

func (pq pointHeap) Less(i, j int) bool {
	// We want a min-heap (points in chronological order), so use less than.
	return pq[i].timestamp < pq[j].timestamp
}

func (pq pointHeap) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *pointHeap) Push(x interface{}) {
	item := x.(*pointHeapItem)
	*pq = append(*pq, item)
}

func (pq *pointHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// seriesCursor is a cursor that walks a single series. It provides lookahead functionality.
type seriesCursor struct {
	cursor     Cursor // BoltDB cursor for a series
	filter     influxql.Expr
	tags       map[string]string
	seekto     int64
	seekResult struct {
		k int64
		v []byte
	}
}

// newSeriesCursor returns a new instance of a series cursor.
func newSeriesCursor(cur Cursor, filter influxql.Expr, tags map[string]string) *seriesCursor {
	return &seriesCursor{
		cursor: cur,
		filter: filter,
		tags:   tags,
		seekto: -1,
	}
}

// Seek positions returning the timestamp and value at that key.
func (sc *seriesCursor) SeekTo(key int64) (timestamp int64, value []byte) {
	if sc.seekto != -1 && sc.seekto < key && (sc.seekResult.k == -1 || sc.seekResult.k >= key) {
		// we've seeked on this cursor. This seek is after that previous cached seek
		// and the result it gave was after the key for this seek.
		//
		// In this case, any seek would just return what we got before, so there's
		// no point in reseeking.
		return sc.seekResult.k, sc.seekResult.v
	}
	k, v := sc.cursor.Seek(u64tob(uint64(key)))
	if k == nil {
		timestamp = -1
	} else {
		timestamp, value = int64(btou64(k)), v
	}
	sc.seekto = key
	sc.seekResult.k = timestamp
	sc.seekResult.v = v
	return
}

// Next returns the next timestamp and value from the cursor.
func (sc *seriesCursor) Next() (key int64, value []byte) {
	// calling next on this cursor means that we need to invalidate the seek
	sc.seekto = -1
	sc.seekResult.k = 0
	sc.seekResult.v = nil
	k, v := sc.cursor.Next()
	if k == nil {
		key = -1
	} else {
		key, value = int64(btou64(k)), v
	}
	return
}

// btou64 converts an 8-byte slice into an uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
