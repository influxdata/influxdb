package reads

import (
	"container/heap"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type sequenceResultSet struct {
	items []ResultSet
	rs    ResultSet
	err   error
	stats cursors.CursorStats
}

// NewSequenceResultSet combines results into a single ResultSet,
// draining each ResultSet in order before moving to the next.
func NewSequenceResultSet(results []ResultSet) ResultSet {
	if len(results) == 0 {
		return nil
	} else if len(results) == 1 {
		return results[0]
	}

	rs := &sequenceResultSet{items: results}
	rs.pop()
	return rs
}

func (r *sequenceResultSet) Err() error { return r.err }

func (r *sequenceResultSet) Close() {
	if r.rs != nil {
		r.rs.Close()
		r.rs = nil
	}

	for _, rs := range r.items {
		rs.Close()
	}
	r.items = nil
}

func (r *sequenceResultSet) pop() bool {
	if r.rs != nil {
		r.rs.Close()
		r.rs = nil
	}

	if len(r.items) > 0 {
		r.rs = r.items[0]
		r.items[0] = nil
		r.items = r.items[1:]
		return true
	}

	return false
}

func (r *sequenceResultSet) Next() bool {
RETRY:
	if r.rs != nil {
		if r.rs.Next() {
			return true
		}

		err := r.rs.Err()
		stats := r.rs.Stats()
		if err != nil {
			r.err = err
			r.Close()
			return false
		}

		r.stats.Add(stats)

		if r.pop() {
			goto RETRY
		}
	}

	return false
}

func (r *sequenceResultSet) Cursor() cursors.Cursor {
	return r.rs.Cursor()
}

func (r *sequenceResultSet) Tags() models.Tags {
	return r.rs.Tags()
}

func (r *sequenceResultSet) Stats() cursors.CursorStats {
	return r.stats
}

type mergedResultSet struct {
	heap  resultSetHeap
	err   error
	first bool
	stats cursors.CursorStats
}

// NewMergedResultSet combines the results into a single ResultSet,
// producing keys in ascending lexicographical order. It requires
// all input results are ordered.
func NewMergedResultSet(results []ResultSet) ResultSet {
	if len(results) == 0 {
		return nil
	} else if len(results) == 1 {
		return results[0]
	}

	mrs := &mergedResultSet{first: true}
	mrs.heap.init(results)
	return mrs
}

func (r *mergedResultSet) Err() error { return r.err }

func (r *mergedResultSet) Close() {
	for _, rs := range r.heap.items {
		rs.Close()
	}
	r.heap.items = nil
}

func (r *mergedResultSet) Next() bool {
	if len(r.heap.items) == 0 {
		return false
	}

	if !r.first {
		top := r.heap.items[0]
		if top.Next() {
			heap.Fix(&r.heap, 0)
			return true
		}
		err := top.Err()
		stats := top.Stats()
		top.Close()
		heap.Pop(&r.heap)
		if err != nil {
			r.err = err
			r.Close()
			return false
		}

		r.stats.Add(stats)

		return len(r.heap.items) > 0
	}

	r.first = false
	return true
}

func (r *mergedResultSet) Cursor() cursors.Cursor {
	return r.heap.items[0].Cursor()
}

func (r *mergedResultSet) Tags() models.Tags {
	return r.heap.items[0].Tags()
}

func (r *mergedResultSet) Stats() cursors.CursorStats {
	return r.stats
}

type resultSetHeap struct {
	items []ResultSet
}

func (h *resultSetHeap) init(results []ResultSet) {
	if cap(h.items) < len(results) {
		h.items = make([]ResultSet, 0, len(results))
	} else {
		h.items = h.items[:0]
	}

	for _, rs := range results {
		if rs.Next() {
			h.items = append(h.items, rs)
		} else {
			rs.Close()
		}
	}
	heap.Init(h)
}

func (h *resultSetHeap) Less(i, j int) bool {
	return models.CompareTags(h.items[i].Tags(), h.items[j].Tags()) == -1
}

func (h *resultSetHeap) Len() int {
	return len(h.items)
}

func (h *resultSetHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *resultSetHeap) Push(x interface{}) {
	panic("not implemented")
}

func (h *resultSetHeap) Pop() interface{} {
	n := len(h.items)
	item := h.items[n-1]
	h.items[n-1] = nil
	h.items = h.items[:n-1]
	return item
}

// MergedStringIterator merges multiple storage.StringIterators into one.
// It sorts and deduplicates adjacent values, so the output is sorted iff all inputs are sorted.
// If all inputs are not sorted, then output order and deduplication are undefined and unpleasant.
type MergedStringIterator struct {
	heap      stringIteratorHeap
	nextValue string
	stats     cursors.CursorStats
}

// API compatibility
var _ cursors.StringIterator = (*MergedStringIterator)(nil)

func NewMergedStringIterator(iterators []cursors.StringIterator) *MergedStringIterator {
	nonEmptyIterators := make([]cursors.StringIterator, 0, len(iterators))
	var stats cursors.CursorStats

	for _, iterator := range iterators {
		// All iterators must be Next()'d so that their Value() methods return a meaningful value, and sort properly.
		if iterator.Next() {
			nonEmptyIterators = append(nonEmptyIterators, iterator)
		} else {
			stats.Add(iterator.Stats())
		}
	}

	msi := &MergedStringIterator{
		heap:  stringIteratorHeap{iterators: nonEmptyIterators},
		stats: stats,
	}
	heap.Init(&msi.heap)

	return msi
}

func (msi *MergedStringIterator) Next() bool {
	for msi.heap.Len() > 0 {
		iterator := msi.heap.iterators[0]

		haveNext := false
		if proposedNextValue := iterator.Value(); proposedNextValue != msi.nextValue { // Skip dupes.
			msi.nextValue = proposedNextValue
			haveNext = true
		}

		if iterator.Next() {
			// iterator.Value() has changed, so re-order that iterator within the heap
			heap.Fix(&msi.heap, 0)
		} else {
			// iterator is drained, so count the stats and remove it from the heap
			msi.stats.Add(iterator.Stats())
			heap.Pop(&msi.heap)
		}

		if haveNext {
			return true
		}
	}

	return false
}

func (msi *MergedStringIterator) Value() string {
	return msi.nextValue
}

func (msi *MergedStringIterator) Stats() cursors.CursorStats {
	return msi.stats
}

type stringIteratorHeap struct {
	iterators []cursors.StringIterator
}

func (h stringIteratorHeap) Len() int {
	return len(h.iterators)
}

func (h stringIteratorHeap) Less(i, j int) bool {
	return h.iterators[i].Value() < h.iterators[j].Value()
}

func (h *stringIteratorHeap) Swap(i, j int) {
	h.iterators[i], h.iterators[j] = h.iterators[j], h.iterators[i]
}

func (h *stringIteratorHeap) Push(x interface{}) {
	h.iterators = append(h.iterators, x.(cursors.StringIterator))
}

func (h *stringIteratorHeap) Pop() interface{} {
	n := len(h.iterators)
	item := h.iterators[n-1]
	h.iterators[n-1] = nil
	h.iterators = h.iterators[:n-1]
	return item
}
