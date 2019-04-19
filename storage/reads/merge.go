package reads

import (
	"container/heap"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type mergedResultSet struct {
	heap  resultSetHeap
	err   error
	first bool
	stats cursors.CursorStats
}

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
}

// API compatibility
var _ cursors.StringIterator = (*MergedStringIterator)(nil)

func NewMergedStringIterator(iterators []cursors.StringIterator) *MergedStringIterator {
	nonEmptyIterators := make([]cursors.StringIterator, 0, len(iterators))

	for _, iterator := range iterators {
		// All iterators must be Next()'d so that their Value() methods return a meaningful value, and sort properly.
		if iterator.Next() {
			nonEmptyIterators = append(nonEmptyIterators, iterator)
		}
	}

	msi := &MergedStringIterator{
		heap: stringIteratorHeap{iterators: nonEmptyIterators},
	}
	heap.Init(&msi.heap)

	return msi
}

func (msi *MergedStringIterator) Next() bool {
	for msi.heap.Len() > 0 {
		iterator := heap.Pop(&msi.heap).(cursors.StringIterator)

		haveNext := false
		if proposedNextValue := iterator.Value(); proposedNextValue != msi.nextValue { // Skip dupes.
			msi.nextValue = proposedNextValue
			haveNext = true
		}

		if iterator.Next() {
			heap.Push(&msi.heap, iterator)
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

func (mr *MergedStringIterator) Stats() cursors.CursorStats {
	return cursors.CursorStats{}
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
