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
		top.Close()
		heap.Pop(&r.heap)
		if err != nil {
			r.err = err
			r.Close()
		}

		r.stats.Add(top.Stats())

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
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	h.items = old[0 : n-1]
	return item
}
