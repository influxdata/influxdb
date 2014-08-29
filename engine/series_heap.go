package engine

import (
	"sort"

	"github.com/influxdb/influxdb/common/heap"
	"github.com/influxdb/influxdb/protocol"
)

type Value struct {
	streamId int
	s        *protocol.Series
}

type ValueSlice []Value

func (vs ValueSlice) Len() int {
	return len(vs)
}

func (vs ValueSlice) Less(i, j int) bool {
	return vs[i].s.Points[0].GetTimestamp() < vs[j].s.Points[0].GetTimestamp()
}

func (vs ValueSlice) Swap(i, j int) {
	vs[i], vs[j] = vs[j], vs[i]
}

// A heap that keeps holds one point series' (series that have one
// point only) and their stream ids. See http://en.wikipedia.org/wiki/Heap_(data_structure)
// for more info on heaps. The heap is used by the Merger to emit
// points from multiple streams in monotic order.
type SeriesHeap struct {
	Ascending bool
	values    []Value
}

// returns the number of values in the heap so far
func (sh *SeriesHeap) Size() int {
	return len(sh.values)
}

// Add another one point series with the given stream id. TODO: This
// is slightly inefficient way to construct the initial value slice,
// if we had a value slice we can construct the heap in O(n) instead
// of O(n logn) which is required if we construct the heap using
// multiple calls to Add()
func (sh *SeriesHeap) Add(streamId int, s *protocol.Series) {
	sh.values = append(sh.values, Value{
		s:        s,
		streamId: streamId,
	})
	l := sh.Size()
	if sh.Ascending {
		heap.BubbleUp(ValueSlice(sh.values), l-1)
	} else {
		heap.BubbleUp(sort.Reverse(ValueSlice(sh.values)), l-1)
	}
}

// Get and remove the next one point series that has smallest (or
// largest) timestmap, according to the Ascending field. TODO: This is
// slightly inefficient since we remove a value from the values slice
// and do a BubbleDown which is inefficient since the next value from
// the stream will be added immediately after and will cause a
// BubbleUp. In big O() notation this step doesn't change much, it
// only adds a contant to the upper bound.
func (sh *SeriesHeap) Next() (int, *protocol.Series) {
	idx := 0
	s := sh.Size()
	v := sh.values[idx]
	sh.values, sh.values[0] = sh.values[:s-1], sh.values[s-1]
	if sh.Ascending {
		heap.BubbleDown(ValueSlice(sh.values), 0)
	} else {
		heap.BubbleDown(sort.Reverse(ValueSlice(sh.values)), 0)
	}
	return v.streamId, v.s
}
