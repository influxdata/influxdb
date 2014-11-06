package engine

import (
	"container/heap"

	"github.com/influxdb/influxdb/protocol"
)

type Value struct {
	streamId int
	s        *protocol.Series
}

type MinValueSlice struct {
	values []Value
}

func (mvs *MinValueSlice) Len() int {
	return len(mvs.values)
}

func (mvs *MinValueSlice) Less(i, j int) bool {
	return mvs.values[i].s.Points[0].GetTimestamp() < mvs.values[j].s.Points[0].GetTimestamp()
}

func (mvs *MinValueSlice) Swap(i, j int) {
	mvs.values[i], mvs.values[j] = mvs.values[j], mvs.values[i]
}

func (mvs *MinValueSlice) Push(x interface{}) {
	mvs.values = append(mvs.values, x.(Value))
}

func (mvs *MinValueSlice) Pop() interface{} {
	l := len(mvs.values)
	var v interface{}
	v, mvs.values = mvs.values[l-1], mvs.values[:l-1]
	return v
}

type MaxValueSlice struct {
	*MinValueSlice
}

func (mvs MaxValueSlice) Less(i, j int) bool {
	return mvs.values[i].s.Points[0].GetTimestamp() >
		mvs.values[j].s.Points[0].GetTimestamp()
}

// A heap that holds one point series' (series that have one
// point only) and their stream ids. See http://en.wikipedia.org/wiki/Heap_(data_structure)
// for more info on heaps. The heap is used by the Merger to emit
// points from multiple streams in monotic order.
type SeriesHeap struct {
	Ascending bool
	values    *MinValueSlice
}

func NewSeriesHeap(asc bool) SeriesHeap {
	return SeriesHeap{
		Ascending: asc,
		values:    &MinValueSlice{},
	}
}

// returns the number of values in the heap so far
func (sh SeriesHeap) Size() int {
	return sh.values.Len()
}

// Add another one point series with the given stream id. TODO: This
// is slightly inefficient way to construct the initial value slice,
// if we had a value slice we can construct the heap in O(n) instead
// of O(n logn) which is required if we construct the heap using
// multiple calls to Add()
func (sh SeriesHeap) Add(streamId int, s *protocol.Series) {
	var vs heap.Interface
	if sh.Ascending {
		vs = sh.values
	} else {
		vs = MaxValueSlice{sh.values}
	}
	heap.Push(vs, Value{
		s:        s,
		streamId: streamId,
	})
}

// Get and remove the next one point series that has smallest (or
// largest) timestmap, according to the Ascending field. TODO: This is
// slightly inefficient since we remove a value from the values slice
// and do a BubbleDown which is inefficient since the next value from
// the stream will be added immediately after and will cause a
// BubbleUp. In big O() notation this step doesn't change much, it
// only adds a contant to the upper bound.
func (sh SeriesHeap) Next() (int, *protocol.Series) {
	var vs heap.Interface
	if sh.Ascending {
		vs = sh.values
	} else {
		vs = MaxValueSlice{sh.values}
	}
	v := heap.Remove(vs, 0).(Value)
	return v.streamId, v.s
}
