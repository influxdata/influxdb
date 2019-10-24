package gen

import (
	"container/heap"
	"math"

	"github.com/influxdata/influxdb/models"
)

type mergedSeriesGenerator struct {
	heap  seriesGeneratorHeap
	last  constSeries
	n     int64
	first bool
}

func NewMergedSeriesGenerator(s []SeriesGenerator) SeriesGenerator {
	if len(s) == 0 {
		return nil
	} else if len(s) == 1 {
		return s[0]
	}

	msg := &mergedSeriesGenerator{first: true, n: math.MaxInt64}
	msg.heap.init(s)
	return msg
}

func NewMergedSeriesGeneratorLimit(s []SeriesGenerator, n int64) SeriesGenerator {
	if len(s) == 0 {
		return nil
	}

	msg := &mergedSeriesGenerator{first: true, n: n}
	msg.heap.init(s)
	return msg
}

func (s *mergedSeriesGenerator) Next() bool {
	if len(s.heap.items) == 0 {
		return false
	}

	if s.n > 0 {
		s.n--
		if !s.first {
			top := s.heap.items[0]
			s.last.CopyFrom(top) // capture last key for duplicate checking

			for {
				if top.Next() {
					if len(s.heap.items) > 1 {
						heap.Fix(&s.heap, 0)
					}
				} else {
					heap.Pop(&s.heap)
					if len(s.heap.items) == 0 {
						return false
					}
				}

				top = s.heap.items[0]
				if CompareSeries(&s.last, top) == 0 {
					// duplicate key, get next
					continue
				}
				return true
			}
		}

		s.first = false
		return true
	}

	return false
}

func (s *mergedSeriesGenerator) Key() []byte {
	return s.heap.items[0].Key()
}

func (s *mergedSeriesGenerator) Name() []byte {
	return s.heap.items[0].Name()
}

func (s *mergedSeriesGenerator) Tags() models.Tags {
	return s.heap.items[0].Tags()
}

func (s *mergedSeriesGenerator) Field() []byte {
	return s.heap.items[0].Field()
}

func (s *mergedSeriesGenerator) TimeValuesGenerator() TimeValuesSequence {
	return s.heap.items[0].TimeValuesGenerator()
}

type seriesGeneratorHeap struct {
	items []SeriesGenerator
}

func (h *seriesGeneratorHeap) init(results []SeriesGenerator) {
	if cap(h.items) < len(results) {
		h.items = make([]SeriesGenerator, 0, len(results))
	} else {
		h.items = h.items[:0]
	}

	for _, rs := range results {
		if rs.Next() {
			h.items = append(h.items, rs)
		}
	}
	heap.Init(h)
}

func (h *seriesGeneratorHeap) Less(i, j int) bool {
	return CompareSeries(h.items[i], h.items[j]) == -1
}

func (h *seriesGeneratorHeap) Len() int {
	return len(h.items)
}

func (h *seriesGeneratorHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *seriesGeneratorHeap) Push(x interface{}) {
	panic("not implemented")
}

func (h *seriesGeneratorHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	h.items = old[0 : n-1]
	return item
}
