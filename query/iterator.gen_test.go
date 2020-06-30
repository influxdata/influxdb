package query

import (
	"testing"

	"github.com/influxdata/influxql"
)

// a simple FloatIterator for testing
type floatIterator struct {
	points []FloatPoint
	closed bool
	stats  IteratorStats
}

func (itr *floatIterator) Stats() IteratorStats { return itr.stats }
func (itr *floatIterator) Close() error         { itr.closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *floatIterator) Next() (*FloatPoint, error) {
	if len(itr.points) == 0 || itr.closed {
		return nil, nil
	}
	v := &itr.points[0]
	itr.points = itr.points[1:]
	return v, nil
}

func TestSortedMergeHeap_DetectFast(t *testing.T) {

	suite := []*struct {
		inputs    []FloatIterator
		ascending bool
		fast      bool // expected status
	}{

		// case 0
		{
			inputs: []FloatIterator{
				&floatIterator{
					points: []FloatPoint{
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 0, Value: 1},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 12, Value: 3},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 30, Value: 4},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "B"}), Time: 40, Value: 2},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 50, Value: 8},
					},
					stats: IteratorStats{SeriesN: 3},
				},
				&floatIterator{
					points: []FloatPoint{
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 0, Value: 1},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 12, Value: 3},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 30, Value: 4},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "B"}), Time: 40, Value: 2},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 50, Value: 8},
					},
					stats: IteratorStats{SeriesN: 3},
				},
			},
			ascending: true,
			fast:      false,
		},
		// case 1
		{
			inputs: []FloatIterator{
				&floatIterator{
					points: []FloatPoint{
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 0, Value: 1},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 12, Value: 3},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 30, Value: 4},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 61, Value: 8},
					},
					stats: IteratorStats{SeriesN: 1},
				},
				&floatIterator{
					points: []FloatPoint{
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 0, Value: 1},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 12, Value: 3},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 30, Value: 4},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 4, Value: 2},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 61, Value: 8},
					},
					stats: IteratorStats{SeriesN: 1},
				},
			},
			ascending: false,
			fast:      true,
		},
		// case 2
		{
			inputs: []FloatIterator{
				&floatIterator{
					points: []FloatPoint{
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 0, Value: 1},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 12, Value: 3},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 30, Value: 4},
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "A"}), Time: 51, Value: 8},
					},
					stats: IteratorStats{SeriesN: 1},
				},
				&floatIterator{
					points: []FloatPoint{
						{Name: "cpu", Tags: NewTags(map[string]string{"host": "B"}), Time: 1, Value: 8},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 10, Value: 1},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 12, Value: 3},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 30, Value: 4},
						{Name: "mem", Tags: NewTags(map[string]string{"host": "B"}), Time: 40, Value: 2},
					},
					stats: IteratorStats{SeriesN: 2},
				},
			},
			ascending: true,
			fast:      false,
		},
	}

	for i, c := range suite {
		h := createFloatSortedMergeHeap(
			c.inputs,
			IteratorOptions{
				StartTime: influxql.MinTime,
				EndTime:   influxql.MaxTime,
				Ascending: c.ascending,
			})
		h.detectFast()
		if h.fast != c.fast {
			t.Fatalf("unexpected shortcut status for sorted merge heap, case %d", i)
		}
	}
}

func createFloatSortedMergeHeap(inputs []FloatIterator, opt IteratorOptions) *floatSortedMergeHeap {
	h := &floatSortedMergeHeap{
		items: make([]*floatSortedMergeHeapItem, 0, len(inputs)),
		opt:   opt,
	}

	items2 := make([]*floatSortedMergeHeapItem, 0, len(inputs))
	for _, input := range inputs {
		items2 = append(items2, &floatSortedMergeHeapItem{itr: input})
	}
	for _, item := range items2 {
		var err error
		if item.point, err = item.itr.Next(); err != nil {
			panic(err)
		} else if item.point == nil {
			continue
		}
		h.items = append(h.items, item)
	}
	return h
}

// a simple iterator that has only a single series
type simpleFloatIterator struct {
	point     FloatPoint
	size      int
	populated int
	stats     IteratorStats
}

func (itr *simpleFloatIterator) Stats() IteratorStats {
	return itr.stats
}

func (itr *simpleFloatIterator) Close() error { itr.populated = itr.size; return nil }
func (itr *simpleFloatIterator) Next() (*FloatPoint, error) {
	if itr.populated >= itr.size {
		return nil, nil
	}
	p := itr.point.Clone()
	p.Time += int64(itr.populated * 1000)
	itr.populated++
	return p, nil
}

func BenchmarkSortedMergeIterator_Fast(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sortedMergeIterFast()
	}
}

func BenchmarkSortedMergeIterator_NotFast(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sortedMergeIterNotFast()
	}
}

func sortedMergeIterFast() {
	inputs := []Iterator{}
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "one"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 1},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "two"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 1},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "three"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 1},
		})

	itr := NewSortedMergeIterator(inputs, IteratorOptions{}).(*floatSortedMergeIterator)
	p, _ := itr.Next()
	for p != nil {
		p, _ = itr.Next()
	}
}

func sortedMergeIterNotFast() {
	inputs := []Iterator{}
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "four"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 2},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "five"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 2},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "fix"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 2},
		})

	opt := IteratorOptions{
		Dimensions: []string{"taga", "tagb", "tagc"},
	}
	itr := NewSortedMergeIterator(inputs, opt).(*floatSortedMergeIterator)
	p, _ := itr.Next()
	for p != nil {
		p, _ = itr.Next()
	}
}

func BenchmarkSortedMergeIterator_FastCheckOverhead(b *testing.B) {
	inputs := []FloatIterator{}
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "one"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 1},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "two"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 1},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "three"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 1},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "four"}), Time: 10, Value: 2},
			size:  1000000,
			stats: IteratorStats{SeriesN: 1},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "five"}), Time: 10, Value: 2},
			size:  1000000,
			stats: IteratorStats{SeriesN: 1},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "fix"}), Time: 10, Value: 2},
			size:  1000000,
			stats: IteratorStats{SeriesN: 1},
		})
	inputs = append(inputs,
		&simpleFloatIterator{
			point: FloatPoint{Name: "cpu", Tags: NewTags(map[string]string{"taga": "aaaaaaaaaa", "tagb": "bbbbbbbbbb", "tagc": "cccccccccc", "tagd": "dddddddddd", "tage": "eeeeeeeeee", "tagf": "one"}), Time: 10, Value: 2},
			size:  10000,
			stats: IteratorStats{SeriesN: 1},
		})
	h := createFloatSortedMergeHeap(
		inputs,
		IteratorOptions{
			StartTime: influxql.MinTime,
			EndTime:   influxql.MaxTime,
		})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.detectFast()
		if h.fast {
			panic("unexpected shortcut")
		}
	}
}
