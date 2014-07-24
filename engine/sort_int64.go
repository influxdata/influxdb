package engine

import (
	"sort"
)

// Int64Slice attaches the methods of sort.Interface to []int64, sorting in increasing order.
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func SortInt64(ints []int64) {
	sort.Sort(Int64Slice(ints))
}
