package reads

import (
	"bytes"
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/slices"
)

// groupNoneMergedGroupResultSet produces a single GroupCursor, merging all
// GroupResultSet#Keys
type groupNoneMergedGroupResultSet struct {
	g    []GroupResultSet
	gc   groupNoneMergedGroupCursor
	done bool
}

// Returns a GroupResultSet that merges results using the datatypes.GroupNone
// strategy. Each source GroupResultSet in g must be configured using the
// GroupNone strategy or the results are undefined.
//
// The GroupNone strategy must merge the partition key and tag keys
// from each source GroupResultSet when producing its
func NewGroupNoneMergedGroupResultSet(g []GroupResultSet) GroupResultSet {
	if len(g) == 0 {
		return nil
	} else if len(g) == 1 {
		return g[0]
	}

	grs := &groupNoneMergedGroupResultSet{
		g: g,
		gc: groupNoneMergedGroupCursor{
			mergedResultSet: mergedResultSet{first: true},
		},
	}

	var km keyMerger
	results := make([]ResultSet, 0, len(g))
	for _, rs := range g {
		if gc := rs.Next(); gc != nil {
			results = append(results, gc)
			km.mergeKeys(gc.Keys())
		} else if rs.Err() != nil {
			grs.done = true
			grs.gc.err = rs.Err()
			results = nil
			break
		}
	}

	if len(results) > 0 {
		grs.gc.keys = km.get()
		grs.gc.heap.init(results)
	}

	return grs
}

func (r *groupNoneMergedGroupResultSet) Next() GroupCursor {
	if !r.done {
		r.done = true
		return &r.gc
	}
	return nil
}

func (r *groupNoneMergedGroupResultSet) Err() error { return r.gc.err }

func (r *groupNoneMergedGroupResultSet) Close() {
	r.gc.Close()
	for _, grs := range r.g {
		grs.Close()
	}
	r.g = nil
}

type groupNoneMergedGroupCursor struct {
	mergedResultSet
	keys [][]byte
}

func (r *groupNoneMergedGroupCursor) Keys() [][]byte {
	return r.keys
}

func (r *groupNoneMergedGroupCursor) PartitionKeyVals() [][]byte {
	return nil
}

// groupByMergedGroupResultSet implements the GroupBy strategy.
type groupByMergedGroupResultSet struct {
	items        []*groupCursorItem
	alt          []*groupCursorItem
	groupCursors []GroupCursor
	resultSets   []ResultSet
	nilVal       []byte
	err          error

	km models.TagKeysSet
	gc groupByMergedGroupCursor
}

// Returns a GroupResultSet that merges results using the datatypes.GroupBy
// strategy. Each source GroupResultSet in g must be configured using the
// GroupBy strategy with the same GroupKeys or the results are undefined.
func NewGroupByMergedGroupResultSet(g []GroupResultSet) GroupResultSet {
	if len(g) == 0 {
		return nil
	} else if len(g) == 1 {
		return g[0]
	}

	grs := &groupByMergedGroupResultSet{}
	grs.nilVal = nilSortHi
	grs.groupCursors = make([]GroupCursor, 0, len(g))
	grs.resultSets = make([]ResultSet, 0, len(g))
	grs.items = make([]*groupCursorItem, 0, len(g))
	grs.alt = make([]*groupCursorItem, 0, len(g))
	for _, rs := range g {
		grs.items = append(grs.items, &groupCursorItem{grs: rs})
	}

	return grs
}

// next determines the cursors for the next partition key.
func (r *groupByMergedGroupResultSet) next() {
	r.alt = r.alt[:0]
	for i, item := range r.items {
		if item.gc == nil {
			item.gc = item.grs.Next()
			if item.gc != nil {
				r.alt = append(r.alt, item)
			} else {
				r.err = item.grs.Err()
				item.grs.Close()
			}
		} else {
			// append remaining non-nil cursors
			r.alt = append(r.alt, r.items[i:]...)
			break
		}
	}

	r.items, r.alt = r.alt, r.items
	if len(r.items) == 0 {
		r.groupCursors = r.groupCursors[:0]
		r.resultSets = r.resultSets[:0]
		return
	}

	if r.err != nil {
		r.Close()
		return
	}

	sort.Slice(r.items, func(i, j int) bool {
		return comparePartitionKey(r.items[i].gc.PartitionKeyVals(), r.items[j].gc.PartitionKeyVals(), r.nilVal) == -1
	})

	r.groupCursors = r.groupCursors[:1]
	r.resultSets = r.resultSets[:1]

	first := r.items[0].gc
	r.groupCursors[0] = first
	r.resultSets[0] = first
	r.items[0].gc = nil

	for i := 1; i < len(r.items); i++ {
		if slices.CompareSlice(first.PartitionKeyVals(), r.items[i].gc.PartitionKeyVals()) == 0 {
			r.groupCursors = append(r.groupCursors, r.items[i].gc)
			r.resultSets = append(r.resultSets, r.items[i].gc)
			r.items[i].gc = nil
		}
	}
}

func (r *groupByMergedGroupResultSet) Next() GroupCursor {
	r.next()
	if len(r.groupCursors) == 0 {
		return nil
	}

	r.gc.first = true
	r.gc.heap.init(r.resultSets)

	r.km.Clear()
	for i := range r.groupCursors {
		r.km.UnionBytes(r.groupCursors[i].Keys())
	}

	r.gc.keys = append(r.gc.keys[:0], r.km.KeysBytes()...)
	r.gc.vals = r.groupCursors[0].PartitionKeyVals()
	return &r.gc
}

func (r *groupByMergedGroupResultSet) Err() error { return r.err }

func (r *groupByMergedGroupResultSet) Close() {
	r.gc.Close()
	for _, grs := range r.items {
		if grs.gc != nil {
			grs.gc.Close()
		}
		grs.grs.Close()
	}
	r.items = nil
	r.alt = nil
}

type groupByMergedGroupCursor struct {
	mergedResultSet
	keys [][]byte
	vals [][]byte
}

func (r *groupByMergedGroupCursor) Keys() [][]byte {
	return r.keys
}

func (r *groupByMergedGroupCursor) PartitionKeyVals() [][]byte {
	return r.vals
}

type groupCursorItem struct {
	grs GroupResultSet
	gc  GroupCursor
}

func comparePartitionKey(a, b [][]byte, nilVal []byte) int {
	i := 0
	for i < len(a) && i < len(b) {
		av, bv := a[i], b[i]
		if len(av) == 0 {
			av = nilVal
		}
		if len(bv) == 0 {
			bv = nilVal
		}
		if v := bytes.Compare(av, bv); v == 0 {
			i++
			continue
		} else {
			return v
		}
	}

	if i < len(b) {
		// b is longer, so assume a is less
		return -1
	} else if i < len(a) {
		// a is longer, so assume b is less
		return 1
	} else {
		return 0
	}
}
