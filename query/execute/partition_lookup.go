package execute

import (
	"sort"

	"github.com/influxdata/platform/query"
)

type PartitionLookup struct {
	partitions partitionEntries

	//  range state
	rangeIdx int
}

type partitionEntry struct {
	key   query.PartitionKey
	value interface{}
}

func NewPartitionLookup() *PartitionLookup {
	return &PartitionLookup{
		partitions: make(partitionEntries, 0, 100),
	}
}

func (l *PartitionLookup) findIdx(key query.PartitionKey) int {
	i := sort.Search(len(l.partitions), func(i int) bool {
		return !l.partitions[i].key.Less(key)
	})
	if i < len(l.partitions) && l.partitions[i].key.Equal(key) {
		return i
	}
	return -1
}

func (l *PartitionLookup) Lookup(key query.PartitionKey) (interface{}, bool) {
	if key == nil {
		return nil, false
	}
	i := l.findIdx(key)
	if i >= 0 {
		return l.partitions[i].value, true
	}
	return nil, false
}

func (l *PartitionLookup) Set(key query.PartitionKey, value interface{}) {
	i := l.findIdx(key)
	if i >= 0 {
		l.partitions[i].value = value
	} else {
		l.partitions = append(l.partitions, partitionEntry{
			key:   key,
			value: value,
		})
		sort.Sort(l.partitions)
	}
}

func (l *PartitionLookup) Delete(key query.PartitionKey) (v interface{}, found bool) {
	if key == nil {
		return
	}
	i := l.findIdx(key)
	found = i >= 0
	if found {
		if i <= l.rangeIdx {
			l.rangeIdx--
		}
		v = l.partitions[i].value
		l.partitions = append(l.partitions[:i], l.partitions[i+1:]...)
	}
	return
}

// Range will iterate over all partitions keys in sorted order.
// Range must not be called within another call to Range.
// It is safe to call Set/Delete while ranging.
func (l *PartitionLookup) Range(f func(key query.PartitionKey, value interface{})) {
	for l.rangeIdx = 0; l.rangeIdx < len(l.partitions); l.rangeIdx++ {
		entry := l.partitions[l.rangeIdx]
		f(entry.key, entry.value)
	}
}

type partitionEntries []partitionEntry

func (p partitionEntries) Len() int               { return len(p) }
func (p partitionEntries) Less(i int, j int) bool { return p[i].key.Less(p[j].key) }
func (p partitionEntries) Swap(i int, j int)      { p[i], p[j] = p[j], p[i] }
