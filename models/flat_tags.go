package models

import (
	"bytes"
	"sync"
)

// flatTagsPool is used to encourage reuse of flatTags instances.
var flatTagsPool = &sync.Pool{
	New: func() interface{} {
		return &flatTags{Data: []flatTag{}}
	},
}

// flatTagsGetFromPool gets a ready-to-use flatTags instance from the pool.
func flatTagsGetFromPool() *flatTags {
	return flatTagsPool.Get().(*flatTags)
}

// flatTagsPutIntoPool resets a flatTags instance and puts it into its pool.
func flatTagsPutIntoPool(x *flatTags) {
	x.Data = x.Data[:0]
	x.Sorted = true
	flatTagsPool.Put(x)
}

// flatTag holds a basic pair of byte slices.
type flatTag struct {
	Key, Val []byte
}

// flatTags holds a slice of flatTag instances. It is a lower-overhead
// alternative to map[string]string that is used elsewhere in this package.
type flatTags struct {
	Sorted bool
	Data   []flatTag
}

// Len helps implement sort.Interface.
func (a *flatTags) Len() int { return len(a.Data) }

// Swap helps implement sort.Interface.
func (a *flatTags) Swap(i, j int) { a.Data[i], a.Data[j] = a.Data[j], a.Data[i] }

// Less helps implement sort.Interface.
func (a *flatTags) Less(i, j int) bool {
	return bytes.Compare(a.Data[i].Key, a.Data[j].Key) < 0
}

// Get accesses the i'th element.
func (a *flatTags) Get(i int) ([]byte, []byte) {
	x := a.Data[i]
	return x.Key, x.Val
}

// IsSorted is a simple predicate. Use it to skip unnecessary calls to
// InsertionSort.
func (a *flatTags) IsSorted() bool {
	return a.Sorted
}

// InsertionSort is an in-place zero-overhead sort. It should be used instead
// of sort.Sort, to avoid heap allocations. It is less efficient for larger
// sets of items.
func (a *flatTags) InsertionSort() {
	for i := 1; i < len(a.Data); i++ {
		for j := i; j > 0 && a.Less(j, j-1); j-- {
			a.Swap(j, j-1)
		}
	}
	a.Sorted = true
}

// Append appends a new key, value pair to this flatTags instance.
func (a *flatTags) Append(k, v []byte) {
	a.Data = append(a.Data, flatTag{
		Key: k,
		Val: v,
	})
	if a.Sorted && len(a.Data) >= 2 {
		i := len(a.Data) - 2
		j := len(a.Data) - 1
		a.Sorted = a.Less(i, j)
	}
}
