package storageflux

import (
	"container/list"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/influxdata/flux/execute"
)

// defaultMaxLengthForTagsCache is the default maximum number of
// tags that will be memoized when retrieving tags from the tags
// cache.
const defaultMaxLengthForTagsCache = 100

type tagsCache struct {
	// startColumn is a special slot for holding the start column.
	startColumn *array.Int64

	// stopColumn is a special slot for holding the stop column.
	stopColumn *array.Int64

	// tags holds cached arrays for various tag values.
	// An lru is used to keep track of the least recently used
	// item in the cache so that it can be ejected. An lru is used
	// here because we cannot be certain if tag values are going to
	// be used again and we do not want to retain a reference
	// that may have already been released. This makes an lru a good
	// fit since it will be more likely to eject a value that is not
	// going to be used again than another data structure.
	//
	// The increase in performance by reusing arrays for tag values
	// is dependent on the order of the tags coming out of storage.
	// It is possible that a value will be reused but could get
	// ejected from the cache before it would be reused.
	//
	// The map contains the tag **values** and not the tag keys.
	// An array can get shared among two different tag keys that
	// have the same value.
	tags      map[string]*list.Element
	lru       *list.List
	maxLength int
}

// newTagsCache will create a tags cache that will retain
// the last sz entries. If zero, the default will be used.
func newTagsCache(sz int) *tagsCache {
	return &tagsCache{maxLength: sz}
}

// GetBounds will return arrays that match with the bounds.
// If an array that is within the cache works with the bounds
// and can be sliced to the length, a reference to it will be
// returned.
func (c *tagsCache) GetBounds(b execute.Bounds, l int, mem memory.Allocator) (start *array.Int64, stop *array.Int64) {
	if c == nil {
		start = c.createBounds(b.Start, l, mem)
		stop = c.createBounds(b.Stop, l, mem)
		return start, stop
	}

	if c.startColumn != nil {
		start = c.getOrReplaceBounds(&c.startColumn, b.Start, l, mem)
	} else {
		start = c.createBounds(b.Start, l, mem)
		start.Retain()
		c.startColumn = start
	}

	if c.stopColumn != nil {
		stop = c.getOrReplaceBounds(&c.stopColumn, b.Stop, l, mem)
	} else {
		stop = c.createBounds(b.Stop, l, mem)
		stop.Retain()
		c.stopColumn = stop
	}

	return start, stop
}

// getOrReplaceBounds will get or replace an array of timestamps
// and return a new reference to it.
func (c *tagsCache) getOrReplaceBounds(arr **array.Int64, ts execute.Time, l int, mem memory.Allocator) *array.Int64 {
	if (*arr).Len() < l {
		(*arr).Release()
		*arr = c.createBounds(ts, l, mem)
		(*arr).Retain()
		return *arr
	} else if (*arr).Len() == l {
		(*arr).Retain()
		return *arr
	}

	// If the lengths do not match, but the cached array is less
	// than the desired array, then we can use slice.
	// NewSlice will automatically create a new reference to the
	// passed in array so we do not need to manually retain.
	data := array.NewSliceData((*arr).Data(), 0, int64(l))
	vs := array.NewInt64Data(data)
	data.Release()
	return vs
}

func (c *tagsCache) createBounds(ts execute.Time, l int, mem memory.Allocator) *array.Int64 {
	b := array.NewInt64Builder(mem)
	b.Resize(l)
	for i := 0; i < l; i++ {
		b.Append(int64(ts))
	}
	return b.NewInt64Array()
}

// GetTag returns a binary arrow array that contains the value
// repeated l times. If an array with a length greater than or
// equal to the length and with the same value exists in the cache,
// a reference to the data will be retained and returned.
// Otherwise, the allocator will be used to construct a new column.
func (c *tagsCache) GetTag(value string, l int, mem memory.Allocator) *array.Binary {
	if l == 0 {
		return c.createTag(value, l, mem)
	}

	if elem, ok := c.tags[value]; ok {
		return c.getOrReplaceTag(elem, value, l, mem)
	}

	arr := c.createTag(value, l, mem)
	if c.lru == nil {
		c.lru = list.New()
	}
	if c.tags == nil {
		c.tags = make(map[string]*list.Element)
	}
	c.tags[value] = c.lru.PushFront(arr)
	c.maintainLRU()
	arr.Retain()
	return arr
}

func (c *tagsCache) getOrReplaceTag(elem *list.Element, value string, l int, mem memory.Allocator) *array.Binary {
	// Move this element to the front of the lru.
	c.lru.MoveBefore(elem, c.lru.Front())

	// Determine if the array can be reused.
	arr := elem.Value.(*array.Binary)
	if arr.Len() < l {
		// Create a new array with the appropriate length since
		// this one cannot be reused here.
		arr.Release()
		arr = c.createTag(value, l, mem)
		elem.Value = arr
		arr.Retain()
		return arr
	} else if arr.Len() == l {
		arr.Retain()
		return arr
	}

	// If the lengths do not match, but the cached array is less
	// than the desired array, then we can use slice.
	// Slice will automatically create a new reference to the
	// passed in array so we do not need to manually retain.
	data := array.NewSliceData(arr.Data(), 0, int64(l))
	vs := array.NewBinaryData(data)
	data.Release()
	return vs
}

// maintainLRU will ensure the lru cache maintains the appropriate
// length by ejecting the least recently used value from the cache
// until the cache is the appropriate size.
func (c *tagsCache) maintainLRU() {
	max := c.maxLength
	if max == 0 {
		max = defaultMaxLengthForTagsCache
	}
	if c.lru.Len() <= max {
		return
	}
	arr := c.lru.Remove(c.lru.Back()).(*array.Binary)
	value := arr.ValueString(0)
	delete(c.tags, value)
	arr.Release()
}

func (c *tagsCache) createTag(value string, l int, mem memory.Allocator) *array.Binary {
	b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.String)
	b.Resize(l)
	b.ReserveData(l * len(value))
	for i := 0; i < l; i++ {
		b.AppendString(value)
	}
	return b.NewBinaryArray()
}

// Release will release all references to cached tag columns.
func (c *tagsCache) Release() {
	if c.startColumn != nil {
		c.startColumn.Release()
		c.startColumn = nil
	}

	if c.stopColumn != nil {
		c.stopColumn.Release()
		c.stopColumn = nil
	}

	for _, elem := range c.tags {
		elem.Value.(*array.Binary).Release()
	}
	c.tags = nil
	c.lru = nil
}
