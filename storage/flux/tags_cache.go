package storageflux

import (
	"container/list"
	"sync"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/influxdata/flux/array"
	"github.com/influxdata/flux/execute"
)

// defaultMaxLengthForTagsCache is the default maximum number of
// tags that will be memoized when retrieving tags from the tags
// cache.
const defaultMaxLengthForTagsCache = 100

type tagsCache struct {
	// startColumn is a special slot for holding the start column.
	startColumn *array.Int

	// stopColumn is a special slot for holding the stop column.
	stopColumn *array.Int

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
	mu        sync.RWMutex
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
func (c *tagsCache) GetBounds(b execute.Bounds, l int, mem memory.Allocator) (start *array.Int, stop *array.Int) {
	if c == nil {
		start = c.createBounds(b.Start, l, mem)
		stop = c.createBounds(b.Stop, l, mem)
		return start, stop
	}

	// Retrieve the columns from the cache if they exist.
	c.mu.RLock()
	start, _ = c.getBoundsFromCache(c.startColumn, l)
	stop, _ = c.getBoundsFromCache(c.stopColumn, l)
	c.mu.RUnlock()

	// If we could not retrieve an array from the cache,
	// create one here outside of the lock.
	// Record that we will need to replace the values in
	// the cache.
	replace := false
	if start == nil {
		start, replace = c.createBounds(b.Start, l, mem), true
	}
	if stop == nil {
		stop, replace = c.createBounds(b.Stop, l, mem), true
	}

	if !replace {
		// No need to retrieve the write lock.
		// Return now since we retrieved all values from
		// the cache.
		return start, stop
	}

	c.mu.Lock()
	c.replaceBounds(&c.startColumn, start)
	c.replaceBounds(&c.stopColumn, stop)
	c.mu.Unlock()
	return start, stop
}

// getBoundsFromCache will return an array of values
// if the array in the cache is of the appropriate size.
// This must be called from inside of a lock.
func (c *tagsCache) getBoundsFromCache(arr *array.Int, l int) (*array.Int, bool) {
	if arr == nil || arr.Len() < l {
		return nil, false
	} else if arr.Len() == l {
		arr.Retain()
		return arr, true
	}

	// If the lengths do not match, but the cached array is less
	// than the desired array, then we can use slice.
	// NewSlice will automatically create a new reference to the
	// passed in array so we do not need to manually retain.
	vs := array.Slice(arr, 0, l)
	return vs.(*array.Int), true
}

// replaceBounds will examine the array and replace it if
// the length of the array is greater than the current array
// or if there isn't an array in the cache.
// This must be called from inside of a write lock.
func (c *tagsCache) replaceBounds(cache **array.Int, arr *array.Int) {
	if *cache != nil {
		if (*cache).Len() >= arr.Len() {
			// The cached value is longer so just keep it.
			return
		}
		(*cache).Release()
	}
	arr.Retain()
	*cache = arr
}

// createBounds will create an array of times for the given time with
// the given length.
//
// DO NOT CALL THIS METHOD IN A LOCK. It is slow and will probably
// cause lock contention.
func (c *tagsCache) createBounds(ts execute.Time, l int, mem memory.Allocator) *array.Int {
	b := array.NewIntBuilder(mem)
	b.Resize(l)
	for i := 0; i < l; i++ {
		b.Append(int64(ts))
	}
	return b.NewIntArray()
}

// GetTag returns a binary arrow array that contains the value
// repeated l times. If an array with a length greater than or
// equal to the length and with the same value exists in the cache,
// a reference to the data will be retained and returned.
// Otherwise, the allocator will be used to construct a new column.
func (c *tagsCache) GetTag(value string, l int, mem memory.Allocator) *array.String {
	if l == 0 || c == nil {
		return c.createTag(value, l, mem)
	}

	// Attempt to retrieve the array from the cache.
	arr, ok := c.getTagFromCache(value, l)
	if !ok {
		// The array is not in the cache so create it.
		arr = c.createTag(value, l, mem)
	}
	c.touchOrReplaceTag(arr)
	return arr
}

// getTagFromCache will return an array of values with the
// specified value at the specified length. If there is no
// cache entry or the entry is not large enough for the
// specified length, then this returns false.
func (c *tagsCache) getTagFromCache(value string, l int) (*array.String, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	elem, ok := c.tags[value]
	if !ok {
		return nil, false
	}

	arr := elem.Value.(*array.String)
	if arr.Len() == l {
		arr.Retain()
		return arr, true
	} else if arr.Len() < l {
		return nil, false
	}

	// If the lengths do not match, but the cached array is less
	// than the desired array, then we can use slice.
	// Slice will automatically create a new reference to the
	// passed in array so we do not need to manually retain.
	vs := array.Slice(arr, 0, l)
	return vs.(*array.String), true
}

// touchOrReplaceTag will update the LRU cache to have
// the value specified by the array as the most recently
// used entry. If the cache entry does not exist or the
// current array in the cache is shorter than this one,
// it will replace the array.
func (c *tagsCache) touchOrReplaceTag(arr *array.String) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		c.lru = list.New()
	}
	if c.tags == nil {
		c.tags = make(map[string]*list.Element)
	}

	value := arr.Value(0)
	if elem, ok := c.tags[value]; ok {
		// If the array in the cache is longer to or
		// equal to the current tag, then do not touch it.
		carr := elem.Value.(*array.String)
		if carr.Len() < arr.Len() {
			// Retain this array again and release our
			// previous reference to the other array.
			arr.Retain()
			elem.Value = arr
			carr.Release()
		}

		// Move this element to the front of the lru.
		c.lru.MoveBefore(elem, c.lru.Front())
	} else {
		arr.Retain()
		c.tags[value] = c.lru.PushFront(arr)
	}
	c.maintainLRU()
}

// maintainLRU will ensure the lru cache maintains the appropriate
// length by ejecting the least recently used value from the cache
// until the cache is the appropriate size.
//
// This function must be called from inside of a lock.
func (c *tagsCache) maintainLRU() {
	max := c.maxLength
	if max == 0 {
		max = defaultMaxLengthForTagsCache
	}
	if c.lru.Len() <= max {
		return
	}
	arr := c.lru.Remove(c.lru.Back()).(*array.String)
	value := arr.Value(0)
	delete(c.tags, value)
	arr.Release()
}

// createTag will create a new array for a tag with the given
// length.
//
// DO NOT CALL THIS METHOD IN A LOCK. It is slow and will probably
// cause lock contention.
func (c *tagsCache) createTag(value string, l int, mem memory.Allocator) *array.String {
	b := array.NewStringBuilder(mem)
	b.Resize(l)
	b.ReserveData(l * len(value))
	for i := 0; i < l; i++ {
		b.Append(value)
	}
	return b.NewStringArray()
}

// Release will release all references to cached tag columns.
func (c *tagsCache) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.startColumn != nil {
		c.startColumn.Release()
		c.startColumn = nil
	}

	if c.stopColumn != nil {
		c.stopColumn.Release()
		c.stopColumn = nil
	}

	for _, elem := range c.tags {
		elem.Value.(*array.String).Release()
	}
	c.tags = nil
	c.lru = nil
}
