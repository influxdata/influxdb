package tsi1

import (
	"container/list"
	"sync"

	"github.com/influxdata/influxdb/v2/tsdb"
)

// TagValueSeriesIDCache is an LRU cache for series id sets associated with
// name -> key -> value mappings. The purpose of the cache is to provide
// efficient means to get sets of series ids that would otherwise involve merging
// many individual bitmaps at query time.
//
// When initialising a TagValueSeriesIDCache a capacity must be provided. When
// more than c items are added to the cache, the least recently used item is
// evicted from the cache.
//
// A TagValueSeriesIDCache comprises a linked list implementation to track the
// order by which items should be evicted from the cache, and a hashmap implementation
// to provide constant time retrievals of items from the cache.
type TagValueSeriesIDCache struct {
	sync.RWMutex
	cache   map[string]map[string]map[string]*list.Element
	evictor *list.List

	capacity int
}

// NewTagValueSeriesIDCache returns a TagValueSeriesIDCache with capacity c.
func NewTagValueSeriesIDCache(c int) *TagValueSeriesIDCache {
	return &TagValueSeriesIDCache{
		cache:    map[string]map[string]map[string]*list.Element{},
		evictor:  list.New(),
		capacity: c,
	}
}

// Get returns the SeriesIDSet associated with the {name, key, value} tuple if it
// exists.
func (c *TagValueSeriesIDCache) Get(name, key, value []byte) *tsdb.SeriesIDSet {
	c.Lock()
	defer c.Unlock()
	return c.get(name, key, value)
}

func (c *TagValueSeriesIDCache) get(name, key, value []byte) *tsdb.SeriesIDSet {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				c.evictor.MoveToFront(ele) // This now becomes most recently used.
				return ele.Value.(*seriesIDCacheElement).SeriesIDSet
			}
		}
	}
	return nil
}

// exists returns true if the item exists for the tuple {name, key, value}.
func (c *TagValueSeriesIDCache) exists(name, key, value string) bool {
	if mmap, ok := c.cache[name]; ok {
		if tkmap, ok := mmap[key]; ok {
			_, ok := tkmap[value]
			return ok
		}
	}
	return false
}

// addToSet adds x to the SeriesIDSet associated with the tuple {name, key, value}
// if it exists. This method takes a lock on the underlying SeriesIDSet.
//
// NB this does not count as an access on the set—therefore the set is not promoted
// within the LRU cache.
func (c *TagValueSeriesIDCache) addToSet(name, key, value []byte, x uint64) {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				ss := ele.Value.(*seriesIDCacheElement).SeriesIDSet
				if ss == nil {
					ele.Value.(*seriesIDCacheElement).SeriesIDSet = tsdb.NewSeriesIDSet(x)
					return
				}
				ele.Value.(*seriesIDCacheElement).SeriesIDSet.Add(x)
			}
		}
	}
}

// measurementContainsSets returns true if there are sets cached for the provided measurement.
func (c *TagValueSeriesIDCache) measurementContainsSets(name []byte) bool {
	_, ok := c.cache[string(name)]
	return ok
}

// Put adds the SeriesIDSet to the cache under the tuple {name, key, value}. If
// the cache is at its limit, then the least recently used item is evicted.
func (c *TagValueSeriesIDCache) Put(name, key, value []byte, ss *tsdb.SeriesIDSet) {
	c.Lock()
	// Convert once; the same string backing array is shared between
	// the cache element (used during eviction) and the map keys.
	nameStr := string(name)
	keyStr := string(key)
	valueStr := string(value)
	// Check under the write lock if the relevant item is now in the cache.
	if c.exists(nameStr, keyStr, valueStr) {
		c.Unlock()
		return
	}
	defer c.Unlock()

	// Ensure our SeriesIDSet is go heap backed.
	if ss != nil {
		ss = ss.Clone()
	}

	// Create list item, and add to the front of the eviction list.
	listElement := c.evictor.PushFront(&seriesIDCacheElement{
		name:        nameStr,
		key:         keyStr,
		value:       valueStr,
		SeriesIDSet: ss,
	})

	// Add the listElement to the set of items.
	if mmap, ok := c.cache[nameStr]; ok {
		if tkmap, ok := mmap[keyStr]; ok {
			if _, ok := tkmap[valueStr]; ok {
				goto EVICT
			}

			// Add the set to the map
			tkmap[valueStr] = listElement
			goto EVICT
		}

		// No series set map for the tag key - first tag value for the tag key.
		mmap[keyStr] = map[string]*list.Element{valueStr: listElement}
		goto EVICT
	}

	// No map for the measurement - first tag key for the measurement.
	c.cache[nameStr] = map[string]map[string]*list.Element{
		keyStr: {valueStr: listElement},
	}

EVICT:
	c.checkEviction()
}

// Delete removes x from the tuple {name, key, value} if it exists.
// This method takes a lock on the underlying SeriesIDSet.
func (c *TagValueSeriesIDCache) Delete(name, key, value []byte, x uint64) {
	c.Lock()
	c.delete(name, key, value, x)
	c.Unlock()
}

// delete removes x from the tuple {name, key, value} if it exists.
func (c *TagValueSeriesIDCache) delete(name, key, value []byte, x uint64) {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				if ss := ele.Value.(*seriesIDCacheElement).SeriesIDSet; ss != nil {
					ele.Value.(*seriesIDCacheElement).SeriesIDSet.Remove(x)
				}
			}
		}
	}
}

// checkEviction checks if the cache is too big, and evicts the least recently used
// item if it is.
func (c *TagValueSeriesIDCache) checkEviction() {
	if c.evictor.Len() <= c.capacity {
		return
	}

	e := c.evictor.Back() // Least recently used item.
	listElement := e.Value.(*seriesIDCacheElement)
	name := listElement.name
	key := listElement.key
	value := listElement.value

	c.evictor.Remove(e)                                       // Remove from evictor
	delete(c.cache[string(name)][string(key)], string(value)) // Remove from hashmap of items.

	// Check if there are no more tag values for the tag key.
	if len(c.cache[string(name)][string(key)]) == 0 {
		delete(c.cache[string(name)], string(key))
	}

	// Check there are no more tag keys for the measurement.
	if len(c.cache[string(name)]) == 0 {
		delete(c.cache, string(name))
	}
}

// seriesIDCacheElement is an item stored within a cache.
type seriesIDCacheElement struct {
	name        string
	key         string
	value       string
	SeriesIDSet *tsdb.SeriesIDSet
}
